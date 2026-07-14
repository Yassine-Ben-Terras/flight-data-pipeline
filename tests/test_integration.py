"""Exercise a real broker and an isolated, disposable PostgreSQL database."""
import json
import os
import time
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv('RUN_INTEGRATION') != '1' and os.getenv('RUN_DATABASE_TESTS') != '1',
    reason='requires Docker services')


@pytest.fixture
def warehouse(monkeypatch):
    from app.config import connect
    from psycopg import sql
    name = 'test_opensky_' + uuid4().hex[:12]
    admin = connect()
    admin.autocommit = True
    admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    monkeypatch.setenv('PGDATABASE', name)
    conn = connect()
    conn.autocommit = True
    conn.execute(Path('postgres/init.sql').read_text())
    try:
        yield conn
    finally:
        conn.close()
        admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        admin.close()


@pytest.mark.skipif(os.getenv('RUN_INTEGRATION') != '1', reason='requires Kafka')
def test_kafka_replay_out_of_order_reject_and_rollback(warehouse):
    from confluent_kafka import Producer, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from app.config import Settings
    from app.consumer import write_batch
    from app.demo import snapshot
    import psycopg

    bootstrap = Settings().bootstrap
    topic = 'test-' + uuid4().hex
    admin = AdminClient({'bootstrap.servers':bootstrap})
    for future in admin.create_topics([NewTopic(topic, 1, 1)]).values():
        future.result()
    producer = Producer({'bootstrap.servers':bootstrap})
    consumer = Consumer({'bootstrap.servers':bootstrap, 'group.id':topic,
                         'enable.auto.commit':False, 'auto.offset.reset':'earliest'})
    now = int(time.time())
    def envelope(stamp):
        return {'schema_version':1, 'source':'demo',
                'polled_at':datetime.now(timezone.utc).isoformat(),
                'state':snapshot(stamp)['states'][0]}
    records = [envelope(now), envelope(now), envelope(now-60), {'invalid':True}]
    try:
        errors = []
        for record in records:
            producer.produce(topic, json.dumps(record).encode(), callback=lambda e,m: errors.append(e) if e else None)
        assert producer.flush(20) == 0 and not errors
        consumer.subscribe([topic])
        batch = []
        deadline = time.monotonic()+40
        while len(batch)<4 and time.monotonic()<deadline:
            batch += consumer.consume(4-len(batch), timeout=1)
        assert len(batch) == 4 and not any(m.error() for m in batch)

        # Force the final metrics insert to fail: all preceding writes must roll back.
        warehouse.execute('ALTER TABLE pipeline_metrics ADD CONSTRAINT fail_test CHECK (batch_size < 0)')
        with pytest.raises(psycopg.errors.CheckViolation):
            write_batch(warehouse, batch)
        assert warehouse.execute('SELECT count(*) FROM raw_flight_states').fetchone()[0] == 0
        assert warehouse.execute('SELECT count(*) FROM processed_messages').fetchone()[0] == 0
        assert warehouse.execute('SELECT count(*) FROM rejected_flight_states').fetchone()[0] == 0
        warehouse.execute('ALTER TABLE pipeline_metrics DROP CONSTRAINT fail_test')

        assert write_batch(warehouse, batch) == {'processed':4,'inserted':2,'rejected':1}
        assert write_batch(warehouse, batch) == {'processed':0,'inserted':0,'rejected':0}
        assert warehouse.execute('SELECT extract(epoch from position_at)::bigint FROM flight_current').fetchone()[0] == now
        assert warehouse.execute('SELECT count(*) FROM pipeline_metrics').fetchone()[0] == 1
        assert warehouse.execute('SELECT count(*) FROM rejected_flight_states').fetchone()[0] == 1
        assert warehouse.execute('SELECT count(*) FROM raw_flight_states').fetchone()[0] == 2
        consumer.commit(message=batch[-1], asynchronous=False)
        assert consumer.committed([TopicPartition(topic,0)])[0].offset == 4
    finally:
        consumer.close()
        for future in admin.delete_topics([topic]).values():
            future.result()


def test_database_transaction_replay_and_retention(warehouse):
    import psycopg
    from app.consumer import write_batch
    from app.demo import snapshot
    from app.maintenance import maintain

    class Message:
        def __init__(self, offset, value):
            self.index, self.body = offset, value
        def topic(self): return 'database-test'
        def partition(self): return 0
        def offset(self): return self.index
        def value(self): return self.body

    now = int(time.time())
    def payload(stamp):
        return json.dumps({'schema_version':1,'source':'demo',
            'polled_at':datetime.now(timezone.utc).isoformat(),
            'state':snapshot(stamp)['states'][0]}).encode()
    batch = [Message(0,payload(now)),Message(1,payload(now)),Message(2,payload(now-60)),
             Message(3,b'not-json'),Message(4,b'{"value": NaN}'),Message(5,b'\x00')]
    warehouse.execute('ALTER TABLE pipeline_metrics ADD CONSTRAINT fail_test CHECK (batch_size < 0)')
    with pytest.raises(psycopg.errors.CheckViolation):
        write_batch(warehouse,batch)
    for table in ['raw_flight_states','flight_current','rejected_flight_states','processed_messages']:
        assert warehouse.execute('SELECT count(*) FROM '+table).fetchone()[0] == 0
    warehouse.execute('ALTER TABLE pipeline_metrics DROP CONSTRAINT fail_test')
    assert write_batch(warehouse,batch) == {'processed':6,'inserted':2,'rejected':3}
    assert write_batch(warehouse,batch) == {'processed':0,'inserted':0,'rejected':0}
    assert warehouse.execute('SELECT extract(epoch from position_at)::bigint FROM flight_current').fetchone()[0] == now
    assert warehouse.execute('SELECT count(*) FROM pipeline_metrics').fetchone()[0] == 1
    # An expired partition can be removed without touching current observations.
    old_name = warehouse.execute("SELECT 'raw_flight_states_' || to_char(current_date-16,'YYYYMMDD')").fetchone()[0]
    warehouse.execute('SELECT ensure_raw_partition(current_date-16)')
    maintain()
    assert warehouse.execute('SELECT to_regclass(%s)',(old_name,)).fetchone()[0] is None
    assert warehouse.execute('SELECT count(*) FROM raw_flight_states').fetchone()[0] == 2
