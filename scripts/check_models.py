"""Optional local dbt smoke test against a disposable database.

Requires dbt-postgres installed locally and PostgreSQL reachable via PGHOST/PGPORT.
Tests full build, incremental late visibility, and preservation of key uniqueness.
"""
import os
import time
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from dbt.cli.main import dbtRunner

from app.demo import snapshot
from app.validation import validate


def main():
    args = {'host':os.getenv('PGHOST','127.0.0.1'), 'port':os.getenv('PGPORT','55432'),
            'user':os.getenv('PGUSER','pipeline'), 'password':os.environ['POSTGRES_PASSWORD']}
    admin = psycopg2.connect(dbname='flight_warehouse', **args)
    admin.autocommit = True
    name = 'test_models_' + uuid4().hex[:12]
    conn = None
    original_db = os.environ.get('PGDATABASE')
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        os.environ['PGDATABASE'] = name
        conn = psycopg2.connect(dbname=name, **args)
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(Path('postgres/init.sql').read_text())
        now = int(time.time())
        def insert(state, late=False):
            row = validate(state)
            with conn.cursor() as cursor:
                cursor.execute('SELECT ensure_raw_partition(%s)', (row['position_at'].date(),))
                cursor.execute("""INSERT INTO raw_flight_states
                    (icao24,position_at,callsign,origin_country,longitude,latitude,
                     baro_altitude,velocity,on_ground,source,payload,polled_at,ingested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'demo',%s,now(),
                            now() - %s * interval '1 day')""",
                    (row['icao24'],row['position_at'],row['callsign'],row['origin_country'],
                     row['longitude'],row['latitude'],row['baro_altitude'],row['velocity'],
                     row['on_ground'],Json(state),int(late)))
        for state in snapshot(now)['states']:
            insert(state)
        for incremental in (False,True):
            if incremental:
                insert(snapshot(now-60)['states'][0], late=True)
            result = dbtRunner().invoke(['build','--project-dir','dbt_project/flight_analytics',
                '--profiles-dir','dbt_project/flight_analytics','--target-path','target/model-smoke'])
            if not result.success:
                raise RuntimeError('dbt build failed')
            with conn.cursor() as cursor:
                cursor.execute('SELECT count(*) FROM analytics.stg_flight_states')
                assert cursor.fetchone()[0] == 32 + int(incremental)
        print('PASS: full build and incremental late-visible observation (32 -> 33 rows)')
    finally:
        if conn:
            conn.close()
        if original_db is None:
            os.environ.pop('PGDATABASE',None)
        else:
            os.environ['PGDATABASE'] = original_db
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(name)))
        admin.close()


if __name__ == '__main__':
    main()
