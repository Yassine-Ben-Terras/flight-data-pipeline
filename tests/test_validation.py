from datetime import datetime, timezone
import pytest
from app.config import Settings, credit_cost
from app.demo import snapshot
from app.validation import validate
from app.opensky import retry_delay

NOW = datetime(2026, 10, 8, tzinfo=timezone.utc)


def vector():
    return snapshot(int(NOW.timestamp()))['states'][0]


def test_valid_and_original_preserved():
    state = vector()
    assert validate(state, NOW)['icao24'] == 'f00000'
    assert state == vector()


@pytest.mark.parametrize('index,value', [(0,None),(0,'nothex'),(3,None),(3,True),
    (5,181),(6,-91),(6,float('nan')),(7,20001),(9,351),(9,-1),(8,1), (1,123),
    (1,'BAD\x00'), (1,'BAD\ud800')])
def test_rejects_corruption(index, value):
    state = vector()
    state[index] = value
    with pytest.raises(ValueError):
        validate(state, NOW)


def test_optional_altitude_and_velocity():
    state = vector()
    state[7] = state[9] = None
    assert validate(state, NOW)['velocity'] is None


@pytest.mark.parametrize('state', [None, {}, [], [0]*19])
def test_invalid_shape(state):
    with pytest.raises(ValueError):
        validate(state, NOW)


@pytest.mark.parametrize('delta', [-15*86400, 301])
def test_timestamp_boundary(delta):
    state = vector()
    state[3] += delta
    with pytest.raises(ValueError):
        validate(state, NOW)


@pytest.mark.parametrize('area,cost', [(25,1),(25.1,2),(100,2),(100.1,3),(400,3),(401,4)])
def test_credit_tiers(area, cost):
    assert credit_cost(area) == cost


def test_budget_and_box():
    assert Settings(budget=3600, coverage='regional').interval == 24
    with pytest.raises(ValueError):
        Settings(lamin=40, lamax=30)
    with pytest.raises(ValueError):
        Settings(budget=0)


def test_backoff():
    assert retry_delay(99) == 900
    assert retry_delay(0, {'X-Rate-Limit-Retry-After-Seconds':'4200'}) == 4200
    assert retry_delay(1, {'Retry-After':'bad'}) == 10


def test_global_coverage_budget():
    settings = Settings(coverage='global', budget=3600)
    assert settings.bbox == {}
    assert settings.cost == 4
    assert settings.interval == 96
    with pytest.raises(ValueError):
        Settings(coverage='invalid')
