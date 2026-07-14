import pytest
from app.config import Settings
from app.opensky import OpenSky


class Response:
    def __init__(self, data, status=200):
        self.data, self.status_code = data, status
        self.headers = {'X-Rate-Limit-Remaining':'3599'}

    def json(self):
        return self.data

    def raise_for_status(self):
        import requests
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


class Session:
    def __init__(self):
        self.auth_calls = 0
        self.state_response = Response({'time':123, 'states':None})

    def post(self, url, data, timeout):
        assert url.startswith('https://auth.opensky-network.org/')
        assert data['grant_type'] == 'client_credentials'
        self.auth_calls += 1
        return Response({'access_token':'test-token', 'expires_in':300})

    def get(self, url, params, headers, timeout):
        assert url == 'https://opensky-network.org/api/states/all'
        assert headers['Authorization'] == 'Bearer test-token'
        self.last_params = params
        return self.state_response


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setenv('OPENSKY_CLIENT_ID','test-client')
    monkeypatch.setenv('OPENSKY_CLIENT_SECRET','test-secret')
    client = OpenSky(Settings())
    client.session = Session()
    return client


def test_token_cached_and_empty_states_accepted(api):
    assert api.poll()[0]['states'] is None
    api.poll()
    assert api.session.auth_calls == 1


def test_expired_token_refreshed(api):
    api.poll()
    api.expires_at = 0
    api.poll()
    assert api.session.auth_calls == 2


def test_401_invalidates_token(api):
    import requests
    api.session.state_response = Response({}, 401)
    with pytest.raises(requests.HTTPError):
        api.poll()
    assert api.expires_at == 0


@pytest.mark.parametrize('data', [[], {}, {'time':123,'states':{}}, {'time':'abc'}])
def test_malformed_response(api, data):
    api.session.state_response = Response(data)
    with pytest.raises(ValueError):
        api.poll()


def test_global_poll_has_no_geographic_filter(api):
    api.settings = Settings(coverage='global', budget=3600)
    api.poll()
    assert api.session.last_params == {}


def test_regional_poll_keeps_bounding_box(api):
    api.settings = Settings(coverage='regional')
    api.poll()
    assert api.session.last_params == api.settings.bbox
