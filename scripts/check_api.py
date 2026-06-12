"""One authenticated request. Prints only status/count/budget, never secrets or data."""
import json
from app.config import Settings
from app.opensky import OpenSky

if __name__ == '__main__':
    import requests
    try:
        payload, headers = OpenSky(Settings()).poll()
        print(json.dumps({'authenticated':True, 'state_count':len(payload.get('states') or []),
                          'remaining_credits':headers.get('X-Rate-Limit-Remaining')}))
    except requests.RequestException as exc:
        print(json.dumps({'authenticated':False, 'error_type':type(exc).__name__,
                          'http_status':exc.response.status_code if exc.response is not None else None}))
        raise SystemExit(1)
