.PHONY: up down init test logs dbt api-check
up:
	docker compose up -d --build
down:
	docker compose down
init: up
test:
	docker compose --profile test run --rm tests
	docker compose stop dbt-runner
	docker compose run --rm --no-deps dbt-runner dbt build
	docker compose start dbt-runner
logs:
	docker compose logs -f --tail 50 producer consumer dbt-runner
dbt:
	docker compose stop dbt-runner
	docker compose run --rm --no-deps dbt-runner dbt build
	docker compose start dbt-runner
api-check:
	docker compose run --rm --no-deps producer python -c "from app.config import Settings; from app.opensky import OpenSky; p,h=OpenSky(Settings()).poll(); print('Authenticated; aircraft:',len(p.get('states') or []))"
