"""Generate the two provisioned, live-only Grafana dashboards."""
import csv
import json
from pathlib import Path

DS = {'type': 'grafana-postgresql-datasource', 'uid': 'flight-warehouse'}
AIRLINES = json.loads((Path(__file__).resolve().parents[1] / 'grafana/airlines.json').read_text())
# Inline reference data keeps provisioning independent of the dbt refresh.
AIRLINE_SQL = "(VALUES " + ','.join("('%s','%s')" % (prefix, a['name'].replace("'", "''")) for a in AIRLINES for prefix in a['prefixes']) + ") AS airlines(code,name)"
JOIN = " JOIN " + AIRLINE_SQL + " ON upper(left(trim(callsign),3))=airlines.code "
CURRENT = "FROM flight_current" + JOIN + "WHERE source='live' AND position_at > now()-interval '5 minutes'"
RAW = "FROM raw_flight_states WHERE source='live'"
POLLS = "FROM pipeline_metrics WHERE component='producer' AND source='live'"


def panel(pid, title, kind, query, pos, unit='short', description='', color='#5794F2'):
    defaults = {'unit': unit, 'decimals': 0, 'noValue': 'En attente',
                'color': {'mode': 'fixed', 'fixedColor': color}}
    options = {}
    if kind == 'stat':
        options = {'colorMode': 'value', 'graphMode': 'none', 'justifyMode': 'center',
                   'textMode': 'auto', 'wideLayout': True,
                   'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}
    elif kind == 'timeseries':
        defaults['custom'] = {'drawStyle': 'line', 'lineInterpolation': 'smooth',
            'lineWidth': 2, 'fillOpacity': 12, 'showPoints': 'never',
            'spanNulls': False, 'axisBorderShow': False}
        options = {'legend': {'displayMode': 'list', 'placement': 'bottom', 'showLegend': True},
                   'tooltip': {'mode': 'multi', 'sort': 'desc'}}
    elif kind == 'table':
        defaults['unit'] = 'none'
        options = {'showHeader': True, 'cellHeight': 'sm',
                   'footer': {'show': False}, 'sortBy': []}
    return {'id': pid, 'title': title, 'type': kind, 'description': description,
            'datasource': DS, 'gridPos': dict(zip(('x', 'y', 'w', 'h'), pos)),
            'targets': [{'refId': 'A', 'datasource': DS, 'rawSql': query,
                'format': 'time_series' if kind == 'timeseries' else 'table',
                'rawQuery': True, 'editorMode': 'code'}],
            'fieldConfig': {'defaults': defaults, 'overrides': []}, 'options': options}


def banner(title, text):
    return {'id': 1, 'title': '', 'type': 'text', 'transparent': True,
            'gridPos': {'x': 0, 'y': 0, 'w': 24, 'h': 3},
            'options': {'mode': 'markdown', 'content': f'### {title}\n{text}'}}


def freshness(p):
    p['fieldConfig']['defaults'].update({'color': {'mode': 'thresholds'},
        'thresholds': {'mode': 'absolute', 'steps': [
            {'color': '#73BF69', 'value': None}, {'color': '#FF9830', 'value': 120},
            {'color': '#F2495C', 'value': 300}]}})
    return p


def dashboard(uid, title, panels):
    return {'uid': uid, 'title': title, 'description': 'Données réelles OpenSky uniquement.',
            'tags': ['OpenSky', 'API', 'Live'], 'timezone': 'utc', 'schemaVersion': 40,
            'version': 2, 'editable': False, 'refresh': '15s',
            'timepicker': {'refresh_intervals': ['15s', '30s', '1m', '5m']},
            'time': {'from': 'now-1h', 'to': 'now'},
            'links': [{'title': 'Trafic aérien', 'url': '/d/flight-operations', 'icon': 'dashboard'},
                      {'title': 'Qualité du flux', 'url': '/d/pipeline-health', 'icon': 'heart'}],
            'panels': panels}


def build():
    observed = panel(2, 'Avions observés · 5 min', 'stat',
        'SELECT count(*) AS avions '+CURRENT, (0, 3, 6, 4),
        description='Avions distincts avec une position OpenSky datant de moins de 5 minutes.')
    airborne = panel(3, 'En vol · 5 min', 'stat',
        'SELECT count(*) AS avions '+CURRENT+' AND NOT on_ground', (6, 3, 6, 4), color='#73BF69')
    observed['fieldConfig']['defaults']['unit'] = 'none'
    airborne['fieldConfig']['defaults']['unit'] = 'none'
    altitude = panel(4, 'Altitude moyenne · en vol', 'stat',
        'SELECT avg(baro_altitude)/1000 AS altitude '+CURRENT+' AND NOT on_ground',
        (12, 3, 6, 4), 'suffix: km', 'Altitude barométrique des avions en vol observés récemment.', '#B877D9')
    altitude['fieldConfig']['defaults']['decimals'] = 1
    age = freshness(panel(5, 'Dernière position · âge', 'stat',
        "SELECT extract(epoch FROM now()-max(position_at)) AS age FROM flight_current"+JOIN+"WHERE source='live'",
        (18, 3, 6, 4), 's', 'Âge de la position OpenSky la plus récente. Orange au-delà de 2 min, rouge au-delà de 5 min.'))
    geo = panel(6, 'Positions réelles · monde · 20 compagnies', 'geomap',
        '''SELECT coalesce(nullif(trim(callsign),''),icao24) AS "Indicatif",
            airlines.name AS "Compagnie",latitude,longitude,round(baro_altitude::numeric) AS "Altitude (m)",
            round((velocity*3.6)::numeric) AS "Vitesse (km/h)",
            position_at AS "Position UTC" '''+CURRENT,
        (0, 7, 16, 13), description='Une position par avion, fraîche de moins de 5 minutes. Survolez un point pour les détails.')
    geo['options'] = {
        'view': {'id': 'coords', 'lat': 25, 'lon': 5, 'zoom': 1.5},
        'basemap': {'type': 'osm-standard', 'name': 'OpenStreetMap', 'config': {}},
        'controls': {'showZoom': True, 'showAttribution': True, 'showScale': True},
        'tooltip': {'mode': 'details'},
        'layers': [{'type': 'markers', 'name': 'Positions OpenSky',
            'location': {'mode': 'coords', 'latitude': 'latitude', 'longitude': 'longitude'},
            'config': {'showLegend': False, 'style': {
                'color': {'fixed': '#0057B8'}, 'size': {'fixed': 7}, 'opacity': 0.9}}}]}
    latest = panel(7, 'Les 20 compagnies · avions observés', 'table',
        'SELECT airlines.name AS "Compagnie", count(f.icao24) AS "Avions" FROM '+AIRLINE_SQL+
        " LEFT JOIN flight_current f ON upper(left(trim(f.callsign),3))=airlines.code AND f.source='live'"
        " AND f.position_at > now()-interval '5 minutes' GROUP BY airlines.name ORDER BY 2 DESC,1",
        (16, 7, 8, 13), description='Sélection fixe OAG Takeoff 2025. Compagnie déduite du préfixe OACI de l’indicatif. Principaux codes easyJet, LATAM et Wizz inclus ; couverture des filiales non exhaustive. Zéro signifie aucun avion observé récemment.')
    traffic = panel(8, 'Avions observés par minute', 'timeseries',
        "SELECT date_trunc('minute',position_at) AS time,count(DISTINCT icao24) AS \"Observés\","
        " count(DISTINCT icao24) FILTER (WHERE NOT on_ground) AS \"En vol\" FROM raw_flight_states"+JOIN+
        "WHERE source='live' AND $__timeFilter(position_at) GROUP BY 1 ORDER BY 1",
        (0, 20, 14, 8), description='Avions des 20 compagnies observés par minute. Les intervalles sans collecte ne sont pas des zéros.')
    traffic['fieldConfig']['defaults']['color'] = {'mode': 'palette-classic'}
    countries = panel(9, 'Pays d’immatriculation · avions observés', 'barchart',
        '''SELECT coalesce(origin_country,'Inconnu') AS "Pays", count(*) AS "Avions" '''
            +CURRENT+' GROUP BY 1 ORDER BY 2 DESC,1 LIMIT 8',
        (14, 20, 10, 8), description='Pays indiqué par OpenSky, distinct de la position géographique actuelle.')
    countries['options'] = {'orientation': 'horizontal', 'showValue': 'always',
        'xField': 'Pays', 'barWidth': 0.65, 'groupWidth': 0.8,
        'legend': {'showLegend': False}, 'tooltip': {'mode': 'single'}}
    ops = dashboard('flight-operations', 'OpenSky · Trafic aérien', [
        banner('OpenSky / Trafic aérien',
            '**20 grandes compagnies · monde** · données API réelles · positions de moins de 5 min · collecte ≈ 96 s · UTC'),
        observed, airborne, altitude, age, geo, latest, traffic, countries])

    health = dashboard('pipeline-health', 'OpenSky · Qualité du flux', [
        banner('OpenSky / Qualité du flux',
            '**Flux API réel uniquement** · fraîcheur, ingestion et crédits · heures UTC'),
        freshness(panel(2, 'Dernier appel réussi · âge', 'stat',
            'SELECT extract(epoch FROM now()-max(recorded_at)) AS age '+POLLS,
            (0, 3, 6, 4), 's')),
        panel(3, 'Crédits API restants', 'stat',
            'SELECT credits_remaining AS credits '+POLLS+' AND credits_remaining IS NOT NULL ORDER BY recorded_at DESC LIMIT 1',
            (6, 3, 6, 4), description='Dernier solde communiqué par OpenSky ; ce n’est pas une estimation.', color='#73BF69'),
        panel(4, 'Observations reçues · période', 'stat',
            'SELECT count(*) AS observations '+RAW+' AND $__timeFilter(ingested_at)',
            (12, 3, 6, 4), description='Positions dédupliquées reçues pendant la période sélectionnée.'),
        panel(5, 'Crédits estimés · jour UTC', 'stat',
            "SELECT coalesce(sum(credits_estimated),0) AS credits "+POLLS+" AND recorded_at >= date_trunc('day',now())",
            (18, 3, 6, 4), description='Somme des coûts estimés des appels réussis enregistrés ; d’autres usages du compte peuvent consommer des crédits.', color='#FF9830'),
        panel(6, 'Délai collecte → stockage · médiane / p95', 'timeseries',
            '''SELECT date_trunc('minute',ingested_at) AS time,
                percentile_cont(0.5) within group (order by extract(epoch FROM ingested_at-polled_at)) AS "Médiane",
                percentile_cont(0.95) within group (order by extract(epoch FROM ingested_at-polled_at)) AS "P95" '''
                +RAW+' AND $__timeFilter(ingested_at) GROUP BY 1 ORDER BY 1',
            (0, 7, 12, 8), 's', 'Mesuré après la réponse API, jusqu’à l’écriture dans PostgreSQL. Uniquement les observations réelles.'),
        panel(7, 'Observations stockées par minute', 'timeseries',
            "SELECT date_trunc('minute',ingested_at) AS time,count(*) AS observations "+RAW+
                ' AND $__timeFilter(ingested_at) GROUP BY 1 ORDER BY 1', (12, 7, 12, 8)),
        panel(8, 'Avions renvoyés par appel API', 'timeseries',
            'SELECT recorded_at AS time,batch_size AS avions '+POLLS+
                ' AND $__timeFilter(recorded_at) ORDER BY 1', (0, 15, 12, 8)),
        panel(9, 'Solde de crédits communiqué par OpenSky', 'timeseries',
            'SELECT recorded_at AS time,credits_remaining AS credits '+POLLS+
                ' AND $__timeFilter(recorded_at) ORDER BY 1', (12, 15, 12, 8)),
        panel(10, 'Données dbt · âge', 'stat',
            "SELECT extract(epoch FROM now()-max(ingested_at)) AS age FROM analytics.stg_flight_states WHERE source='live'",
            (0, 23, 8, 5), 's', 'Âge des données disponibles dans les modèles ; ce n’est pas la durée du dernier calcul dbt.'),
        panel(11, 'Derniers appels OpenSky réussis · UTC', 'table',
            '''SELECT to_char(recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') AS "Appel UTC",
                batch_size AS "Avions",credits_remaining AS "Crédits restants",
                round(duration_ms::numeric) AS "Durée (ms)" '''+POLLS+
                ' ORDER BY recorded_at DESC LIMIT 15', (8, 23, 16, 5))])
    health['panels'][5]['fieldConfig']['defaults']['color'] = {'mode': 'palette-classic'}
    health['panels'][5]['fieldConfig']['defaults']['decimals'] = 2
    for p in health['panels']:
        if p['id'] in (3, 4, 5, 9):
            p['fieldConfig']['defaults']['unit'] = 'none'
    project = Path(__file__).resolve().parents[1]
    # Share the dashboard's verified prefixes with the analytical dimension.
    reference = {prefix: a['name'] for a in AIRLINES for prefix in a['prefixes']}
    reference.update({'RAM': 'Royal Air Maroc', 'RMA': 'Air Arabia Maroc',
                      'IBE': 'Iberia', 'AFR': 'Air France',
                      'DEM': 'Synthetic demo traffic', 'UNK': 'Unknown'})
    seed = project / 'dbt_project/flight_analytics/seeds/airlines.csv'
    with seed.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle, lineterminator='\n')
        writer.writerow(['callsign_prefix', 'airline_name'])
        writer.writerows(sorted(reference.items()))
    root = project / 'grafana' / 'dashboards'
    for value in (ops, health):
        (root / (value['uid']+'.json')).write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')


if __name__ == '__main__':
    build()
