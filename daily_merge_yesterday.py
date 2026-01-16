import time
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.cloud import bigquery

# ================= CONFIG =================
API_KEY = "aqui viene tu apikey mor bebe" # Reemplaza con tu API key real
FOOTBALL_BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

PROJECT_ID = "byads-dsp"
DATASET = "report"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.calendar_sports_staging"
DELTA_TABLE  = f"{PROJECT_ID}.{DATASET}.calendar_sports_delta"

TZ_CO = ZoneInfo("America/Bogota")
SLEEP_SECONDS = 0.25

BACKFILL_FROM_CO = "2024-01-01"
BACKFILL_TO_CO = datetime.now(TZ_CO).date().isoformat()

SEASONS = [2023, 2024, 2025, 2026]

COLUMNS = [
    "event_id", "sport", "league", "season", "event_date", "event_datetime", "timezone",
    "home_team", "away_team", "venue", "country", "status", "round",
    "home_score", "away_score", "api_source", "api_updated_at", "ingested_at"
]

# ✅ FÚTBOL (sin Asia) + locales con nombres (fuente de verdad para league/country)
FOOTBALL_LEAGUES = [
    # EUROPA
    {"id": 140, "name": "LaLiga (España)",                 "country": "España",      "region": "Europa"},
    {"id": 39,  "name": "Premier League (Inglaterra)",     "country": "Inglaterra",  "region": "Europa"},
    {"id": 135, "name": "Serie A (Italia)",                "country": "Italia",      "region": "Europa"},
    {"id": 78,  "name": "Bundesliga (Alemania)",           "country": "Alemania",    "region": "Europa"},
    {"id": 61,  "name": "Ligue 1 (Francia)",               "country": "Francia",     "region": "Europa"},
    {"id": 2,   "name": "UEFA Champions League",           "country": "Europa",      "region": "Europa"},
    {"id": 3,   "name": "UEFA Europa League",              "country": "Europa",      "region": "Europa"},

    # SUDAMÉRICA – INTERNACIONAL
    {"id": 13,  "name": "CONMEBOL Libertadores",           "country": "Sudamérica",  "region": "Sudamérica"},
    {"id": 11,  "name": "CONMEBOL Sudamericana",           "country": "Sudamérica",  "region": "Sudamérica"},

    # NORTEAMÉRICA
    {"id": 253, "name": "MLS (USA)",                       "country": "USA",         "region": "Norteamérica"},
    {"id": 262, "name": "Liga MX (México)",                "country": "México",      "region": "Norteamérica"},

    # LOCALES CLIENTES
    {"id": 162, "name": "Liga Promerica (Costa Rica)",     "country": "Costa Rica",  "region": "Costa Rica"},
    {"id": 179, "name": "Liga Nacional (Honduras)",        "country": "Honduras",    "region": "Honduras"},
    {"id": 167, "name": "Liga Nacional (Guatemala)",       "country": "Guatemala",   "region": "Guatemala"},
    {"id": 168, "name": "Primera División (El Salvador)",  "country": "El Salvador", "region": "El Salvador"},
    {"id": 239, "name": "Liga BetPlay / Primera A (Colombia)","country":"Colombia",  "region": "Colombia"},
    {"id": 242, "name": "LigaPro Serie A (Ecuador)",       "country": "Ecuador",     "region": "Ecuador"},
    {"id": 281, "name": "Liga 1 (Perú)",                   "country": "Perú",        "region": "Perú"},
    {"id": 71,  "name": "Brasileirão Serie A (Brasil)",    "country": "Brasil",      "region": "Brasil"},

    # ✅ CONFIRMADOS
    {"id": 265, "name": "Primera División (Chile)",        "country": "Chile",       "region": "Chile"},
    {"id": 396, "name": "Primera Division (Nicaragua)",    "country": "Nicaragua",   "region": "Nicaragua"},

    # ✅ MUNDIAL (en tu log: league_id=1; fixtures sólo aparecen en season 2026)
    {"id": 1,   "name": "FIFA World Cup",                  "country": "Mundial",     "region": "Mundial"},
]
# =========================================


def api_get(path: str, params: dict):
    r = requests.get(f"{FOOTBALL_BASE}{path}", headers=HEADERS, params=params, timeout=60)
    data = r.json()
    if data.get("errors"):
        raise RuntimeError({"path": path, "params": params, "errors": data["errors"]})
    return data.get("response", [])


def iso_to_co_date(iso_utc: str) -> str:
    dt_utc = datetime.fromisoformat(str(iso_utc).replace("Z", "+00:00"))
    return dt_utc.astimezone(TZ_CO).date().isoformat()


def transform_football(item, league_override: str | None, country_override: str | None):
    fixture = item.get("fixture") or {}
    league = item.get("league") or {}
    teams = item.get("teams") or {}
    goals = item.get("goals") or {}

    event_datetime = str(fixture.get("date") or "").replace("+00:00", "Z")
    if not event_datetime:
        return None

    # ✅ Fuente de verdad: tu lista. Fallback: API.
    final_league = league_override if league_override else league.get("name")
    final_country = country_override if country_override else league.get("country")

    home = (teams.get("home") or {}).get("name")
    away = (teams.get("away") or {}).get("name")

    venue_obj = fixture.get("venue")
    venue = venue_obj.get("name") if isinstance(venue_obj, dict) else None

    status_obj = fixture.get("status")
    status = status_obj.get("short") if isinstance(status_obj, dict) else None

    return {
        "event_id": str(fixture.get("id")),
        "sport": "football",
        "league": final_league,
        "season": str(league.get("season", "")),
        "event_date": event_datetime[:10],
        "event_datetime": event_datetime,
        "timezone": fixture.get("timezone"),
        "home_team": home,
        "away_team": away,
        "venue": venue,
        "country": final_country,
        "status": status,
        "round": league.get("round"),
        "home_score": goals.get("home") if goals.get("home") is not None else 0,
        "away_score": goals.get("away") if goals.get("away") is not None else 0,
        "api_source": "api-football",
        "api_updated_at": event_datetime,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def ensure_delta_table(client: bigquery.Client):
    client.query(
        f"CREATE TABLE IF NOT EXISTS `{DELTA_TABLE}` AS "
        f"SELECT * FROM `{TARGET_TABLE}` WHERE 1=0"
    ).result()


def load_delta_table(client: bigquery.Client, rows: list[dict]) -> int:
    ensure_delta_table(client)

    schema = client.get_table(TARGET_TABLE).schema
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    client.load_table_from_json(rows, DELTA_TABLE, job_config=job_config).result()
    return len(rows)


def merge_delta_into_target(client: bigquery.Client):
    set_clause = ",\n      ".join([f"{c}=S.{c}" for c in COLUMNS if c != "event_id"])
    insert_cols = ", ".join(COLUMNS)
    insert_vals = ", ".join([f"S.{c}" for c in COLUMNS])

    sql = f"""
    MERGE `{TARGET_TABLE}` T
    USING `{DELTA_TABLE}` S
    ON T.event_id = S.event_id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """
    client.query(sql).result()


def main():
    print(f"📅 Backfill (Colombia): {BACKFILL_FROM_CO} → {BACKFILL_TO_CO}")
    print(f"🏆 Ligas: {len(FOOTBALL_LEAGUES)} | Seasons: {SEASONS}")

    leagues = {x["id"]: x for x in FOOTBALL_LEAGUES}
    league_ids = sorted(leagues.keys())

    seen = set()
    rows = []

    for league_id in league_ids:
        cfg = leagues[league_id]
        league_name = cfg["name"]
        country_override = cfg.get("country")
        print(f"\n⚽ {league_name} (ID {league_id})")

        for season in SEASONS:
            fixtures = api_get("/fixtures", {"league": league_id, "season": season})

            in_range = 0
            added = 0
            samples = 0

            for f in fixtures:
                fixture = f.get("fixture") or {}
                dt = fixture.get("date")
                if not dt:
                    continue

                co_day = iso_to_co_date(dt)
                if co_day < BACKFILL_FROM_CO or co_day > BACKFILL_TO_CO:
                    continue
                in_range += 1

                eid = str(fixture.get("id"))
                if not eid or eid == "None":
                    continue
                if eid in seen:
                    continue
                seen.add(eid)

                row = transform_football(f, league_override=league_name, country_override=country_override)
                if not row:
                    continue

                rows.append(row)
                added += 1

                # DEBUG Honduras: imprime 5 muestras
                if league_id == 179 and samples < 5:
                    samples += 1
                    print("   [HONDURAS SAMPLE]",
                          "date=", row["event_date"],
                          "league=", row["league"],
                          "country=", row["country"],
                          "home=", row["home_team"],
                          "away=", row["away_team"])

            print(f"   season {season}: in_range={in_range} +{added} (fixtures API: {len(fixtures)})")
            time.sleep(SLEEP_SECONDS)

    print(f"\n📦 Total eventos únicos a cargar: {len(rows)}")

    client = bigquery.Client(project=PROJECT_ID)
    loaded = load_delta_table(client, rows)
    print(f"✅ Delta cargado: {loaded} filas → `{DELTA_TABLE}`")

    merge_delta_into_target(client)
    print("✅ MERGE OK (historico desde 2024).")
    print("🎉 DONE.")


if __name__ == "__main__":
    main()
