import glob as globmod
import io
import os
import zipfile

import psycopg2.extras
import requests
import yaml


def parse_delivery(
    delivery_data: dict,
    match_id: str,
    innings_number: int,
    over_number: int,
    ball_number: int,
) -> dict:
    """Parse a single delivery from Cricsheet YAML into a flat dict."""
    runs = delivery_data.get("runs", {})
    extras = delivery_data.get("extras", {})

    # Determine extras type (first key in extras dict if present)
    extras_type = None
    if extras:
        extras_type = list(extras.keys())[0]

    is_wide = extras_type == "wides"
    is_noball = extras_type == "noballs"

    # Wicket info — new format: "wickets" list; old format: "wicket" dict
    wickets = delivery_data.get("wickets", [])
    if not wickets and delivery_data.get("wicket"):
        wickets = [delivery_data["wicket"]]
    wicket = wickets[0] if wickets else {}
    wicket_player_out = wicket.get("player_out", "").strip() if wicket.get("player_out") else None
    wicket_kind = wicket.get("kind")

    wicket_fielders = None
    if wicket.get("fielders"):
        wicket_fielders = [
            f.get("name", "").strip() if isinstance(f, dict) else str(f).strip()
            for f in wicket["fielders"]
        ]

    return {
        "match_id": match_id,
        "innings_number": innings_number,
        "over_number": over_number,
        "ball_number": ball_number,
        "batter": (delivery_data.get("batter") or delivery_data.get("batsman", "")).strip(),
        "bowler": delivery_data.get("bowler", "").strip() if delivery_data.get("bowler") else None,
        "non_striker": delivery_data.get("non_striker", "").strip() if delivery_data.get("non_striker") else None,
        "runs_batter": runs.get("batter") if runs.get("batter") is not None else runs.get("batsman", 0),
        "runs_extras": runs.get("extras", 0),
        "runs_total": runs.get("total", 0),
        "extras_type": extras_type,
        "is_wide": is_wide,
        "is_noball": is_noball,
        "wicket_player_out": wicket_player_out,
        "wicket_kind": wicket_kind,
        "wicket_fielders": wicket_fielders,
    }


def parse_match(yaml_path: str, match_id: str) -> dict:
    """Parse a Cricsheet YAML match file into structured dicts.

    Returns dict with keys: match, innings, deliveries, players.
    """
    with open(yaml_path, encoding="utf-8-sig") as f:
        data = yaml.safe_load(f)

    info = data.get("info", {})

    # Match metadata
    dates = info.get("dates", [])
    match_date = dates[0] if dates else None

    # Normalize Cricsheet's "IT20" (International T20) to "T20I"
    match_type = info.get("match_type")
    if match_type == "IT20":
        match_type = "T20I"
    venue = info.get("venue")
    city = info.get("city")
    teams = info.get("teams", [])

    # Outcome — handle missing gracefully
    outcome = info.get("outcome", {})
    winner = outcome.get("winner")
    win_by = outcome.get("by", {})
    win_by_runs = win_by.get("runs")
    win_by_wickets = win_by.get("wickets")
    win_method = outcome.get("method")

    match_dict = {
        "match_id": match_id,
        "match_type": match_type,
        "match_date": match_date,
        "venue": venue,
        "city": city,
        "team1": teams[0] if len(teams) > 0 else None,
        "team2": teams[1] if len(teams) > 1 else None,
        "winner": winner,
        "win_by_runs": win_by_runs,
        "win_by_wickets": win_by_wickets,
        "win_method": win_method,
    }

    # Players
    players_dict = info.get("players", {})
    players = []
    for team, team_players in players_dict.items():
        for player_name in team_players:
            players.append({
                "match_id": match_id,
                "team": team,
                "player_name": player_name.strip(),
            })

    # Innings and deliveries
    innings_list = []
    deliveries_list = []

    for innings_idx, innings_data in enumerate(data.get("innings", []), start=1):
        # Cricsheet YAML structure: each innings entry is a dict with one key
        innings_key = list(innings_data.keys())[0] if isinstance(innings_data, dict) else None

        # Handle both old format (nested under key) and new format (flat)
        if innings_key and innings_key != "deliveries":
            innings_info = innings_data[innings_key]
        else:
            innings_info = innings_data

        batting_team = innings_info.get("team", "")
        is_super_over = innings_info.get("super_over", False)

        # Determine bowling team
        bowling_team = ""
        for t in teams:
            if t != batting_team:
                bowling_team = t
                break

        innings_list.append({
            "match_id": match_id,
            "innings_number": innings_idx,
            "batting_team": batting_team,
            "bowling_team": bowling_team,
            "is_super_over": bool(is_super_over),
        })

        # Parse deliveries
        deliveries_data = innings_info.get("deliveries", [])
        for delivery_entry in deliveries_data:
            if isinstance(delivery_entry, dict):
                for ball_key, ball_data in delivery_entry.items():
                    # ball_key is like "0.1" -> over 0, ball 1
                    parts = str(ball_key).split(".")
                    over_number = int(parts[0])
                    ball_number = int(parts[1]) if len(parts) > 1 else 1

                    parsed = parse_delivery(
                        ball_data, match_id, innings_idx, over_number, ball_number
                    )
                    deliveries_list.append(parsed)

    return {
        "match": match_dict,
        "innings": innings_list,
        "deliveries": deliveries_list,
        "players": players,
    }


def download_and_extract(url: str, dest: str) -> list:
    """Download a Cricsheet zip archive and extract YAML files.

    Returns list of paths to extracted YAML files.
    """
    os.makedirs(dest, exist_ok=True)
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    yaml_paths = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for name in zf.namelist():
            if name.endswith(".yaml"):
                zf.extract(name, dest)
                yaml_paths.append(os.path.join(dest, name))

    return yaml_paths


DOWNLOAD_URLS = {
    "ODI": "https://cricsheet.org/downloads/odis.zip",
    "T20I": "https://cricsheet.org/downloads/it20s.zip",
}


def upsert_match(parsed: dict, conn) -> None:
    """Insert or update a parsed match and all its related data.

    Uses a single transaction. Caller is responsible for commit/rollback.
    """
    cur = conn.cursor()
    try:
        m = parsed["match"]
        # Upsert match
        cur.execute(
            """INSERT INTO matches (match_id, match_type, match_date, venue, city,
                                    team1, team2, winner, win_by_runs, win_by_wickets, win_method)
               VALUES (%(match_id)s, %(match_type)s, %(match_date)s, %(venue)s, %(city)s,
                       %(team1)s, %(team2)s, %(winner)s, %(win_by_runs)s, %(win_by_wickets)s, %(win_method)s)
               ON CONFLICT (match_id) DO UPDATE SET
                   match_type = EXCLUDED.match_type,
                   match_date = EXCLUDED.match_date,
                   venue = EXCLUDED.venue,
                   city = EXCLUDED.city,
                   team1 = EXCLUDED.team1,
                   team2 = EXCLUDED.team2,
                   winner = EXCLUDED.winner,
                   win_by_runs = EXCLUDED.win_by_runs,
                   win_by_wickets = EXCLUDED.win_by_wickets,
                   win_method = EXCLUDED.win_method""",
            m,
        )

        # Upsert innings
        for inn in parsed["innings"]:
            cur.execute(
                """INSERT INTO innings (match_id, innings_number, batting_team, bowling_team, is_super_over)
                   VALUES (%(match_id)s, %(innings_number)s, %(batting_team)s, %(bowling_team)s, %(is_super_over)s)
                   ON CONFLICT (match_id, innings_number) DO UPDATE SET
                       batting_team = EXCLUDED.batting_team,
                       bowling_team = EXCLUDED.bowling_team,
                       is_super_over = EXCLUDED.is_super_over""",
                inn,
            )

        # Upsert deliveries using executemany
        if parsed["deliveries"]:
            # Convert wicket_fielders list to PostgreSQL array format
            delivery_tuples = []
            for d in parsed["deliveries"]:
                delivery_tuples.append((
                    d["match_id"], d["innings_number"], d["over_number"], d["ball_number"],
                    d["batter"], d["bowler"], d["non_striker"],
                    d["runs_batter"], d["runs_extras"], d["runs_total"],
                    d["extras_type"], d["is_wide"], d["is_noball"],
                    d["wicket_player_out"], d["wicket_kind"], d["wicket_fielders"],
                ))
            psycopg2.extras.execute_batch(
                cur,
                """INSERT INTO deliveries (match_id, innings_number, over_number, ball_number,
                                           batter, bowler, non_striker,
                                           runs_batter, runs_extras, runs_total,
                                           extras_type, is_wide, is_noball,
                                           wicket_player_out, wicket_kind, wicket_fielders)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (match_id, innings_number, over_number, ball_number) DO UPDATE SET
                       batter = EXCLUDED.batter,
                       bowler = EXCLUDED.bowler,
                       non_striker = EXCLUDED.non_striker,
                       runs_batter = EXCLUDED.runs_batter,
                       runs_extras = EXCLUDED.runs_extras,
                       runs_total = EXCLUDED.runs_total,
                       extras_type = EXCLUDED.extras_type,
                       is_wide = EXCLUDED.is_wide,
                       is_noball = EXCLUDED.is_noball,
                       wicket_player_out = EXCLUDED.wicket_player_out,
                       wicket_kind = EXCLUDED.wicket_kind,
                       wicket_fielders = EXCLUDED.wicket_fielders""",
                delivery_tuples,
                page_size=1000,
            )

        # Upsert players
        for p in parsed["players"]:
            cur.execute(
                """INSERT INTO players (match_id, team, player_name)
                   VALUES (%(match_id)s, %(team)s, %(player_name)s)
                   ON CONFLICT (match_id, team, player_name) DO NOTHING""",
                p,
            )
    finally:
        cur.close()


def ingest_all(data_dir: str, conn, formats: list = None) -> dict:
    """Ingest all YAML files from data_dir into the database.

    Downloads data if not already present. Skips already-ingested matches.
    Returns dict with success/failed/skipped counts.
    """
    if formats is None:
        formats = ["ODI", "T20I"]

    results = {"success": 0, "failed": 0, "skipped": 0}

    for fmt in formats:
        url = DOWNLOAD_URLS.get(fmt)
        if not url:
            continue

        fmt_dir = os.path.join(data_dir, fmt.lower())
        yaml_files = globmod.glob(os.path.join(fmt_dir, "*.yaml"))

        if not yaml_files:
            yaml_files = download_and_extract(url, fmt_dir)

        for yaml_path in yaml_files:
            match_id = os.path.splitext(os.path.basename(yaml_path))[0]

            # Skip already-ingested
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM matches WHERE match_id = %s", (match_id,))
            if cur.fetchone():
                cur.close()
                results["skipped"] += 1
                continue
            cur.close()

            try:
                parsed = parse_match(yaml_path, match_id)

                # Skip non-target formats
                if parsed["match"]["match_type"] not in formats:
                    results["skipped"] += 1
                    continue

                upsert_match(parsed, conn)
                conn.commit()
                results["success"] += 1
            except Exception as e:
                conn.rollback()
                results["failed"] += 1
                print(f"  FAILED {match_id}: {e}")

    return results
