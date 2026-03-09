import yaml
import pytest

from src.ingest import parse_delivery, parse_match


def _write_yaml(tmp_path, data, filename="match.yaml"):
    path = tmp_path / filename
    path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
    return str(path)


def _minimal_match_yaml(overrides=None):
    """Return a minimal Cricsheet-style match dict. Apply overrides on top."""
    data = {
        "info": {
            "dates": ["2024-01-15"],
            "match_type": "T20",
            "venue": "Eden Gardens",
            "city": "Kolkata",
            "teams": ["India", "Australia"],
            "outcome": {
                "winner": "India",
                "by": {"runs": 42},
            },
            "players": {
                "India": ["V Kohli", "R Sharma"],
                "Australia": ["S Smith", "D Warner"],
            },
        },
        "innings": [
            {
                "1st innings": {
                    "team": "India",
                    "deliveries": [
                        {
                            "0.1": {
                                "batter": "V Kohli",
                                "bowler": "P Cummins",
                                "non_striker": "R Sharma",
                                "runs": {"batter": 4, "extras": 0, "total": 4},
                            }
                        }
                    ],
                }
            },
        ],
    }
    if overrides:
        data["info"].update(overrides)
    return data


# ---------- Test 1: parse_match extracts metadata ----------

def test_parse_match_extracts_metadata(tmp_path):
    data = _minimal_match_yaml()
    path = _write_yaml(tmp_path, data)

    result = parse_match(path, "m001")
    m = result["match"]

    assert m["match_id"] == "m001"
    assert m["match_date"] == "2024-01-15"
    assert m["match_type"] == "T20"
    assert m["venue"] == "Eden Gardens"
    assert m["city"] == "Kolkata"
    assert m["team1"] == "India"
    assert m["team2"] == "Australia"
    assert m["winner"] == "India"
    assert m["win_by_runs"] == 42
    assert m["win_by_wickets"] is None
    assert m["win_method"] is None


# ---------- Test 2: missing outcome -> all win fields None ----------

def test_parse_match_no_result(tmp_path):
    data = _minimal_match_yaml()
    del data["info"]["outcome"]
    path = _write_yaml(tmp_path, data)

    result = parse_match(path, "m002")
    m = result["match"]

    assert m["winner"] is None
    assert m["win_by_runs"] is None
    assert m["win_by_wickets"] is None
    assert m["win_method"] is None


# ---------- Test 3: parse_delivery basic runs ----------

def test_parse_delivery_basic_runs():
    delivery_data = {
        "batter": "V Kohli",
        "bowler": "P Cummins",
        "non_striker": "R Sharma",
        "runs": {"batter": 4, "extras": 1, "total": 5},
    }

    result = parse_delivery(delivery_data, "m003", 1, 5, 3)

    assert result["match_id"] == "m003"
    assert result["innings_number"] == 1
    assert result["over_number"] == 5
    assert result["ball_number"] == 3
    assert result["batter"] == "V Kohli"
    assert result["bowler"] == "P Cummins"
    assert result["runs_batter"] == 4
    assert result["runs_extras"] == 1
    assert result["runs_total"] == 5


# ---------- Test 4: wicket run out -> no bowler credit ----------

def test_wicket_run_out_no_bowler_credit():
    delivery_data = {
        "batter": "R Sharma",
        "bowler": "M Starc",
        "non_striker": "V Kohli",
        "runs": {"batter": 0, "extras": 0, "total": 0},
        "wickets": [
            {
                "player_out": "R Sharma",
                "kind": "run out",
                "fielders": [{"name": "S Smith"}],
            }
        ],
    }

    result = parse_delivery(delivery_data, "m004", 1, 3, 2)

    assert result["wicket_kind"] == "run out"
    assert result["wicket_player_out"] == "R Sharma"


# ---------- Test 5: wicket bowled -> credits bowler ----------

def test_wicket_bowled_credits_bowler():
    delivery_data = {
        "batter": "R Sharma",
        "bowler": "M Starc",
        "non_striker": "V Kohli",
        "runs": {"batter": 0, "extras": 0, "total": 0},
        "wickets": [
            {
                "player_out": "R Sharma",
                "kind": "bowled",
            }
        ],
    }

    result = parse_delivery(delivery_data, "m005", 1, 4, 1)

    assert result["wicket_kind"] == "bowled"
    assert result["bowler"] == "M Starc"
    assert result["wicket_player_out"] == "R Sharma"


# ---------- Test 6: super_over innings flagged ----------

def test_super_over_innings_flagged(tmp_path):
    data = _minimal_match_yaml()
    # Add a super over innings
    data["innings"].append({
        "3rd innings": {
            "team": "India",
            "super_over": True,
            "deliveries": [
                {
                    "0.1": {
                        "batter": "V Kohli",
                        "bowler": "P Cummins",
                        "non_striker": "R Sharma",
                        "runs": {"batter": 6, "extras": 0, "total": 6},
                    }
                }
            ],
        }
    })
    path = _write_yaml(tmp_path, data)

    result = parse_match(path, "m006")
    innings = result["innings"]

    # First innings should not be super over
    assert innings[0]["is_super_over"] is False

    # Second innings (the super over) should be flagged
    assert innings[1]["is_super_over"] is True


# ---------- Test 7: missing city does not raise ----------

def test_missing_city_does_not_raise(tmp_path):
    data = _minimal_match_yaml()
    del data["info"]["city"]
    path = _write_yaml(tmp_path, data)

    result = parse_match(path, "m007")
    assert result["match"]["city"] is None


# ---------- Test 8: player names are stripped ----------

def test_player_names_are_stripped(tmp_path):
    data = _minimal_match_yaml(overrides={
        "players": {
            "India": ["  V Kohli  ", "\tR Sharma\n"],
            "Australia": ["S Smith", " D Warner "],
        },
    })
    path = _write_yaml(tmp_path, data)

    result = parse_match(path, "m008")
    player_names = [p["player_name"] for p in result["players"]]

    assert "V Kohli" in player_names
    assert "R Sharma" in player_names
    assert "S Smith" in player_names
    assert "D Warner" in player_names
    # Ensure no whitespace remains
    for p in result["players"]:
        assert p["player_name"] == p["player_name"].strip()
