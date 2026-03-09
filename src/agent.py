import json
import os
from typing import Any, Callable, Generator

import anthropic
import psycopg2

from src.db import get_conn, put_conn
from src import vault

SYSTEM_PROMPT = """You are a cricket statistics assistant. You answer questions by querying a PostgreSQL database of international cricket matches sourced from Cricsheet.

## Database Schema

### Tables

**matches**
- match_id TEXT (primary key, e.g. "1234567")
- match_type TEXT — values: 'ODI', 'T20I'
- match_date DATE
- venue TEXT
- city TEXT
- team1 TEXT, team2 TEXT
- winner TEXT (NULL if no result)
- win_by_runs INT (NULL if won by wickets or no result)
- win_by_wickets INT (NULL if won by runs or no result)
- win_method TEXT (e.g. 'D/L' for Duckworth-Lewis)

**innings**
- match_id TEXT, innings_number INT (1 or 2, or 3+ for super overs)
- batting_team TEXT, bowling_team TEXT
- is_super_over BOOLEAN

**deliveries**
- match_id TEXT, innings_number INT, over_number INT (0-based), ball_number INT
- batter TEXT, bowler TEXT, non_striker TEXT
- runs_batter INT, runs_extras INT, runs_total INT
- extras_type TEXT (NULL, 'wides', 'noballs', 'byes', 'legbyes', 'penalty')
- is_wide BOOLEAN, is_noball BOOLEAN
- wicket_player_out TEXT (player dismissed, NULL if no wicket)
- wicket_kind TEXT — values: 'caught', 'bowled', 'lbw', 'run out', 'caught and bowled', 'stumped', 'retired hurt', 'hit wicket', 'obstructing the field', 'retired not out', 'retired out', 'handled the ball'
- wicket_fielders TEXT[] (array of fielder names, NULL if not applicable)

**players**
- match_id TEXT, team TEXT, player_name TEXT

### Views (use these for career aggregates — they exclude super overs)

**batting_career_stats**
- player TEXT, format TEXT, matches INT, innings INT
- total_runs INT, dismissals INT, average NUMERIC, strike_rate NUMERIC

**bowling_career_stats**
- player TEXT, format TEXT, wickets INT, economy NUMERIC, average NUMERIC

## Player Names

Player names use Cricsheet abbreviations. Examples:
- Virat Kohli → 'V Kohli'
- Rohit Sharma → 'RG Sharma'
- Sachin Tendulkar → 'SR Tendulkar'
- MS Dhoni → 'MS Dhoni'
- AB de Villiers → 'AB de Villiers'
- Babar Azam → 'Babar Azam'
- Kane Williamson → 'KS Williamson'
- Steve Smith → 'SPD Smith'
- David Warner → 'DA Warner'
- Joe Root → 'JE Root'
- Ben Stokes → 'BA Stokes'
- Jasprit Bumrah → 'JJ Bumrah'
- Pat Cummins → 'PJ Cummins'
- Mitchell Starc → 'MA Starc'
- Rashid Khan → 'Rashid Khan'
- Shakib Al Hasan → 'Shakib Al Hasan'
- Kumar Sangakkara → 'KC Sangakkara'
- Mahela Jayawardene → 'DPMD Jayawardene'
- Lasith Malinga → 'SL Malinga'
- Dale Steyn → 'DW Steyn'
- Chris Gayle → 'CH Gayle'
- Hashim Amla → 'HM Amla'
- Shoaib Akhtar → 'Shoaib Akhtar'
- Wasim Akram → 'Wasim Akram'
- Jacques Kallis → 'JH Kallis'

If a query returns 0 rows for a player name, the name format is likely wrong. Try variations or check nearby spellings.

## Rules and Limitations

- Coverage: ODIs from ~2006 onward, T20Is limited dataset (~324 matches). Older matches may be missing.
- Centuries and fifties must be calculated via CTE. Sum runs_batter per (batter, match_id, innings_number), then count innings where total >= 100 (century) or >= 50 and < 100 (fifty). Always exclude super overs (join innings i and filter i.is_super_over = FALSE). Example:
  WITH innings_totals AS (
      SELECT d.batter, m.match_type, d.match_id, d.innings_number,
             SUM(d.runs_batter) AS innings_runs
      FROM deliveries d
      JOIN matches m USING (match_id)
      JOIN innings i ON d.match_id = i.match_id AND d.innings_number = i.innings_number
      WHERE i.is_super_over = FALSE
      GROUP BY d.batter, m.match_type, d.match_id, d.innings_number
  )
  SELECT batter, COUNT(*) AS centuries FROM innings_totals
  WHERE match_type = 'ODI' AND innings_runs >= 100
  GROUP BY batter ORDER BY centuries DESC LIMIT 10;
- over_number is 0-based: over 1 = over_number 0, over 6 = over_number 5.
- For economy/average, use bowling_career_stats view — it correctly excludes run outs and byes.
- Wickets for bowlers: only 'caught', 'bowled', 'lbw', 'caught and bowled', 'stumped', 'hit wicket' credit the bowler (use bowling_career_stats which handles this).
- If the question is outside cricket or cannot be answered from this data, say so clearly.
"""

TOOL_DEFINITION = {
    "name": "run_query",
    "description": "Execute a read-only SQL query against the cricket database and return the results.",
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "A SELECT or WITH ... SELECT SQL query.",
            }
        },
        "required": ["sql"],
    },
}


def _safe_query(
    sql: str,
    conn,
    cache_get: Callable | None = None,
    cache_put: Callable | None = None,
) -> str:
    """Run sql with guardrails. Returns result as a formatted string."""
    stripped = sql.strip()

    # Only allow SELECT and WITH (CTEs)
    upper = stripped.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return "Error: only SELECT or WITH queries are allowed."

    # Auto-append LIMIT if missing
    if "LIMIT" not in upper:
        stripped = stripped.rstrip(";") + " LIMIT 100"

    # Check cache
    if cache_get is not None:
        cached = cache_get(stripped)
        if cached is not None:
            return cached

    cur = conn.cursor()
    try:
        cur.execute("SET statement_timeout = '10s'")
        cur.execute(stripped)
        rows = cur.fetchall()
        if not rows:
            result = "Query returned 0 rows."
        else:
            cols = [desc[0] for desc in cur.description]
            lines = ["\t".join(cols)]
            for row in rows:
                lines.append("\t".join(str(v) if v is not None else "NULL" for v in row))
            result = "\n".join(lines)

        if cache_put is not None:
            cache_put(stripped, result)
        return result
    except psycopg2.Error as e:
        conn.rollback()
        return f"Query error: {e.pgerror or str(e)}"
    finally:
        cur.close()


def ask(
    question: str,
    reader_url: str,
    verbose: bool = False,
    history: list | None = None,
    cache_get: Callable | None = None,
    cache_put: Callable | None = None,
) -> dict:
    """Run an agentic loop: send question to Claude, execute tool calls, return answer + queries.

    Returns dict: {"answer": str, "queries": list[str]}
    history: prior conversation turns as [{"role": ..., "content": ...}]
    """
    client = anthropic.Anthropic(api_key=vault.get_anthropic_key())
    conn = get_conn()

    messages = list(history or []) + [{"role": "user", "content": question}]
    queries: list[str] = []

    try:
        while True:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=[TOOL_DEFINITION],
                messages=messages,
            )

            # Append assistant turn
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                for block in response.content:
                    if hasattr(block, "text"):
                        return {"answer": block.text, "queries": queries}
                return {"answer": "", "queries": queries}

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        sql = block.input.get("sql", "")
                        queries.append(sql)
                        if verbose:
                            print(f"\n[SQL]\n{sql}\n")
                        result = _safe_query(sql, conn, cache_get, cache_put)
                        if verbose:
                            print(f"[Result]\n{result}\n")
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "user", "content": tool_results})
            else:
                break

        return {"answer": "No response generated.", "queries": queries}
    finally:
        put_conn(conn)


def ask_streaming(
    question: str,
    reader_url: str,
    history: list | None = None,
    cache_get: Callable | None = None,
    cache_put: Callable | None = None,
) -> Generator[dict, None, None]:
    """Streaming version of ask(). Yields event dicts:
    - {"type": "token", "content": str}
    - {"type": "query", "sql": str}
    """
    client = anthropic.Anthropic(api_key=vault.get_anthropic_key())
    conn = get_conn()

    messages = list(history or []) + [{"role": "user", "content": question}]

    try:
        while True:
            # Use streaming API
            collected_content = []
            stop_reason = None

            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=[TOOL_DEFINITION],
                messages=messages,
            ) as stream:
                current_text = ""
                for event in stream:
                    if event.type == "content_block_start":
                        if event.content_block.type == "text":
                            current_text = ""
                        elif event.content_block.type == "tool_use":
                            pass
                    elif event.type == "content_block_delta":
                        if event.delta.type == "text_delta":
                            current_text += event.delta.text
                            yield {"type": "token", "content": event.delta.text}

                # Get the final message for tool handling
                final_message = stream.get_final_message()
                stop_reason = final_message.stop_reason
                collected_content = final_message.content

            messages.append({"role": "assistant", "content": collected_content})

            if stop_reason == "end_turn":
                return

            if stop_reason == "tool_use":
                tool_results = []
                for block in collected_content:
                    if block.type == "tool_use":
                        sql = block.input.get("sql", "")
                        yield {"type": "query", "sql": sql}
                        result = _safe_query(sql, conn, cache_get, cache_put)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "user", "content": tool_results})
            else:
                return
    finally:
        put_conn(conn)
