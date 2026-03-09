import os
from typing import List

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.table import Table

from src.agent import ask as agent_ask
from src.db import get_connection
from src.ingest import ingest_all

load_dotenv()

app = typer.Typer()
console = Console()


@app.command()
def ingest(
    formats: List[str] = typer.Option(
        ["ODI", "T20I"], "--formats", "-f", help="Match formats to ingest"
    ),
):
    """Download Cricsheet data and load into PostgreSQL."""
    database_url = os.environ.get("WRITE_DATABASE_URL") or os.environ["DATABASE_URL"]
    conn = get_connection(database_url)

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    data_dir = os.path.abspath(data_dir)

    console.print(f"Ingesting formats: {', '.join(formats)}")
    results = ingest_all(data_dir, conn, formats)
    conn.close()

    console.print(
        f"[green]Success: {results['success']}[/green]  "
        f"[red]Failed: {results['failed']}[/red]  "
        f"[yellow]Skipped: {results['skipped']}[/yellow]"
    )


@app.command()
def verify():
    """Run sanity checks against the database."""
    database_url = os.environ["DATABASE_URL"]
    conn = get_connection(database_url)
    cur = conn.cursor()

    # 1. Total matches
    cur.execute("SELECT COUNT(*) FROM matches")
    count = cur.fetchone()[0]
    console.print(f"Total matches: {count}")

    # 2. Total deliveries
    cur.execute("SELECT COUNT(*) FROM deliveries")
    count = cur.fetchone()[0]
    console.print(f"Total deliveries: {count}")

    # 3. Matches by format
    cur.execute("SELECT match_type, COUNT(*) FROM matches GROUP BY 1")
    table = Table(title="Matches by Format")
    table.add_column("Format")
    table.add_column("Count", justify="right")
    for row in cur.fetchall():
        table.add_row(str(row[0]), str(row[1]))
    console.print(table)

    # 4. Top 5 ODI run scorers
    cur.execute(
        """SELECT batter, SUM(runs_batter) as total_runs
           FROM deliveries JOIN matches USING(match_id)
           WHERE match_type='ODI'
           GROUP BY 1 ORDER BY 2 DESC LIMIT 5"""
    )
    table = Table(title="Top 5 ODI Run Scorers")
    table.add_column("Batter")
    table.add_column("Total Runs", justify="right")
    for row in cur.fetchall():
        table.add_row(str(row[0]), str(row[1]))
    console.print(table)

    # 5. Kohli ODI batting stats
    cur.execute(
        "SELECT * FROM batting_career_stats WHERE player='V Kohli' AND format='ODI'"
    )
    row = cur.fetchone()
    if row:
        cols = [desc[0] for desc in cur.description]
        table = Table(title="V Kohli ODI Batting Stats")
        for col in cols:
            table.add_column(col)
        table.add_row(*[str(v) for v in row])
        console.print(table)
    else:
        console.print("[yellow]No data found for V Kohli in ODIs[/yellow]")

    cur.close()
    conn.close()


@app.command()
def ask(
    question: str = typer.Argument(..., help="Cricket question in natural language"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print SQL queries as they run"),
):
    """Ask a cricket question in natural language."""
    reader_url = os.environ.get(
        "READER_DATABASE_URL",
        os.environ["DATABASE_URL"].replace("postgresql://postgres:postgres@", "postgresql://cricket_reader:readonlypass@"),
    )
    console.print(f"[dim]Question: {question}[/dim]\n")
    result = agent_ask(question, reader_url, verbose=verbose)
    console.print(Markdown(result["answer"]))


if __name__ == "__main__":
    app()
