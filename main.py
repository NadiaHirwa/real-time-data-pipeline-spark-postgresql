"""
main.py

Single command-line entry point for this project. Does NOT combine
the generator and streaming job into one process - they remain
independent services, started in separate terminals, exactly as a
real producer/consumer pair would run in production. This file exists
purely as a consistent launcher so every operation on this project is
reachable through one interface, rather than requiring someone to
remember which of five different files and commands to run.

Usage:
    python main.py generator [--iterations N]
    python main.py stream
    python main.py verify
    python main.py test
    python main.py clean
    python main.py reset [--force]
    python main.py status
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / "scripts"))

import config
import database
from monitoring_logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent

# Every table the pipeline writes to, truncated together by the reset
# subcommand. Kept as one list so the count in reset's summary message
# can never disagree with the tables it actually cleared. Must stay in
# sync with sql/postgres_setup.sql.
RESET_TABLES = ["events", "rejected_events", "staging_events", "stream_metrics"]


def run_generator(iterations: int | None) -> None:
    """Delegates to data_generator.run() - the producer."""
    from data_generator import run as generator_run
    generator_run(max_iterations=iterations)


def run_stream() -> None:
    """Delegates to spark_streaming.run() - the consumer."""
    from spark_streaming import build_spark_session, run as streaming_run
    spark = build_spark_session()
    try:
        streaming_run(spark)
    except KeyboardInterrupt:
        logger.info("Streaming job stopped by user.")
    finally:
        spark.stop()


def run_verify() -> None:
    """Runs the SQL verification queries from database.py and prints results."""
    if not database.test_connection():
        print("Could not connect to the database. Check .env settings.")
        return

    print(f"Total events:                 {database.row_count()}")
    print(f"Total rejected:                {database.rejected_count()}")
    print(f"Constraint violations (want 0): {len(database.constraint_violations())}")
    print(f"Duplicate event_ids (want 0):   {len(database.duplicate_event_ids())}")
    print("\nLatest 5 events:")
    for row in database.latest_events(5):
        print(" ", row)


def run_tests() -> None:
    """Runs the pytest suite."""
    result = subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v"], cwd=PROJECT_ROOT)
    sys.exit(result.returncode)


def _clear_files_and_checkpoint() -> int:
    """
    Delete generated/archived CSVs and clear the Spark checkpoint,
    returning how many CSV files were removed.

    Shared by clean and reset rather than duplicated, so the two can
    never drift apart in what they treat as "a fresh local state" -
    reset is defined as exactly clean plus the database tables, not as
    a second implementation of the same idea.
    """
    import shutil

    removed = 0
    for folder in [config.INCOMING_DIR, config.ARCHIVE_DIR, config.REJECTED_DIR]:
        for f in folder.glob("*.csv"):
            f.unlink()
            removed += 1

    if config.CHECKPOINT_DIR.exists():
        shutil.rmtree(config.CHECKPOINT_DIR, ignore_errors=True)
        config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        remaining = list(config.CHECKPOINT_DIR.rglob("*"))
        if remaining:
            print(
                f"Warning: {len(remaining)} checkpoint file(s) could not be removed "
                "(likely a Windows file lock from OneDrive sync or an open editor). "
                "Close any open files under checkpoint/ and re-run, or delete manually."
            )

    return removed


def run_clean() -> None:
    """
    Removes generated/archived CSVs and clears the Spark checkpoint,
    for a fresh local test run. Never touches the database - this is
    guaranteed, documented behaviour (see docs/retention_policy.md),
    which is why clearing the tables lives in a separate subcommand
    rather than behind a flag here. Use `python main.py reset` if the
    stored rows should go too.
    """
    removed = _clear_files_and_checkpoint()
    print(f"Removed {removed} CSV file(s) and cleared checkpoint directory.")
    print("Database rows were NOT touched. Run python main.py reset if you also want the tables cleared.")


def run_reset(force: bool) -> None:
    """
    Everything clean does, PLUS truncating every table in RESET_TABLES.

    Deliberately a separate subcommand rather than a --with-database
    flag on clean: clean is safe to run habitually between test runs,
    and folding destruction into it as an option would make that safety
    depend on remembering which flags were passed.

    The database connection is checked BEFORE any local file is
    deleted, so a reset that cannot reach Postgres fails having changed
    nothing at all, rather than leaving the confusing half-done state
    of wiped CSVs alongside a still-full database.
    """
    if not force:
        print(f"This will permanently delete all data in {', '.join(RESET_TABLES)}.")
        answer = input("Type 'yes' to continue: ")
        if answer.strip().lower() != "yes":
            print("Reset cancelled. Nothing was changed.")
            return

    if not database.test_connection():
        print(
            "Could not connect to the database. Nothing was changed - "
            "local files and checkpoint are untouched. Check .env settings."
        )
        return

    removed = _clear_files_and_checkpoint()
    database.execute_statement(f"TRUNCATE TABLE {', '.join(RESET_TABLES)};")

    print(
        f"Removed {removed} CSV file(s), cleared checkpoint directory, and truncated "
        f"{len(RESET_TABLES)} database tables ({', '.join(RESET_TABLES)})."
    )


def run_status() -> None:
    """Reports whether each piece of the pipeline's environment is ready."""
    print("Directories:")
    for name, path in [
        ("incoming", config.INCOMING_DIR), ("archive", config.ARCHIVE_DIR),
        ("rejected", config.REJECTED_DIR), ("checkpoint", config.CHECKPOINT_DIR),
        ("logs", config.LOG_DIR),
    ]:
        status = "OK" if path.exists() else "MISSING"
        print(f"  {name:12s} {path}  [{status}]")

    print("\nDatabase:")
    db_ok = database.test_connection()
    print(f"  Connection: {'OK' if db_ok else 'FAILED'}")
    if db_ok:
        print(f"  events table row count: {database.row_count()}")

    pending = len(list(config.INCOMING_DIR.glob("*.csv")))
    print(f"\nPending files in incoming/: {pending}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time e-commerce pipeline launcher.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    gen_parser = subparsers.add_parser("generator", help="Run the data generator (producer).")
    gen_parser.add_argument("--iterations", type=int, default=None, help="Stop after N files (default: run forever).")

    subparsers.add_parser("stream", help="Run the Spark Structured Streaming job (consumer).")
    subparsers.add_parser("verify", help="Run SQL verification queries against Postgres.")
    # Runs the whole suite, including the integration tests that need a
    # live PostgreSQL. For a fast, database-free sanity check, run pytest
    # directly instead: pytest -m "not integration"
    subparsers.add_parser(
        "test",
        help='Run the full pytest suite. For a fast subset with no database needed, '
             'run pytest -m "not integration" directly.',
    )
    subparsers.add_parser("clean", help="Remove generated/archived CSVs and clear the checkpoint.")

    reset_parser = subparsers.add_parser(
        "reset",
        help="DESTRUCTIVE: everything clean does, plus truncating all database tables.",
    )
    reset_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the confirmation prompt, for non-interactive use in a script or CI.",
    )

    subparsers.add_parser("status", help="Report readiness of directories and the database connection.")

    args = parser.parse_args()

    if args.command == "generator":
        run_generator(args.iterations)
    elif args.command == "stream":
        run_stream()
    elif args.command == "verify":
        run_verify()
    elif args.command == "test":
        run_tests()
    elif args.command == "clean":
        run_clean()
    elif args.command == "reset":
        run_reset(args.force)
    elif args.command == "status":
        run_status()


if __name__ == "__main__":
    main()