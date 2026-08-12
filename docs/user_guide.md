# User Guide

Step-by-step instructions to set up and run this project from a fresh clone.

## Prerequisites

- Python 3.13 (or compatible)
- Java 17 (required by PySpark - Eclipse Temurin recommended)
- Apache Spark 4.2.0 (or compatible), with JAVA_HOME, SPARK_HOME, HADOOP_HOME set and winutils.exe in place if on Windows
- PostgreSQL, installed and running locally
- The PostgreSQL JDBC driver .jar (e.g. postgresql-42.7.13.jar), downloaded separately and its path configured in scripts/spark_streaming.py's build_spark_session()

See docs/architecture.md for the exact Spark/PostgreSQL configuration used during development.

## Setup

1. Clone this repository
2. Install Python dependencies:
   pip install -r requirements.txt
3. Copy .env.example to .env and fill in your real PostgreSQL username and password:
   cp .env.example .env
4. Create the database and tables:
   - Connect to PostgreSQL (e.g. via pgAdmin's Query Tool) and run the first line of sql/postgres_setup.sql (CREATE DATABASE ecommerce_events;) while connected to the default postgres database
   - Connect to the new ecommerce_events database and run the rest of sql/postgres_setup.sql

   **Already have this database set up from before, and are pulling new changes?** Re-run sql/postgres_setup.sql against your existing database - not just on first setup. The script is idempotent: CREATE TABLE IF NOT EXISTS and ALTER TABLE ... ADD COLUMN IF NOT EXISTS statements are both safe to re-run and won't affect existing data. This matters specifically because rejected_events gained a new corrupt_record column recently; without re-running the setup script, the first rejected-row write after pulling this change will fail with a missing-column error.

5. Verify everything is wired up correctly:
   python main.py status
   This should report all directories as [OK] and the database connection as OK.

## Running the Pipeline

The producer and consumer are independent processes, run in separate terminals - this matches how a real producer/consumer pair works in production, and is a deliberate design choice (see docs/engineering_decisions.md).

Terminal 1 - start the consumer:
   python main.py stream

Terminal 2 - start the producer:
   python main.py generator

Leave both running to observe continuous processing. Stop either with Ctrl+C.

To run the generator for a bounded number of files instead of indefinitely (useful for testing):
   python -c "import sys; sys.path.append('scripts'); from data_generator import run; run(max_iterations=10)"

## Verifying Results

   python main.py verify

Prints total event counts, rejection counts, constraint violation checks, duplicate checks, and the 5 most recent events.

## Running Tests

   python main.py test

## Resetting for a Fresh Test

**Important: only run this while the streaming job is stopped.** Running `python main.py clean` while `python main.py stream` is still active deletes the checkpoint directory out from under a live, running query - this was tested by accident during development and caused the streaming job to stop picking up new files correctly for the rest of that session. Stop the streaming job first (Ctrl+C), confirm it has exited, then run clean.



   python main.py clean

Removes generated/archived CSV files and clears the Spark checkpoint. This does NOT touch database rows. To also clear stored data, run this manually in PostgreSQL (it is a destructive action and is deliberately not automated - see docs/retention_policy.md):

   TRUNCATE TABLE events, rejected_events;

## Checking Environment Status

   python main.py status

Reports whether required directories exist, whether the database connection works, the current events row count, and how many files are currently pending in data/incoming/.

## Troubleshooting

- ModuleNotFoundError for pyspark, psycopg2, or faker: re-run pip install -r requirements.txt
- "No suitable driver found" when writing to Postgres: confirm the JDBC .jar path in spark_streaming.py's build_spark_session() matches where you actually downloaded it
- "HADOOP_HOME and hadoop.home.dir are unset": on Windows, this means the terminal running the command was opened before HADOOP_HOME was set - close it and open a fresh terminal
- Checkpoint files won't delete during python main.py clean: this is a known Windows file-locking issue (see docs/risks_and_limitations.md); close any open editors/processes that might be holding a lock on files under checkpoint/ and retry, or delete the folder manually