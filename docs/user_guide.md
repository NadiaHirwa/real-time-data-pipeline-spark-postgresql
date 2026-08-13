# User Guide

Step-by-step instructions to set up and run this project from a fresh clone.

There are two ways to run this project, using the same application code. The native setup below is how the project was developed and benchmarked. If you would rather install nothing but Docker Desktop, skip ahead to [Running via Docker](#running-via-docker) - none of the prerequisites in the next section apply to that path.

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

Runs the entire suite, including the integration tests that need a live PostgreSQL connection.

For a fast sanity check that needs no database at all, run pytest directly instead (not through main.py):

   pytest -m "not integration"

That skips the tests in tests/test_integration.py, which are the only ones requiring a real database. Useful while iterating on validation logic, or on a machine where Postgres is not running. `python main.py test` is unchanged and still runs everything.

## Resetting for a Fresh Test

**Important: only run either of these while the streaming job is stopped.** Running `python main.py clean` while `python main.py stream` is still active deletes the checkpoint directory out from under a live, running query - this was tested by accident during development and caused the streaming job to stop picking up new files correctly for the rest of that session. Stop the streaming job first (Ctrl+C), confirm it has exited, then run clean or reset.

There are two levels of reset:

**`python main.py clean`** - local files only.

   python main.py clean

Removes generated/archived CSV files and clears the Spark checkpoint. This does NOT touch database rows, guaranteed - that is why clearing the tables is a separate command rather than a flag on this one.

**`python main.py reset`** - local files AND all database tables.

   python main.py reset

Does everything clean does, then truncates events, rejected_events, staging_events, and stream_metrics. Because this permanently destroys stored data, it asks for confirmation first and only proceeds if you type `yes`. To skip the prompt in a script or CI context:

   python main.py reset --force

If the database cannot be reached, reset stops before deleting anything at all - including local files - rather than leaving you with wiped CSVs and a still-full database.

Prefer doing it by hand in pgAdmin? The equivalent SQL is still perfectly valid:

   TRUNCATE TABLE events, rejected_events, staging_events, stream_metrics;

## Checking Environment Status

   python main.py status

Reports whether required directories exist, whether the database connection works, the current events row count, and how many files are currently pending in data/incoming/.

## Running via Docker

A fully containerized alternative to everything above: PostgreSQL, the Spark/Python application, and Adminer all run in containers. This is an ADDITIONAL way to run the project, not a replacement - the native setup above is unchanged and still works, and both use the same application code.

### Prerequisites

Docker Desktop, installed and running. That is the entire list. No Python, Java, Spark, PostgreSQL, JDBC driver, or winutils.exe is needed on the host for this path - the image contains all of it.

### Setup

1. Copy .env.example to .env and fill in your credentials, if you have not already:
   cp .env.example .env

   This is the same .env the native setup uses; the two paths share it. Leave DB_HOST=localhost as-is - docker-compose.yml overrides it to `postgres` for the app container automatically (see docs/architecture.md for why).

2. Build the application image:
   docker compose build

   The first build takes several minutes, mostly downloading PySpark. Later builds reuse the cached dependency layer and take seconds unless requirements.txt changed.

3. Start PostgreSQL and wait for it to report healthy:
   docker compose up -d postgres
   docker compose ps

   Wait until STATUS shows `Up (healthy)`, not just `Up`. On first start the container also creates the schema automatically by running sql/postgres_setup_ci.sql - all four tables, the indexes, and the three views.

4. Start the rest of the stack and confirm the app can reach the database:
   docker compose up -d
   docker compose exec app python main.py status

   This should report all directories as [OK] and the database connection as OK, exactly as the native path does.

### Running the Pipeline

Same producer/consumer split as the native setup, just wrapped in `docker compose exec`.

Terminal 1 - start the consumer:
   docker compose exec app python main.py stream

Terminal 2 - start the producer:
   docker compose exec app python main.py generator

For a bounded number of files instead of indefinitely:
   docker compose exec app python -c "import sys; sys.path.append('scripts'); from data_generator import run; run(max_iterations=10)"

Generated and archived CSV files are visible on the host under data/, since that directory is bind-mounted into the container - so you can inspect them normally while the pipeline runs.

### Verifying Results

   docker compose exec app python main.py verify

### Running Tests

   docker compose exec app python main.py test

All 34 tests run inside the container, including the three integration tests against the containerized PostgreSQL. They run noticeably faster than natively, because the JDBC driver is already baked into the image and needs no Maven resolution.

### Stopping the Stream Cleanly - Read This One

**Ctrl+C on a `docker compose exec` session does NOT stop the process inside the container.** It detaches your terminal from it; the streaming job keeps running. This is not hypothetical - it was hit during testing of this setup, and the symptom is confusing: the next `docker compose exec app python main.py stream` fails with

   Concurrent update to the log. Multiple streaming jobs detected

which reads like a checkpoint corruption problem but actually means the first stream is still alive and holding the checkpoint.

Stop it from inside the container instead:

   docker compose exec app pkill -f "main[.]py str[e]am"

The bracketed characters are deliberate. A plain `pkill -f "main.py stream"` matches the command line of the shell running the pkill itself, so it kills its own session before the target - the brackets make the pattern not match itself while still matching the real process.

To confirm nothing is left running:

   docker compose exec app ps -eo pid,args | grep "main[.]py str[e]am"

No output means the stream is stopped and it is safe to start a new one. The same applies before running `python main.py clean` in a container - see the warning in "Resetting for a Fresh Test" above, which applies identically here.

### Browsing the Database with Adminer

Adminer is a lightweight browser-based SQL client, included as an alternative to pgAdmin so nothing needs installing on the host. Browse to:

   http://localhost:8080

(or whichever port ADMINER_HOST_PORT is set to - see below). The server field is pre-filled with `postgres`; log in with the DB_USER, DB_PASSWORD, and DB_NAME values from your .env.

pgAdmin still works too - point it at localhost and the published Postgres port.

### Port Conflicts

If port 5432 or 8080 is already in use on the host - a natively-installed PostgreSQL already occupies 5432, and another project's Adminer may already occupy 8080 - `docker compose up` fails with a "port is already allocated" error.

Fix it by setting either or both of these in .env:

   POSTGRES_HOST_PORT=5433
   ADMINER_HOST_PORT=8081

Only the HOST side of the mapping changes. The container-internal ports are always 5432 and 8080, so nothing inside the compose network is affected and no other setting needs updating. Remember to point pgAdmin (and Adminer's URL) at the new port.

### Resetting and Data Persistence

   docker compose down

This removes the containers and the network but deliberately KEEPS the named volumes, `postgres_data` and `spark_checkpoint`. Database rows and the Spark checkpoint therefore survive a full down/up cycle - `docker compose up -d` afterwards comes back with all previous data intact, and the schema init script does not re-run (it only runs when the data volume is empty).

To wipe everything, including all stored rows and the checkpoint:

   docker compose down -v

The `-v` deletes the named volumes. This is irreversible: the next `up` starts from a completely empty database and re-runs the schema init script from scratch. Use it when you genuinely want a clean slate, not as a routine stop command.

For a lighter reset that clears generated CSVs and the checkpoint but leaves database rows alone, the normal clean command works inside the container too (stop the stream first, as described above):

   docker compose exec app python main.py clean

## Troubleshooting

- ModuleNotFoundError for pyspark, psycopg2, or faker: re-run pip install -r requirements.txt
- "No suitable driver found" when writing to Postgres: confirm the JDBC .jar path in spark_streaming.py's build_spark_session() matches where you actually downloaded it
- "HADOOP_HOME and hadoop.home.dir are unset": on Windows, this means the terminal running the command was opened before HADOOP_HOME was set - close it and open a fresh terminal
- Checkpoint files won't delete during python main.py clean: this is a known Windows file-locking issue (see docs/risks_and_limitations.md); close any open editors/processes that might be holding a lock on files under checkpoint/ and retry, or delete the folder manually