# Dockerfile
#
# Containerized runtime for the Spark/Python side of this pipeline.
# This is an ADDITIONAL way to run the project - the native Windows
# setup documented in docs/user_guide.md is unchanged and still works.
#
# Base pinned to bookworm explicitly rather than tracking plain
# python:3.13-slim: that tag rolls forward to newer Debian releases,
# and OpenJDK 17 is what bookworm packages. A silent base bump could
# drop the package and break this build with no change on our side -
# the same class of problem as pinning postgres:16 instead of :latest.
FROM python:3.13-slim-bookworm

# Spark 4.2 runs on Java 17 or 21; 17 is bookworm's packaged version.
# procps is not required by Spark but makes the container debuggable
# (ps, top) when inspecting a running stream via docker compose exec.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# The PostgreSQL JDBC driver, baked in at BUILD time rather than
# resolved from Maven on every container start. Deterministic, works
# offline, and removes several seconds of Ivy resolution from startup.
# ADD fetches the URL directly, so no curl/wget install is needed.
#
# This version MUST match POSTGRES_JDBC_COORDINATES in
# scripts/spark_streaming.py - both refer to the same driver, one for
# the native/Maven path and one for this baked-in path.
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    /opt/spark-jars/postgresql-42.7.3.jar
RUN chmod 0644 /opt/spark-jars/postgresql-42.7.3.jar

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Read by config.py. Its PRESENCE is what tells build_spark_session()
# to use the baked-in jar instead of fetching from Maven. It is unset
# on the native setup, which therefore behaves exactly as it did before
# this image existed.
ENV POSTGRES_JDBC_JAR=/opt/spark-jars/postgresql-42.7.3.jar

WORKDIR /app

# requirements.txt first, as its own layer: dependencies (pyspark is
# a large install) are only re-downloaded when requirements.txt itself
# changes, not on every edit to scripts/.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# The pipeline's working directories. data/ and checkpoint/ are mounted
# over by docker-compose, but creating them here means the image also
# works standalone via plain docker run, and gives the checkpoint named
# volume a valid empty directory to initialize from.
RUN mkdir -p \
        /app/data/incoming \
        /app/data/processed_archive \
        /app/data/rejected \
        /app/checkpoint \
        /app/logs

# Deliberately does NOT start the streaming job. The generator and the
# stream are separate services run on demand (see main.py's docstring),
# so the container stays alive and idle, ready for:
#   docker compose exec app python main.py <subcommand>
CMD ["sleep", "infinity"]
