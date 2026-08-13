# Makefile
#
# A thin convenience layer over the commands this project already has.
# Every target below simply calls main.py or docker compose underneath -
# there is no logic here, and nothing works differently through make than
# it does when typed out by hand. main.py remains the real entry point
# (see its docstring); this exists so the common commands are shorter to
# type and discoverable in one place via `make help`.
#
# Both supported ways of running this project are covered, matching the
# two sections of docs/user_guide.md:
#   - Native targets  (generator, stream, verify, ...) run on the host
#   - docker-* targets run the same commands inside the app container
#
# Requires GNU make. On Windows it is available through Git Bash,
# Chocolatey (choco install make), or WSL.

# Overridable, so the same Makefile works where the interpreter is named
# differently - e.g. `make test PYTHON=python3` on macOS/Linux.
PYTHON ?= python
COMPOSE ?= docker compose

# Plain `make` with no arguments shows the help, rather than silently
# running whichever target happens to be defined first.
.DEFAULT_GOAL := help

# Every target is a command, not a file to be built. Without this, make
# would skip any target whose name matched an existing file or directory
# - `clean` and `test` are exactly the names most likely to collide.
.PHONY: help \
        generator stream verify test test-fast clean reset status \
        docker-build docker-up docker-down docker-logs \
        docker-status docker-stream docker-generator docker-verify \
        docker-test docker-reset

# --- Help ---------------------------------------------------------------

# Every line is quoted deliberately. make runs recipes through a shell,
# and an unquoted parenthesis - as in "(producer)" - is a syntax error in
# sh, while a bare `echo.` prints a literal dot rather than a blank line.
help:
	@echo ""
	@echo "Real-Time E-Commerce Streaming Pipeline"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "NATIVE - runs on this machine, needs Python/Java/Spark/PostgreSQL installed:"
	@echo "  generator         Run the data generator (producer)"
	@echo "  stream            Run the Spark Structured Streaming job (consumer)"
	@echo "  verify            Run the SQL verification queries against PostgreSQL"
	@echo "  test              Run the full pytest suite, including integration tests"
	@echo "  test-fast         Run only tests that need no database - quick sanity check"
	@echo "  clean             Remove generated/archived CSVs and clear the checkpoint"
	@echo "  reset             Everything clean does, plus truncating all tables - asks first"
	@echo "  status            Report directory and database readiness"
	@echo ""
	@echo "DOCKER - runs in containers, needs only Docker Desktop:"
	@echo "  docker-build      Build the application image"
	@echo "  docker-up         Start PostgreSQL and wait for it to be healthy"
	@echo "  docker-down       Stop and remove containers - named volumes are kept"
	@echo "  docker-logs       Follow container logs"
	@echo "  docker-status     Report readiness from inside the app container"
	@echo "  docker-stream     Run the streaming job inside the app container"
	@echo "  docker-generator  Run the data generator inside the app container"
	@echo "  docker-verify     Run the verification queries inside the app container"
	@echo "  docker-test       Run the full test suite inside the app container"
	@echo "  docker-reset      Clean and truncate all tables inside the container - asks first"
	@echo ""
	@echo "Full instructions for both paths: docs/user_guide.md"
	@echo ""

# --- Native targets -----------------------------------------------------
# Straight pass-throughs to main.py's subcommands.

generator:
	$(PYTHON) main.py generator

stream:
	$(PYTHON) main.py stream

verify:
	$(PYTHON) main.py verify

test:
	$(PYTHON) main.py test

# The one target that does not go through main.py, because main.py test
# deliberately always runs everything. This is the marker-based subset
# registered in pytest.ini - it skips tests/test_integration.py, the only
# tests needing a live PostgreSQL.
test-fast:
	$(PYTHON) -m pytest -m "not integration"

clean:
	$(PYTHON) main.py clean

# Deliberately NOT passed --force. reset destroys stored data, and its
# confirmation prompt is a safety feature, not friction to be smoothed
# away by a shortcut. Anyone who genuinely wants it non-interactive can
# still run `python main.py reset --force` directly.
reset:
	$(PYTHON) main.py reset

status:
	$(PYTHON) main.py status

# --- Docker targets -----------------------------------------------------
# The same commands, run inside the app container. These assume the stack
# is already up (make docker-up, then docker compose up -d app).

docker-build:
	$(COMPOSE) build

# Starts PostgreSQL only, matching the documented setup flow - the app
# container is brought up afterwards, once Postgres reports healthy.
docker-up:
	$(COMPOSE) up -d postgres

# Containers and network are removed; postgres_data and spark_checkpoint
# are named volumes and deliberately survive. Use `docker compose down -v`
# by hand to wipe those too - not exposed here, since it is irreversible.
docker-down:
	$(COMPOSE) down

docker-logs:
	$(COMPOSE) logs -f

docker-status:
	$(COMPOSE) exec app $(PYTHON) main.py status

docker-stream:
	$(COMPOSE) exec app $(PYTHON) main.py stream

docker-generator:
	$(COMPOSE) exec app $(PYTHON) main.py generator

docker-verify:
	$(COMPOSE) exec app $(PYTHON) main.py verify

docker-test:
	$(COMPOSE) exec app $(PYTHON) main.py test

docker-reset:
	$(COMPOSE) exec app $(PYTHON) main.py reset
