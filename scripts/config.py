"""
config.py

Centralized configuration for the entire pipeline. Every script reads
settings from here rather than calling os.getenv() directly, so there
is exactly one place to see (and change) every configurable value.

Values are read from a .env file at the project root (see .env.example
for the full list of expected variables). Sensible defaults are
provided for anything not set, so the pipeline can still run locally
with minimal setup.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _get_path(env_var: str, default: str) -> Path:
    """Resolve a configured relative path against the project root."""
    return PROJECT_ROOT / os.getenv(env_var, default)


# --- Database ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_events")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Pipeline paths ---
INCOMING_DIR = _get_path("INCOMING_DIR", "data/incoming")
ARCHIVE_DIR = _get_path("ARCHIVE_DIR", "data/processed_archive")
REJECTED_DIR = _get_path("REJECTED_DIR", "data/rejected")
CHECKPOINT_DIR = _get_path("CHECKPOINT_DIR", "checkpoint")
LOG_DIR = _get_path("LOG_DIR", "logs")

# --- Spark streaming tuning ---
MAX_FILES_PER_TRIGGER = int(os.getenv("MAX_FILES_PER_TRIGGER", "5"))
TRIGGER_INTERVAL_SECONDS = int(os.getenv("TRIGGER_INTERVAL_SECONDS", "5"))

# --- Data generator tuning ---
GENERATOR_INTERVAL_SECONDS = int(os.getenv("GENERATOR_INTERVAL_SECONDS", "3"))
GENERATOR_EVENTS_PER_FILE = int(os.getenv("GENERATOR_EVENTS_PER_FILE", "20"))

# --- Data contract (see docs/data_contract.md - kept here as the single
#     source of truth that both the generator and Spark validation can
#     import, rather than duplicating these values in two places) ---
ALLOWED_EVENT_TYPES = ["view", "purchase"]
MAX_PRICE = 10_000.00
MAX_QUANTITY = 100
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def ensure_directories() -> None:
    """
    Create every directory the pipeline writes to, if it doesn't
    already exist. Called once at the start of both the generator and
    the streaming job, so a fresh clone of this repo runs correctly
    without manual setup.
    """
    for directory in [INCOMING_DIR, ARCHIVE_DIR, REJECTED_DIR, CHECKPOINT_DIR, LOG_DIR]:
        directory.mkdir(parents=True, exist_ok=True)