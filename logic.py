"""Bounded SQLite ledger owned by one node, with explicit optimistic concurrency."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time


DEFAULTS = {
    "namespace": "default", "initial_state": "pending",
    "transitions": {"pending": ["approved", "cancelled"], "approved": ["running", "cancelled"],
                    "running": ["completed", "failed", "cancelled"], "failed": ["pending"],
                    "completed": [], "cancelled": []},
    "max_records": 1000, "max_journal": 10000, "max_dedup": 20000,
    "retention_sec": 604800, "max_bytes": 131072,
}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class StateError(ValueError):
    """An invalid request or unavailable ledger, never a fabricated empty state."""


def identifier(value):
    """Restrict keys while preserving deterministic identity and SQL parameter binding."""
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise StateError("Identifiers must contain 1–128 ASCII letters, digits, dots, underscores, colons or hyphens.")
    return value


def encoded(value, limit):
    """Reject non-finite, oversized or deeply nested JSON before it reaches storage."""
    try:
        raw = json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))
        if len(raw.encode("utf-8")) > limit:
            raise StateError("JSON exceeds the configured byte limit.")
        stack = [(value, 0)]
        while stack:
            item, depth = stack.pop()
            if depth > 32:
                raise StateError("JSON nesting exceeds 32 levels.")
            if isinstance(item, dict):
                if any(not isinstance(key, str) for key in item):
                    raise StateError("JSON object keys must be strings.")
                stack.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                stack.extend((child, depth + 1) for child in item)
        return raw
    except (TypeError, RecursionError, OverflowError, UnicodeError) as exc:
        raise StateError("A bounded JSON value is required.") from exc


def document(value, limit):
    """Accept native objects or JSON text; disallow implicit defaults and extra actions."""
    if isinstance(value, str):
        if len(value.encode("utf-8")) > limit:
            raise StateError("JSON exceeds the configured byte limit.")
        try:
            value = json.loads(value)
        except (ValueError, RecursionError) as exc:
            raise StateError("Malformed JSON request.") from exc
    encoded(value, limit)
    if not isinstance(value, dict):
        raise StateError("A JSON object is required.")
    return value


def configuration(raw):
    """Validate trusted transition policy separately from incoming business events."""
    if not isinstance(raw, dict) or set(raw) - set(DEFAULTS) - {"execution", "runtime_path", "runtime_path_label"}:
        raise StateError("Unknown setting.")
    result = {**DEFAULTS, **{key: value for key, value in raw.items() if key in DEFAULTS}}
    identifier(result["namespace"])
    identifier(result["initial_state"])
    transitions = result["transitions"]
    if not isinstance(transitions, dict) or not 1 <= len(transitions) <= 64:
        raise StateError("transitions must contain 1–64 states.")
    for state, targets in transitions.items():
        identifier(state)
        if not isinstance(targets, list) or len(targets) > 64 or any(not isinstance(x, str) for x in targets):
            raise StateError("Each state requires an array of target states.")
        if len(set(targets)) != len(targets) or any(target not in transitions for target in targets):
            raise StateError("Every target must be a distinct declared state.")
    if result["initial_state"] not in transitions:
        raise StateError("initial_state must be declared in transitions.")
    for key, low, high in (("max_records", 1, 10000), ("max_journal", 1, 50000),
                            ("max_dedup", 1, 100000), ("retention_sec", 60, 7776000),
                            ("max_bytes", 1024, 262144)):
        value = result[key]
        if type(value) is not int or not low <= value <= high:
            raise StateError(f"{key} must be an integer between {low} and {high}.")
    return result


@contextmanager
def ledger(directory):
    """Serialize local writers; the framework alone chooses the canonical node directory."""
    directory = Path(directory)
    if not directory.is_absolute() or not directory.is_dir() or directory.is_symlink():
        raise StateError("The framework did not provide an existing canonical storage directory.")
    path = directory / "state.sqlite3"
    if path.is_symlink():
        raise StateError("The state database must not be a symbolic link.")
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        if not path.is_file():
            raise StateError("The state database is not a regular file.")
    else:
        os.close(fd)
    db = sqlite3.connect(path, timeout=2.0)
    try:
        db.execute("PRAGMA busy_timeout=2000")
        db.execute("PRAGMA max_page_count=32768")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            raise StateError("Unsupported state database schema; no automatic reset or migration.")
        db.execute("CREATE TABLE IF NOT EXISTS records (namespace TEXT, id TEXT, revision INTEGER, state TEXT, data TEXT, updated REAL, PRIMARY KEY(namespace,id))")
        db.execute("CREATE TABLE IF NOT EXISTS dedup (namespace TEXT, id TEXT, fingerprint TEXT, created REAL, PRIMARY KEY(namespace,id))")
        db.execute("CREATE TABLE IF NOT EXISTS journal (seq INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT, created REAL, event TEXT)")
        db.execute("CREATE INDEX IF NOT EXISTS journal_namespace ON journal(namespace,seq)")
        db.execute("PRAGMA user_version=1")
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def snapshot(db, namespace, record_id):
    """Return a detached record with its authoritative monotonic revision."""
    row = db.execute("SELECT revision,state,data,updated FROM records WHERE namespace=? AND id=?", (namespace, record_id)).fetchone()
    if row is None:
        return None
    return {"record_id": record_id, "revision": row[0], "state": row[1], "data": json.loads(row[2]), "updated_at": row[3]}


def process(value, port, config, directory, cancel=lambda: False, now=None):
    """Apply one event atomically, or read without mutating any business record."""
    config = configuration(config)
    request = document(value, config["max_bytes"])
    now = time.time() if now is None else float(now)
    namespace = config["namespace"]
    record_id = identifier(request.get("record_id"))
    if port not in {"event", "query"}:
        raise StateError("Unknown input port.")
    if cancel():
        raise StateError("Operation cancelled.")
    with ledger(directory) as db:
        state = snapshot(db, namespace, record_id)
        if port == "query":
            if set(request) - {"record_id", "history", "limit"} or type(request.get("history", False)) is not bool:
                raise StateError("Invalid query fields.")
            limit = request.get("limit", 20)
            if type(limit) is not int or not 1 <= limit <= 100:
                raise StateError("Query limit must be between 1 and 100.")
            # Read a bounded suffix, then filter locally to avoid requiring SQLite JSON extensions.
            rows = db.execute("SELECT event FROM journal WHERE namespace=? ORDER BY seq DESC LIMIT ?", (namespace, config["max_journal"])).fetchall() if request.get("history") else []
            history = [json.loads(row[0]) for row in rows if json.loads(row[0])["record_id"] == record_id][:limit]
            return {"state": {"namespace": namespace, "record_id": record_id, "record": state, "history": list(reversed(history))}}
        if set(request) - {"id", "record_id", "operation", "expected_revision", "state", "data"}:
            raise StateError("Unknown event field.")
        event_id = identifier(request.get("id"))
        operation = request.get("operation")
        revision = request.get("expected_revision")
        if operation not in {"create", "transition", "patch"} or type(revision) is not int or revision < 0:
            raise StateError("An explicit operation and non-negative expected_revision are required.")
        fingerprint = hashlib.sha256(encoded(request, config["max_bytes"]).encode()).hexdigest()
        db.execute("DELETE FROM dedup WHERE namespace=? AND created<?", (namespace, now - config["retention_sec"]))
        db.execute("DELETE FROM journal WHERE namespace=? AND created<?", (namespace, now - config["retention_sec"]))
        previous = db.execute("SELECT fingerprint FROM dedup WHERE namespace=? AND id=?", (namespace, event_id)).fetchone()
        base = {"namespace": namespace, "record_id": record_id, "record": state}

        def conflict(code):
            return {"state": base, "conflicts": [{"id": event_id, "record_id": record_id, "code": code,
                    "expected_revision": revision, "actual_revision": state["revision"] if state else 0}]}

        if previous:
            return conflict("duplicate" if previous[0] == fingerprint else "event_id_reused")
        if revision != (state["revision"] if state else 0):
            return conflict("revision_conflict")
        if operation == "create":
            if state is not None:
                return conflict("already_exists")
            if "state" in request or not isinstance(request.get("data", {}), dict):
                raise StateError("Create accepts data but uses the configured initial_state.")
            count = db.execute("SELECT count(*) FROM records WHERE namespace=?", (namespace,)).fetchone()[0]
            if count >= config["max_records"]:
                return conflict("record_capacity")
            next_state, data = config["initial_state"], request.get("data", {})
        else:
            if state is None:
                return conflict("not_found")
            data, next_state = state["data"], state["state"]
            if operation == "transition":
                if "data" in request:
                    raise StateError("Use patch to edit data; transition changes state only.")
                target = identifier(request.get("state"))
                if target not in config["transitions"].get(state["state"], []):
                    return conflict("transition_refused")
                next_state = target
            else:
                if "state" in request or not isinstance(request.get("data"), dict):
                    raise StateError("Patch requires a JSON object data and no state field.")
                data = {**data, **request["data"]}
        data_json = encoded(data, config["max_bytes"])
        if db.execute("SELECT count(*) FROM dedup WHERE namespace=?", (namespace,)).fetchone()[0] >= config["max_dedup"]:
            return conflict("dedup_capacity")
        if cancel():
            raise StateError("Operation cancelled.")
        new_revision = revision + 1
        db.execute("INSERT INTO records VALUES (?,?,?,?,?,?) ON CONFLICT(namespace,id) DO UPDATE SET revision=excluded.revision,state=excluded.state,data=excluded.data,updated=excluded.updated",
                   (namespace, record_id, new_revision, next_state, data_json, now))
        event = {"id": event_id, "record_id": record_id, "operation": operation, "revision": new_revision,
                 "previous_state": state["state"] if state else None, "state": next_state, "at": now}
        db.execute("INSERT INTO dedup VALUES (?,?,?,?)", (namespace, event_id, fingerprint, now))
        db.execute("INSERT INTO journal(namespace,created,event) VALUES (?,?,?)", (namespace, now, encoded(event, 2048)))
        db.execute("DELETE FROM journal WHERE namespace=? AND seq NOT IN (SELECT seq FROM journal WHERE namespace=? ORDER BY seq DESC LIMIT ?)",
                   (namespace, namespace, config["max_journal"]))
        return {"state": {**base, "record": snapshot(db, namespace, record_id)}, "events": [event]}
