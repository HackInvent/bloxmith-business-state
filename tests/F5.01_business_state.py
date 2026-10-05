"""FB1/FB2/FB3: atomic transitions, correlation, persistence, bounds and failure isolation."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from blocs.business_state import logic


def rejected(call):
    """Require malformed requests to fail, rather than replacing state with defaults."""
    try:
        call()
    except (logic.StateError, ValueError):
        return
    raise AssertionError("Invalid request was accepted")


def main():
    """Use isolated storage only; no test depends on workstation directories."""
    with TemporaryDirectory(prefix="business-state-") as temporary:
        directory = Path(temporary).resolve()
        config = logic.configuration({})
        event = {"id": "event-1", "record_id": "case-1", "operation": "create", "expected_revision": 0, "data": {"total": 20}}
        first = logic.process(event, "event", config, directory, now=1000)
        assert first["state"]["record"]["revision"] == 1 and first["events"][0]["state"] == "pending"
        replay = logic.process(event, "event", config, directory, now=1001)
        assert "events" not in replay and replay["conflicts"][0]["code"] == "duplicate"
        reused = logic.process({**event, "data": {}}, "event", config, directory, now=1001)
        assert reused["conflicts"][0]["code"] == "event_id_reused"
        assert logic.process({"record_id": "case-1"}, "query", config, directory)["state"]["record"]["data"] == {"total": 20}
        transition = {"id": "event-2", "record_id": "case-1", "operation": "transition", "expected_revision": 1, "state": "approved"}
        bad = logic.process({**transition, "state": "completed"}, "event", config, directory, now=1002)
        assert bad["conflicts"][0]["code"] == "transition_refused" and "events" not in bad
        moved = logic.process(transition, "event", config, directory, now=1002)
        assert moved["state"]["record"]["revision"] == 2
        stale = logic.process({**transition, "id": "stale"}, "event", config, directory, now=1002)
        assert stale["conflicts"][0]["code"] == "revision_conflict"
        patch = {"id": "patch-1", "record_id": "case-1", "operation": "patch", "expected_revision": 2, "data": {"owner": "test"}}
        patched = logic.process(patch, "event", config, directory, now=1003)
        assert patched["state"]["record"]["data"] == {"total": 20, "owner": "test"}
        history = logic.process({"record_id": "case-1", "history": True, "limit": 2}, "query", config, directory)
        assert len(history["state"]["history"]) == 2 and history["state"]["history"][-1]["id"] == "patch-1"
        assert logic.process({"record_id": "case-1"}, "query", {**config, "namespace": "other"}, directory)["state"]["record"] is None
        rejected(lambda: logic.process({**patch, "id": "bad", "data": float("nan")}, "event", config, directory))
        rejected(lambda: logic.process(event, "event", config, directory, cancel=lambda: True))
        rejected(lambda: logic.process({"record_id": "case-1", "limit": 101}, "query", config, directory))
        rejected(lambda: logic.configuration({"namespace": "../escape"}))
        rejected(lambda: logic.configuration({"transitions": {"pending": ["missing"]}}))
        rejected(lambda: logic.configuration({"max_records": True}))
        rejected(lambda: logic.configuration({"unknown": 1}))
        assert logic.configuration({"runtime_path": "state", "runtime_path_label": "State", "execution": {}})["namespace"] == "default"
        one = {**config, "namespace": "small", "max_records": 1, "max_dedup": 2, "max_journal": 1, "retention_sec": 60}
        logic.process(event, "event", one, directory, now=1000)
        full = logic.process({**event, "id": "another", "record_id": "case-2"}, "event", one, directory, now=1001)
        assert full["conflicts"][0]["code"] == "record_capacity"
        logic.process(transition, "event", one, directory, now=1002)
        full = logic.process(patch, "event", one, directory, now=1003)
        assert full["conflicts"][0]["code"] == "dedup_capacity"
        after_expiry = logic.process(patch, "event", one, directory, now=1063)
        assert after_expiry["state"]["record"]["revision"] == 3
        assert len(logic.process({"record_id": "case-1", "history": True}, "query", one, directory)["state"]["history"]) == 1
        # Independent connections race; only one expected revision may be committed.
        def race(index):
            return logic.process({**patch, "id": "race-" + str(index), "expected_revision": 3}, "event", config, directory, now=1004)
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(race, (1, 2)))
        assert sum("events" in result for result in results) == 1
        assert sum("conflicts" in result for result in results) == 1
        assert logic.process({"record_id": "case-1"}, "query", config, directory)["state"]["record"]["revision"] == 4
        assert (directory / "state.sqlite3").stat().st_mode & 0o777 == 0o600
        # No credentials, arbitrary paths or SQL strings are interpreted as policy.
        rejected(lambda: logic.process({**event, "record_id": "'; DROP TABLE records;--"}, "event", config, directory))
    print("[ok] Business State functional behavior")


if __name__ == "__main__":
    main()
