"""FB4/FB5: real per-instance persistent storage in both runtimes and package origins."""

from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from blocs.business_state.block import BusinessStateBlock
from bloxsmith_app.block_api import BlockRuntimeContext
from block_test_packages import install_test_package, surface_payload
from ui_smoke_common import create_project_api, create_run_api, data_edge, display_node, graph_payload, isolated_server, text_node, wait_for_run_terminal


def main():
    """A real scoped graph must acquire storage through the service, not a guessed path."""
    block = BusinessStateBlock()
    assert block.execute_runtime(BlockRuntimeContext(config={})).status == "skipped"
    for mode in ("centralized", "zeromq_active"):
        for origin in (None, "managed", "linked"):
            with isolated_server() as server:
                node = block.build_node_payload(node_id="ledger", position={"x": 360, "y": 140})
                if origin:
                    model = install_test_package(server, "business_state", origin=origin)
                    node["block_version"] = model["version"]
                    for surface in ("modal", "inspector_panel", "node_card"):
                        html = surface_payload(server, model, node, surface=surface)["html"]
                        assert "{{" not in html
                        if surface != "node_card":
                            assert 'data-block-config-field="transitions"' in html
                event = {"id": "event-1", "record_id": "case-1", "operation": "create", "expected_revision": 0, "data": {"title": "Fixture"}}
                document = graph_payload("Business state", [text_node("source", "Event", json.dumps(event), 80, 140),
                    node, display_node("display", "State", 700, 140)], [
                    data_edge("source-state", "source", 1, "ledger", 1), data_edge("state-display", "ledger", 1, "display", 1)])
                project = create_project_api(server, document=document)["project"]
                run = create_run_api(server, document, project_id=project["graph_id"], runtime_mode=mode)
                result = wait_for_run_terminal(server, run["run_id"], timeout_sec=30)
                assert result["status"] == "success", (mode, origin, result.get("logs"))
                value = json.loads(result["output_values"]["ledger:1"]["value"])
                assert value["record"]["state"] == "pending" and value["record"]["revision"] == 1
                assert json.loads(result["output_values"]["ledger:2"]["value"])[0]["record_id"] == "case-1"
    print("[ok] Business State runtime and surface contracts")


if __name__ == "__main__":
    main()
