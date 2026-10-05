"""Revision-controlled state scoped to the framework's persistent node directory."""

import json
import sqlite3
from bloxsmith_app.block_api import APPLICATION_JSON, BlockDefinition, BlockRuntimeOutput, BlockRuntimeResult
from . import logic, ui


# FB1 - Persist records and enforce explicit revision checks and transition rules atomically.
# FB2 - Separate accepted events from conflicts and reject replayed event identifiers.
# FB3 - Bound storage, JSON, retention and diagnostics without silently resetting corrupt state.
# FB4 - Preserve event/query routing and storage identity in both runtimes and package origins.
# FB5 - Provide consistent translated, responsive modal/inspector drafts and owned assets.
class BusinessStateBlock(BlockDefinition):
    """Business records are not Codex sessions or an implicit global database."""

    kind = "business_state"

    def ui_assets(self, surface="modal"):
        """Use the static release manifest in every installation mode."""
        return list(self.model.get("ui_assets", {}).get(surface, []))

    def execute_runtime(self, context):
        """Consume new events, never replay a stale event merely because query changed."""
        try:
            config = logic.configuration(context.config)
            if context.input_events:
                ports = {int(port.id): str(port.name) for port in context.input_ports}
                messages = [(ports.get(event.input_port_id), event.value) for event in context.input_events]
            else:
                messages = [(name, context.input_value(name, ident)) for name, ident in (("event", "1"), ("query", "2"))
                            if context.has_input_value(name, ident)]
                if len(messages) > 1:
                    raise logic.StateError("Manual execution with two populated inputs is ambiguous; send one event or query.")
            if not messages:
                return BlockRuntimeResult(status="skipped", outputs=[])
            if len(messages) > 100:
                raise logic.StateError("Too many input events in one activation.")
            resolver = context.services.get("get_block_storage_dir")
            if not callable(resolver):
                raise logic.StateError("Persistent block storage is unavailable; no fallback directory is used.")
            directory = resolver()
            values = {}
            for port, value in messages:
                result = logic.process(value, port, config, directory,
                                       context.services.get("cancel_requested", lambda: False))
                values["state"] = result["state"]
                for name in ("events", "conflicts"):
                    if name in result:
                        values.setdefault(name, []).extend(result[name])
            outputs = [BlockRuntimeOutput(port_id=int(port.id), port_name=port.name,
                       value=json.dumps(values[port.name], ensure_ascii=False, allow_nan=False), content_type=APPLICATION_JSON)
                       for port in context.output_ports if port.name in values]
            return BlockRuntimeResult(status="success", outputs=outputs, content_type=APPLICATION_JSON,
                                      metadata={self.kind: {"accepted": len(values.get("events", [])),
                                                           "conflicts": len(values.get("conflicts", []))}},
                                      logs=["[business-state] State operation completed."])
        except (ValueError, TypeError, RecursionError, RuntimeError, OSError, sqlite3.Error) as exc:
            detail = str(exc) if isinstance(exc, logic.StateError) else "Persistent state is unavailable or invalid. No state was reset."
            message = self.translate("block.business_state.error", {"detail": detail}, fallback="State operation failed: {detail}")
            return BlockRuntimeResult(status="failed", outputs=[], error=message, exit_code=1,
                                      logs=[f"[business-state-error] {detail}"])

    def handle_ui_action(self, *, node, action, values, payload=None):
        """Validate settings without opening the state database from a UI action."""
        if action in {"save_settings", "modal_update_fields", "inspector_update_fields"}:
            patch = (values or {}).get("node_patch") or {}
            if "config" in patch:
                try:
                    logic.configuration({**self.default_config(), **(node.get("config") or {}), **patch["config"]})
                except (ValueError, TypeError) as exc:
                    return {"error": self.translate("block.business_state.error", {"detail": str(exc)}, fallback="State operation failed: {detail}")}
        result = super().handle_ui_action(node=node, action="modal_update_fields" if action == "save_settings" else action, values=values, payload=payload)
        return ui.replace_config(result, node)

    def render_modal(self, *, node, payload=None):
        """Render settings without altering or disclosing the stored records."""
        return ui.modal(self, node, payload)

    def render_inspector_panel(self, *, node, payload=None):
        """Expose the same bounded policy in the narrow inspector."""
        return ui.inspector(self, node, payload)

    def render_node_card(self, *, node, payload=None):
        """Show the namespace, not private business data, on the canvas."""
        return ui.card(self, node, (node.get("config") or {}).get("namespace", "default"))
