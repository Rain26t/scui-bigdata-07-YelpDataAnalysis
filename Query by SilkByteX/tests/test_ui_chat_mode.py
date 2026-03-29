from __future__ import annotations

from yelp_text_to_sql import ui


def test_research_hypothesis_requests_resolve_to_general_chat() -> None:
    resolved_mode = ui._resolve_chat_mode(
        "Does weather affect restaurant ratings?",
        ui.CHAT_MODE_DATA,
    )

    assert resolved_mode == ui.CHAT_MODE_GENERAL


def test_research_hypothesis_requests_infer_general_chat_in_auto_mode() -> None:
    inferred_mode = ui._infer_auto_chat_mode("cursed storefronts")

    assert inferred_mode == ui.CHAT_MODE_GENERAL
