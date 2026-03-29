from __future__ import annotations

from yelp_text_to_sql import audio_transcription


def test_transcribe_audio_bytes_returns_deepseek_disabled_note() -> None:
    result = audio_transcription.transcribe_audio_bytes(b"RIFFDATA")

    assert result.text == ""
    assert "DeepSeek-only build" in result.notes
    assert result.model == "Unavailable"
