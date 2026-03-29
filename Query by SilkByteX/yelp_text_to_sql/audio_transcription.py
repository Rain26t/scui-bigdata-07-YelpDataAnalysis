from __future__ import annotations

from dataclasses import dataclass

DEEPSEEK_TRANSCRIPTION_NOTE = (
    "Voice transcription is disabled in this DeepSeek-only build. "
    "DeepSeek SQL generation is active, but the voice feature still needs a separate speech-to-text provider."
)


@dataclass
class AudioTranscriptionResult:
    text: str
    raw_response: str
    notes: str
    model: str = "Unavailable"


def transcribe_audio_bytes(
    audio_bytes: bytes,
    *,
    filename: str = "voice-question.wav",
    content_type: str = "audio/wav",
) -> AudioTranscriptionResult:
    """Return a clear note because DeepSeek-only mode does not provide transcription here."""
    _ = (audio_bytes, filename, content_type)
    return AudioTranscriptionResult(
        text="",
        raw_response="",
        notes=DEEPSEEK_TRANSCRIPTION_NOTE,
        model="Unavailable",
    )
