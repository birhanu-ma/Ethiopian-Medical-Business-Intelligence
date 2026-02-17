from dataclasses import dataclass, asdict
from typing import Optional

@dataclass(slots=True)
class TelegramMessage:
    """
    Unified schema for the medical_warehouse ingestion pipeline.
    Validates data structure before it hits the PostgreSQL 'raw' schema.
    """
    message_id: int
    channel_name: str
    message_text: str
    views: int = 0
    forwards: int = 0
    message_date: Optional[str] = None
    has_media: bool = False
    image_path: Optional[str] = None

    def to_dict(self) -> dict:
        """Converts dataclass to dict for JSON serialization or DB insertion."""
        return asdict(self)