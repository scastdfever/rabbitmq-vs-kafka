from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Message:
    id: int
    content: str
    timestamp: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "timestamp": self.timestamp,
        }
