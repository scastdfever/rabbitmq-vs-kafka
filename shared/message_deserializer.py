import json
from typing import Any


class MessageDeserializer:
    @staticmethod
    def deserialize_message(message_bytes: bytes) -> dict[str, Any]:
        return json.loads(message_bytes.decode("utf-8"))
