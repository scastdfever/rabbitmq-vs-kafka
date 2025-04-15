import json

from shared.message import Message


class MessageSerializer:
    @staticmethod
    def serialize_message(message: Message) -> bytes:
        return json.dumps(message.to_dict()).encode("utf-8")