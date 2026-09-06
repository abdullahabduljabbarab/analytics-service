from pydantic import BaseModel


class PubSubPush(BaseModel):
    """The envelope Pub/Sub wraps a pushed message in. `message.data` is the
    base64-encoded ABS event."""

    message: dict
    subscription: str | None = None
