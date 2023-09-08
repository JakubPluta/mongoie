import dataclasses


@dataclasses.dataclass
class Settings:
    DEFAULT_WRITER_FORMAT: str = "json"
