import dataclasses
import os


@dataclasses.dataclass
class Settings:
    DEFAULT_EXPORT_FORMAT: str = os.getenv("DEFAULT_EXPORT_FORMAT", "json")
    DEFAULT_IMPORT_FORMAT: str = os.getenv("DEFAULT_IMPORT_FORMAT", "json")
    NORMALIZE_EXPORTED_DATA: bool = os.getenv("NORMALIZE_EXPORTED_DATA", True)
    DENORMALIZE_IMPORTED_DATA: bool = os.getenv("DENORMALIZE_IMPORTED_DATA", True)
    CLEAR_COLLECTION_BEFORE_IMPORT: bool = os.getenv(
        "CLEAR_COLLECTION_BEFORE_IMPORT", True
    )
    DENORMALIZATION_RECORD_PREFIX: str = os.getenv("DENORMALIZATION_RECORD_PREFIX", ".")
    CHUNK_SIZE: int = os.getenv("CHUNK_SIZE", 5000)
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", False)
