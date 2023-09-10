import dataclasses


@dataclasses.dataclass
class Settings:
    DEFAULT_EXPORT_FORMAT: str = "json"
    DEFAULT_IMPORT_FORMAT: str = "json"
    NORMALIZE_EXPORTED_DATA: bool = True
    DENORMALIZE_IMPORTED_DATA: bool = True
    CLEAR_COLLECTION_BEFORE_IMPORT: bool = True
    DENORMALIZATION_RECORD_PREFIX: str = "."
