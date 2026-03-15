class KeysError(Exception):
    """Base class for all keys library errors."""

class BusinessKeyError(KeysError):
    """Raised when a business key column is missing, empty, or has duplicates."""

class DatabaseError(KeysError):
    """Raised when a database query fails."""

class MissingDimensionKeyError(KeysError):
    """Raised when fact rows reference BKs with no matching dimension key."""

class MergeError(KeysError):
    """Raised when a merge changes the row count unexpectedly."""
