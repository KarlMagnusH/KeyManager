"""
KeyManager - A Python package for managing database keys and dimensions.

This package provides classes for handling primary keys, business keys,
and dimension/fact table relationships in data warehousing scenarios.
"""

from .Keys import KeyManager, KeyDimension, KeyFact, set_business_key

# Package metadata
__version__ = "0.1.0"

# Define what gets imported with "from keymanager import *"
__all__ = [
    "KeyManager",
    "KeyDimension", 
    "KeyFact",
    "set_business_key"
]

# Optional: Add package-level convenience functions or constants
BK_SEP = "||"
DEFAULT_PK_VALUE = -1
DEFAULT_PK_PREFIX = "key"
DEFAULT_BK_PREFIX = "bk"