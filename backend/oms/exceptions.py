"""
Exceptions for OMS (Ontology Management Service)
"""


class OmsBaseException(Exception):
    """Base exception for OMS"""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class OntologyNotFoundError(OmsBaseException):
    """Raised when ontology is not found"""

    pass


class DuplicateOntologyError(OmsBaseException):
    """Raised when trying to create duplicate ontology"""

    pass


class OntologyValidationError(OmsBaseException):
    """Raised when ontology validation fails"""

    pass


class ConnectionError(OmsBaseException):
    """Raised when connection to database fails"""

    pass


class DatabaseNotFoundError(OmsBaseException):
    """Raised when database is not found"""

    pass


class DatabaseError(OmsBaseException):
    """General database error"""

    pass


# Atomic update exceptions
class AtomicUpdateError(OmsBaseException):
    """Base exception for atomic update operations"""

    pass


class PatchUpdateError(AtomicUpdateError):
    """Raised when patch-based atomic update fails"""

    pass


class TransactionUpdateError(AtomicUpdateError):
    """Raised when transaction-based atomic update fails"""

    pass


class WOQLUpdateError(AtomicUpdateError):
    """Raised when WOQL-based atomic update fails"""

    pass


class BackupCreationError(AtomicUpdateError):
    """Raised when backup creation fails"""

    pass


class BackupRestoreError(AtomicUpdateError):
    """Raised when backup restore fails"""

    pass


class CriticalDataLossRisk(AtomicUpdateError):
    """Raised when there's a critical risk of data loss"""

    pass


# Relationship management exceptions
class RelationshipError(OmsBaseException):
    """Base exception for relationship operations"""

    pass


class CircularReferenceError(RelationshipError):
    """Raised when circular reference is detected"""

    pass


class InvalidRelationshipError(RelationshipError):
    """Raised when relationship is invalid"""

    pass


class RelationshipValidationError(RelationshipError):
    """Raised when relationship validation fails"""

    pass
