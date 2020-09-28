class HelpException(Exception):
    pass


class ValidationError(Exception):
    """Error raised when StructuredCypherQueries are not correctly defined."""


class ClassDefinitionConflictError(Exception):
    """Error raised when query given name already defined."""


class InitializationNotAllowedError(Exception):
    """Cannot initialize abstract class."""


class ImproperClassDefinitionError(Exception):
    """Raised when abstract queries are improperly defined."""
