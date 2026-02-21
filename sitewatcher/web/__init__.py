"""sitewatcher.web package.

Avoid importing the FastAPI app at module import time so utility imports
(e.g. sitewatcher.web.auth) do not trigger route registration side effects.
"""

__all__ = ["app"]


def __getattr__(name: str):
    if name == "app":
        from sitewatcher.web.app import app

        return app
    raise AttributeError(f"module 'sitewatcher.web' has no attribute {name!r}")
