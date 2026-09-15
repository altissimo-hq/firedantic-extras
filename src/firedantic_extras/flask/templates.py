"""Template registration for the bundled Jinja pagination macros."""

from __future__ import annotations

from typing import TYPE_CHECKING

from jinja2 import ChoiceLoader, PackageLoader

if TYPE_CHECKING:
    from flask import Flask

__all__ = ["register_macros"]


def register_macros(app: Flask) -> None:
    """Make the bundled macros importable as ``firedantic_extras/macros.html``.

    Call once during app setup::

        from firedantic_extras.flask.templates import register_macros
        register_macros(app)

    Then in any template::

        {% import "firedantic_extras/macros.html" as fe_macros %}
    """
    existing_loaders = [app.jinja_loader] if app.jinja_loader is not None else []
    app.jinja_loader = ChoiceLoader([*existing_loaders, PackageLoader("firedantic_extras", "templates")])
