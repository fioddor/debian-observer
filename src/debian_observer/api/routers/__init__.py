"""API routers for Debian Observatory.

All routers depend on ``request.app.state.store`` for data access.
"""

from . import datasets, events, health, metrics, migrations, packages, releases
