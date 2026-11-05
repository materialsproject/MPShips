import faulthandler
import importlib
import os

# Under gunicorn's gevent worker, threading is already monkey-patched when this
# module is imported. Importing ddtrace.auto *after* patching unloads and
# reloads modules so that threading locks become native (non-cooperative)
# again, which can deadlock greenlets. So only import it here if ddtrace isn't
# loaded yet AND gevent hasn't patched; in deployments `start.sh` runs
# `ddtrace-run gunicorn`, which loads ddtrace before gevent patches.
import sys  # noqa: E402

if "ddtrace" not in sys.modules:
    try:
        from gevent import monkey as _gevent_monkey

        _patched = _gevent_monkey.is_module_patched("threading")
    except ImportError:
        _patched = False
    if not _patched:
        import ddtrace.auto  # noqa: F401

faulthandler.enable()

from mp_ui.core.logging import (  # noqa: E402
    configure_logging,
    restore_warnings_capture,
)
from mp_ui.core.observability import install_dash_observability  # noqa: E402

# Structured (JSON) logging for the app, its libraries (incl. mp-api) and
# captured warnings. Format and levels: MP_LOG_FORMAT, MP_LOG_LEVEL,
# MP_APP_LOG_LEVEL, MP_DEBUG_MODE (see mp_ui.core.logging).
configure_logging()

PACKAGE_NAME = os.environ.get("PACKAGE_NAME")
MODULE_NAME = os.environ.get("MODULE_NAME")
APP_FACTORY = os.environ.get("APP_FACTORY")

module = importlib.import_module(f"{PACKAGE_NAME}.{MODULE_NAME}")
create_app = getattr(module, APP_FACTORY)

if MODULE_NAME == "app":
    # MPShips

    url = f"/{PACKAGE_NAME}/"
    path_to_pages = os.path.join(os.path.dirname(__file__), "pages")
    assets_folder = os.path.join(os.path.dirname(__file__), "assets")

    dash_app = create_app(
        url=url, pages_folder=path_to_pages, assets_folder=assets_folder
    )
else:
    # mp_web
    dash_app = create_app()

# Dash callback names in logs, APM resources and the access log (idempotent,
# the app factory may already have installed it)
install_dash_observability(dash_app)
application = dash_app.server

# Some dependencies silence or redirect warnings at import; undo that so
# MPRestWarning / FutureWarning reach the structured logs
restore_warnings_capture()
