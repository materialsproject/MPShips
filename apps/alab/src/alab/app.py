"""
CRITICAL: DO NOT EDIT. For MPShips users: Manual changes may cause system instability.
"""

from mpships_infra import create_app as _create_app
from . import pages
import os

endpoint = "/alab/"
path_to_pages = os.path.join(os.path.dirname(__file__), "pages")
assets_folder = os.path.join(os.path.dirname(__file__), "assets")


def create_app():
    # Same pattern as mp_web.main: importing this module only defines
    # `create_app`; the Dash app is built once by the caller (wsgi.py in the
    # container, run_app.py locally), not as an import side effect.
    return _create_app(
        endpoint=endpoint, pages_folder=path_to_pages, assets_folder=assets_folder
    )


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=8081)