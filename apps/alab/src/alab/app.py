"""
CRITICAL: DO NOT EDIT. For MPShips users: Manual changes may cause system instability.
"""

from mpships_infra import create_app
from . import pages
import os

endpoint = "/alab/"
path_to_pages = os.path.join(os.path.dirname(__file__), "pages")
assets_folder = os.path.join(os.path.dirname(__file__), "assets")

app = create_app(endpoint=endpoint, pages_folder=path_to_pages, assets_folder=assets_folder)