"""
CRITICAL: DO NOT EDIT. For MPShips users: Manual changes may cause system instability.
"""

from mpships_infra import create_app
from . import example_pages
import os

endpoint = "/dielectric_function/"
path_to_pages = os.path.join(os.path.dirname(__file__), "example_pages")
assets_folder = os.path.join(os.path.dirname(__file__), "assets")

example_app = create_app(endpoint=endpoint, pages_folder=path_to_pages, assets_folder=assets_folder)