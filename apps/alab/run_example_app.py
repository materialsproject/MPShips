"""
CRITICAL: DO NOT EDIT. For MPShips users: Manual changes may cause system instability.
"""

import os
from src.alab.example_app import example_app

server = example_app.server

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 8051

    example_app.run(debug=True, host=host, port=port)