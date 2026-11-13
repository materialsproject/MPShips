"""
CRITICAL: DO NOT EDIT. For MPShips users: Manual changes may cause system instability.
"""

import os
from src.alab.app import create_app

app = create_app()
server = app.server

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 8050

    app.run(debug=True, host=host, port=port)