from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path


NOTEBOOKLM_DIR = Path(__file__).resolve().parents[2]


def main() -> None:
    os.chdir(NOTEBOOKLM_DIR)
    notebooklm_path = str(NOTEBOOKLM_DIR)
    if notebooklm_path not in sys.path:
        sys.path.insert(0, notebooklm_path)

    from mcp_server.pathway_neo4j_server.server import main as server_main

    asyncio.run(server_main())


if __name__ == "__main__":
    main()
