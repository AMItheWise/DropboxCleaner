from __future__ import annotations

import argparse
import socket
from pathlib import Path

import uvicorn

from app.web.server import create_app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start the local Dropbox Cleaner browser UI.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind. Defaults to localhost only.")
    parser.add_argument("--port", type=int, default=0, help="Port to bind. Defaults to a random available port.")
    parser.add_argument("--no-browser", action="store_true", help="Do not open the default browser automatically.")
    parser.add_argument("--static-dir", type=Path, help="Override the built frontend static directory.")
    args = parser.parse_args(argv)

    port = args.port or _find_available_port(args.host)
    url = f"http://{args.host}:{port}"
    app = create_app(static_dir=args.static_dir, browser_url=None if args.no_browser else url)
    print(f"Dropbox Cleaner web UI: {url}")
    uvicorn.run(app, host=args.host, port=port, log_level="info")
    return 0


def _find_available_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


if __name__ == "__main__":
    raise SystemExit(main())
