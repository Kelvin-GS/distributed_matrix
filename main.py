"""
Entry point for the Distributed Matrix Multiplication Node.

Run with:
    python main.py
    python main.py --port 8081
    python main.py --port 8081 --log-level DEBUG
"""

import asyncio
import argparse
import logging
import sys

# Configure logging before any module imports
def _setup_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Distributed Matrix Multiplication Node"
    )
    parser.add_argument(
        "--port", type=int, default=8080,
        help="HTTP port for this node (default: 8080)"
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0",
        help="Bind address (default: 0.0.0.0)"
    )
    args = parser.parse_args()

    # Validate port range
    if not (1024 <= args.port <= 65535):
        parser.error(f"Port must be between 1024 and 65535, got {args.port}")

    return args


async def main():
    args = _parse_args()
    _setup_logging(args.log_level)

    log = logging.getLogger("main")

    # Import after logging is configured
    from node import Node

    try:
        node = Node(port=args.port)
        log.info("Starting distributed matrix node on port %d...", args.port)
        await node.start()
    except OSError as e:
        log.error("Failed to start: %s", e)
        log.error("Is port %d already in use?", args.port)
        sys.exit(1)
    except Exception as e:
        log.exception("Fatal error during startup: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nNode stopped.")