"""
Entry point. Run with:
    python main.py
    python main.py --port 8081
"""

import asyncio
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from node import Node


async def main():
    parser = argparse.ArgumentParser(
        description="Distributed Matrix Multiplication Node"
    )
    parser.add_argument(
        "--port", type=int, default=8080,
        help="HTTP port for this node (default: 8080)"
    )
    args = parser.parse_args()

    node = Node(port=args.port)
    await node.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nNode stopped.")