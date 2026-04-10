"""
Customer Database CLI — launch and manage customer database
"""

import argparse
import sys
import os
from pathlib import Path

# Ensure we can import from parent directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.universal_server.cli import _build_security_config
from backend.universal_server.server import UniversalStateServer


def main():
    parser = argparse.ArgumentParser(
        description="AhanaFlow Customer Database (backed by UniversalStateServer)"
    )
    parser.add_argument(
        "--wal",
        default=os.environ.get("CUSTOMER_DB_WAL", "./customer_db.wal"),
        help="Customer database WAL path",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("CUSTOMER_DB_HOST", "0.0.0.0"),
        help="Bind host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("CUSTOMER_DB_PORT", "9635")),
        help="Bind port (default: 9635)",
    )
    parser.add_argument(
        "--durability-mode",
        choices=["safe", "fast", "strict"],
        default=os.environ.get("CUSTOMER_DB_DURABILITY", "safe"),
        help="Durability mode",
    )
    parser.add_argument(
        "--api-keys-file",
        default=os.environ.get("CUSTOMER_DB_API_KEYS_FILE", ""),
        help="API keys file for auth",
    )
    
    args = parser.parse_args()
    
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  AhanaFlow Customer Database                                 ║")
    print("║  Compressed Customer Data · Support · Marketing              ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()
    print(f"  WAL Path:        {args.wal}")
    print(f"  Listen:          {args.host}:{args.port}")
    print(f"  Durability:      {args.durability_mode}")
    print(f"  Auth:            {'enabled' if args.api_keys_file else 'disabled (local only)'}")
    print()
    
    server = UniversalStateServer(
        Path(args.wal),
        host=args.host,
        port=args.port,
        durability_mode=args.durability_mode,
        security_config=_build_security_config(args.api_keys_file),
    )
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\nShutting down customer database...")
        server.shutdown()
        print("✓ Customer database stopped cleanly")


if __name__ == "__main__":
    main()
