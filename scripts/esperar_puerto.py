"""Espera a que un servicio TCP (host:puerto) acepte conexiones; útil como gate en Compose."""

from __future__ import annotations

import socket
import sys
import time


def main() -> None:
    destino, timeout = sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 180.0
    host, puerto = destino.rsplit(":", 1)
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        try:
            with socket.create_connection((host, int(puerto)), timeout=3):
                print(f"{destino} disponible")
                return
        except OSError:
            time.sleep(2)
    print(f"{destino} no respondió en {timeout:.0f} s", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
