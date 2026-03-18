import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1]
ROOT = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from app_milano.app_milano import AppMilano
from app_milano.config import load_env_file


def main() -> None:
    load_env_file()
    if "--cli" in sys.argv:
        AppMilano().run()
        return

    AppMilano().bootstrap()
    from app_milano.utils.display import launch_desktop

    launch_desktop()


if __name__ == "__main__":
    main()
