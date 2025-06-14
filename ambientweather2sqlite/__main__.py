import sys
import tomllib

from configuration import create_config_file, get_config_path
from daemon import start_daemon
from database import create_database_if_not_exists


def main() -> None:
    if len(sys.argv) > 1:
        print("Read settings from path:", sys.argv[1])
    else:
        config_path = get_config_path()
        if config_path is None:
            config_path = create_config_file()
        config = tomllib.loads(config_path.read_text())
        create_database_if_not_exists(config["database_path"])
        start_daemon(config["live_data_url"], config["database_path"])


if __name__ == "__main__":
    main()
