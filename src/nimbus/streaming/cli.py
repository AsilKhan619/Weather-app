"""Command-line flag shared by every long-running consumer."""

import argparse


def parse_drain_flag(description: str) -> bool:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--drain",
        action="store_true",
        help="consume until caught up to the end of the topic, then exit "
        "(used by `make demo` and replays; omit to run as a long-lived service)",
    )
    return bool(parser.parse_args().drain)
