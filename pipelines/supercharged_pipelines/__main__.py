import sys
from typing import List
from .jobs.providers import etl as providers_etl


def main(args: List[str]):
    if len(args) <= 1:
        print("Cannot run pipeline")
        sys.exit(1)
    providers_etl(args[1])


if __name__ == "__main__":
    main(sys.argv)
