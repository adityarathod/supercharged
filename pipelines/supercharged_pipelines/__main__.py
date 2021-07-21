import sys
from typing import List
from .jobs.facilities import etl as facilities_etl
from .jobs.drgs import etl as drgs_etl
from .jobs.treatments import etl as treatments_etl


def main(args: List[str]):
    if len(args) <= 1:
        print("Cannot run pipeline")
        sys.exit(1)
    facilities_etl(args[1])
    drgs_etl(args[1])
    treatments_etl(args[1])


if __name__ == "__main__":
    main(sys.argv)
