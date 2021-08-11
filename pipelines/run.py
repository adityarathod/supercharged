import sys
import sqlite3
from typing import List
from supercharged_pipelines import Pipeline
from supercharged_pipelines import config
from supercharged_pipelines.jobs import *


def main(args: List[str]):
    if len(args) <= 1:
        print("Cannot run pipeline")
        sys.exit(1)
    pipeline_config = config.load(args[1])
    ensure_schema(pipeline_config.output_db_path)
    pipelines: List[Pipeline] = [
        FacilitiesPipeline(),
        DRGPipeline(),
        TreatmentsPipeline(),
    ]
    for pipeline in pipelines:
        pipeline.run(pipeline_config)


def ensure_schema(path: str):
    conn = sqlite3.connect(path)
    with open("schema.sql") as schema:
        conn.executescript(schema.read())


if __name__ == "__main__":
    main(sys.argv)
