#!/bin/bash

# initialize venv
poetry shell

# store spark root
SUBMIT_BIN="$SPARK_HOME/bin/spark-submit"

# run the thing
$SUBMIT_BIN --driver-class-path ./tmp/sqlite-jdbc-3.36.0.1.jar --jars tmp/sqlite-jdbc-3.36.0.1.jar --py-files dist/*.whl run.py config/dev.json