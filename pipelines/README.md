<center><h1>the ⚡ supercharged etl pipelines</h1></center>

These pipelines automate the extraction, transformation (cleaning), and loading (aka ETL) of Medicare inpatient charge data into a SQLite database to be used by the accompanying web application.

## Available pipelines
We have the following jobs set up to collect and store various bits of data.

| Pipeline                                         | Description                                       |
| ------------------------------------------------ | ------------------------------------------------- |
| `supercharged_pipelines.jobs.FacilitiesPipeline` | Extracts and loads medical facilities             |
| `supercharged_pipelines.jobs.DRGPipeline`        | Extracts and loads diagnosis-related codes (DRGs) |
| `supercharged_pipelines.jobs.TreatmentsPipeline` | Extracts and loads all treatments at facilities   |


## Getting started with the pipelines
### Prerequisites
- Python 3.5+ installed
- Poetry installed
- Latest version of Spark installed and the root directory pointing to the environment variable `$SPARK_HOME` (such that `spark-submit` can be found at `$SPARK_HOME/bin/spark-submit`)
- Have run `chmod +x run.sh` to ensure the run script is executable.

### Building and running the pipelines
1. `poetry install`
2. `poetry build` to build a wheel of the `supercharged_pipelines` wheel into `dist/`.
3. Edit `dev.json` to the path of the Inpatient Charge Data CSV, [downloadable here](https://www.cms.gov/research-statistics-data-systems/medicare-provider-utilization-and-payment-data/medicare-provider-utilization-and-payment-data-inpatient/inpatient-charge-data-fy-2018) and the desired path of the database.
4. `./run.sh`


## Coming soon?
- `run.sh` support for passing in the config file
- Docker support for easier execution