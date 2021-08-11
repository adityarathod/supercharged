import json
from supercharged_pipelines.config.pipeline_config import PipelineConfig


def json_decoder(json_dict: dict) -> PipelineConfig:
    if "input_csv" in json_dict and "output_db" in json_dict:
        return PipelineConfig(json_dict["input_csv"], json_dict["output_db"])
    else:
        raise ValueError("Config file does not contain necessary values.")


def load_config(config_path: str) -> PipelineConfig:
    config: PipelineConfig = None
    with open(config_path, "r") as config_file:
        config = json.load(config_file, object_hook=json_decoder)
    return config
