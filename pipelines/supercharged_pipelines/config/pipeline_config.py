class PipelineConfig:
    input_csv_path: str
    output_db_path: str

    def __init__(self, input_csv_path: str, output_db_path: str):
        self.input_csv_path, self.output_db_path = input_csv_path, output_db_path
