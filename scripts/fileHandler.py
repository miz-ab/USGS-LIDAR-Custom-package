import pandas as pd
import json


class FileHandler:
    

    def to_csv(self, df, csvPath, index=False):
        try:
            df.to_csv(csvPath, index=index)
            self.logger.info(f'Csv file saved in {csvPath}.')

        except Exception:
            self.logger.exception('File saving failed.')

    def read_csv(self, csvPath, missing_values=["n/a", "na", "undefined"]):
        try:
            df = pd.read_csv(csvPath, na_values=missing_values)
            self.logger.info(f'Csv file read from {csvPath}.')
            return df

        except FileNotFoundError:
            self.logger.exception('File not found.')

    def read_json(self, json_path):
        try:
            with open(json_path) as js:
                json_obj = json.load(js)
            self.logger.info(f'Json file read from {json_path}.')
            return json_obj

        except FileNotFoundError:
            self.logger.exception('File not found.')
