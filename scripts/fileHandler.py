import pandas as pd
import json


class FileHandler:
    

    def to_csv(self, df, csvPath, index=False):
        try:
            df.to_csv(csvPath, index=index)
            print('file saved')

        except Exception:
            print('file saving failed')

    def readCsv(self, csvPath, missing_values=["undefined","na"]):
        try:
            df = pd.readCsv(csvPath, na_values=missing_values)
            print('csv file read from {csvPath}.')
            return df

        except FileNotFoundError:
            print('file not found.')

    def readJson(self, json_path):
        try:
            with open(json_path) as js:
                json_obj = json.load(js)
            print('json file read from {json_path}.')
            return json_obj

        except FileNotFoundError:
            self.logger.exception('File not found.')
