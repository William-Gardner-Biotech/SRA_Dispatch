import unittest
import json
import os
from src.query_SRA_for_size import generate_SRR_size_df

# path to test config file
test_config_path = os.path.join(os.getcwd(), "test_config.json")

with open(test_config_path) as f:
    config = json.loads(f.read())

class test_query(unittest.TestCase):
    def test_config_loads(self):
        self.assertIn('wastewater', config['keyword1'])

    def test_query_empty(self):
        # Subtract 4 years from each date to allow empty SRA query error as no sars-cov-2 in 2018
        for key in config['dates']:
            date_parts = config['dates'][key].split('-')
            day, month, year = date_parts[0], date_parts[1], str(int(date_parts[2]) - 4)
            config['dates'][key] = f"{day}-{month}-{year}"
            print(config['dates'][key])

        # New date vals added to config obj
        generate_SRR_size_df(config)

        #self.assertRaises()


test_query_instance = test_query()
test_query_instance.test_query_empty()
