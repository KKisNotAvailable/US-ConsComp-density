import dask.dataframe as dd
import numpy as np
import os
from tqdm import tqdm
import re
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

BULK_PATH = "F:/CoreLogic/"

class Preprocess():
    def __init__(
            self, log_name: str = "", log_path: str = "./log/",
            bulkfile: str = "duke_university_ownertransfer_v3_dpc_01465911_20230803_072211_data.txt"
        ) -> None:
        self.bulkfile = f"{BULK_PATH}{bulkfile}"
        self.__log_file = log_path + log_name

        if not os.path.exists(log_path):
            os.makedirs(log_path)

        if log_name:
            if not os.path.exists(self.__log_file):
                with open(self.__log_file, 'w') as f:
                    pass  # Just create an empty file

    def write_log(self, message):
        """Writes a log message with a timestamp to the log file."""
        with open(self.__log_file, 'a') as f:
            f.write(f"{message}\n")

    def just_checking(self):
        ddf = dd.read_csv(self.bulkfile, delimiter="|", dtype='object')

        keep_col = [
            'CLIP', 'FIPS CODE', ''
        ]

        # =======================================
        #  Save a small dataset for clearer view
        # =======================================
        # tmp = ddf.head(30)
        # tmp.to_csv("./log/deed_2023_first30.txt", index=False)

        # ================
        #  Basic checking
        # ================
        # 1. distribution of fips (including missing)
        #    => but some of the entries with missing fips actually has county name or street + city name...
        # 2. 

        # print(ddf.columns)

        # ttl_rows = ddf['FIPS'].shape[0].compute() # but why len(ddf['FIPS']) won't work?

def main():
    current_date = datetime.now().strftime("%m%d")
    log = f'deed_{current_date}.log'

    p = Preprocess()

    p.just_checking()
    
    # ===========================
    # Generate Stacked Deed Files
    # ===========================
    # deed_workflow(p)

    print("Done!!")
    

if __name__ == "__main__":
    main()