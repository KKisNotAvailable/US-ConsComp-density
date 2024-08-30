import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re
from datetime import datetime
import csv
import warnings
warnings.filterwarnings("ignore")



PROPERTY_INDICATOR = {
    10: "Single Family Residence / Townhouse",
    11: "Condominium (residential)",
    20: "Commercial",
    21: "Duplex, Triplex, Quadplex)",
    22: "Apartment",
    23: "Hotel, Motel",
    24: "Commercial (condominium)",
    25: "Retail",
    26: "Service (general public)",
    27: "Office Building",
    28: "Warehouse",
    29: "Financial Institution",
    30: "Hospital (medical complex, clinic)",
    31: "Parking",
    32: "Amusement-Recreation",
    50: "Industrial",
    51: "Industrial Light",
    52: "Industrial Heavy",
    53: "Transport",
    54: "Utilities",
    70: "Agricultural",
    80: "Vacant",
    90: "Exempt",
    0: "Miscellaneous"
}

CHUNK_SIZE = 1000000 # this for Deed files is about 400MB

class Preprocess():
    def __init__(self, filepath: None, log_path: str = "") -> None:
        self._filepath = filepath
        self.__log_path = log_path

        if not os.path.exists(self.__log_path):
            with open(self.__log_path, 'w') as f:
                pass  # Just create an empty file

    def write_log(self, message):
        """Writes a log message with a timestamp to the log file."""
        with open(self.__log_path, 'a') as f:
            f.write(f"{message}\n")

    def _read_n_clean(self, filename: str = None, cols_to_keep: list = []) -> pd.DataFrame:
        '''
        Based on the filename and cols_to_keep provided, will return
        the desired dataframe.
        Notice that this method does not filter the data.
        '''
        if not filename:
            print("Please provide valid file.")
            return
        
        match = re.search(r'\d+', filename)
        serial_n = match.group()
        
        filename = self._filepath + filename

        def handle_bad_line(bad_line):
            to_log= f"File {serial_n} skipping line: {bad_line}"
            self.write_log(to_log)
            return None  # Returning None tells pandas to skip this line

        dfs = []

        # 如果我現在對於壞的欄位先忽略，那還有需要試dask嗎
        for chunk in pd.read_csv(
            filename, delimiter="|", 
            chunksize=CHUNK_SIZE, # can only work with engine = c or python
            # on_bad_lines=handle_bad_line, # callable can only work with engine = python or pyarrow
            on_bad_lines='skip', # skip the bad rows for now, guess there won't be many (cannot check tho)
            engine='c',
            quoting=csv.QUOTE_NONE
        ):
            if cols_to_keep:
                chunk = chunk[cols_to_keep]
            dfs.append(chunk)

        out_df = pd.concat(dfs, ignore_index=True)

        to_log= f"File {serial_n} done."
        self.write_log(to_log)

        return out_df

    def deed_files(self, files = None):
        '''
        The actions done here will be based on the data type of "files
        str: on single file
        list: on the list of files
        None: all of the files in the directory
        '''
        cols_to_keep = [
            "FIPS",
            "PCL_ID_IRIS_FRMTD", # unique key
            "BLOCK LEVEL LATITUDE",
            "BLOCK LEVEL LONGITUDE",
            "SITUS_CITY",
            "SITUS_STATE",
            "SITUS_ZIP_CODE",
            "SELLER NAME1",
            "SALE AMOUNT",
            "SALE DATE",
            "RECORDING DATE",
            "PROPERTY_INDICATOR", # residential, commercial, ...
            "RESALE/NEW_CONSTRUCTION" # M: re-sale, N: new construction
        ]

        def get_data(f: str):
            if not f.endswith(txt_extention):
                f = f + txt_extention
            
            tmp_df = self._read_n_clean(
                filename=f,
                cols_to_keep=cols_to_keep
            )

            tmp_df = tmp_df[tmp_df['RESALE/NEW_CONSTRUCTION'] == "N"].reset_index(drop=True)

            return tmp_df

        def _simple_analysis(df: pd.DataFrame):
            # TODO: Currently no use, can be deleted.
            # 這裡用沒有經過篩選過的資料跑才有意義
            cond_is_new = df['RESALE/NEW_CONSTRUCTION'] == "N"
            cond_owner_record = df['SELLER NAME1'] == 'OWNER RECORD'

            # check the dist. of 0 sale amount
            df['is_new'] = cond_is_new
            df['amt_is_nan'] = pd.isna(df['SALE AMOUNT'])
            tmp = df.groupby(['is_new', 'amt_is_nan']).size().reset_index(name='counts')
            print("#####")
            print(tmp)
            print("=====")

            df['is_owner'] = cond_owner_record
            tmp = df.groupby(['is_new', 'is_owner']).size().reset_index(name='counts')
            print("#####")
            print(tmp)
            print("=====")

        txt_extention = ".txt"
        dataframes = []

        # TODO: 預計把所有工作都包到一個nested method裡，所以目前這個get_data只是過渡用
        if not files:
            for f in tqdm(os.listdir(self._filepath), desc="Deed files"):
                if f.endswith(txt_extention):
                    dataframes.append(get_data(f))
        elif isinstance(files, list):
            for f in files:
                dataframes.append(get_data(f))
        else:
            dataframes.append(get_data(files))

        return pd.concat(dataframes, ignore_index=True)

    def deed_peep(self, df: pd.DataFrame = None):
        '''
        The actions done here will be based on the data type of self.__filename
        str: does on single file
        list: does on the list of files
        None: all of the files in the directory
        '''
        # if no df provided, do the demo thing: use one of the files
        # from path.
        if df is None:
            filepath = "../Corelogic/bulk_deed_fips_split/"
            filename = filepath+'fips-01001-UniversityofPA_Bulk_Deed.txt'

            df = pd.read_csv(filename, delimiter="|")

            cols_to_keep = [
                "FIPS",
                "PCL_ID_IRIS_FRMTD", # unique key
                "OWNER_1_LAST_NAME", # empty if is company
                "OWNER_1_FIRST_NAME&MI", # company name
                "BLOCK LEVEL LATITUDE",
                "BLOCK LEVEL LONGITUDE",
                "SITUS_CITY",
                "SITUS_STATE",
                "SITUS_ZIP_CODE",
                "SELLER NAME1",
                "SALE AMOUNT", # but after skimming throught, there are several NULL
                "SALE DATE",
                "RECORDING DATE",
                "PROPERTY_INDICATOR", # residential, commercial, ...
                "OWNER_RELATIONSHIP_RIGHTS_CODE", # not sure will keep this column
                "RESALE/NEW_CONSTRUCTION" # M: re-sale, N: new construction
            ]
            df = df[cols_to_keep]

        # this condition would be meaningless if use processed data
        # since in our data reading step we filtered with this condition
        cond_is_new = df['RESALE/NEW_CONSTRUCTION'] == "N"
        cond_owner_record = df['SELLER NAME1'] == 'OWNER RECORD'

        # check the dist. of 0 sale amount
        df['is_new'] = cond_is_new
        df['amt_is_nan'] = pd.isna(df['SALE AMOUNT'])
        tmp = df.groupby(['is_new', 'amt_is_nan']).size().reset_index(name='counts')
        print("#####")
        print(tmp)
        print("=====")

        df['is_owner'] = cond_owner_record
        tmp = df.groupby(['is_new', 'is_owner']).size().reset_index(name='counts')
        print("#####")
        print(tmp)
        print("=====")

        # this result is too lengthy, so commented.
        # tmp = df.groupby(['is_new', 'PROPERTY_INDICATOR']).size().reset_index(name='counts')
        # print(tmp)

        # df = df[cond1]

        # check what owner rights code might actually means
        # tmp = df.groupby(['OWNER_RELATIONSHIP_RIGHTS_CODE']).size().reset_index(name='counts')
        # print(tmp)

        col_to_see = ['SELLER NAME1', 'OWNER_1_FIRST_NAME&MI']

        # print("check owner vs seller name")
        # print(df[df['SELLER NAME1'] != 'OWNER RECORD'][col_to_see])
        # print(df[df['SELLER NAME1'] == 'OWNER RECORD'][col_to_see]])
        return
    
    def data_output(self, df: pd.DataFrame, filename: str) -> None:
        if not filename.endswith(".csv"):
            filename = filename + ".csv"
        df.to_csv(f'{filename}', index=False)
        print(f"{filename} generated.")
        return

    def __check_company_list(self):
        '''
        Don't run this, the file is 10 GB, will freeze your PC.

        This method simply serves as a exploratory analysis of 
        the company list provided by previous research.
        '''
        filepath = "../Merge_Compustat&Corelogic/"
        filename = filepath+'corelogic_clean.dta'
        
        # comp_list = pd.read_stata(filename)

        # print(comp_list.columns)


def main():
    log_path = "./log/"
    filepath = "../Corelogic/bulk_deed_fips_split/"

    if not os.path.exists(log_path):
        os.makedirs(log_path)

    current_date = datetime.now().strftime("%m%d")
    log = f'deed_{current_date}.log'

    p = Preprocess(filepath=filepath, log_path=log_path+log)
    file_list = [
        "fips-01001-UniversityofPA_Bulk_Deed", 
        "fips-01003-UniversityofPA_Bulk_Deed", 
        "fips-01005-UniversityofPA_Bulk_Deed"
    ]
    # file_list = "fips-01001-UniversityofPA_Bulk_Deed"
    # 想一下這個產好的資料要吐出來還是當作attribute
    # 目前覺得吐出來好 (把這個preprocess當工具箱的話應該會是吐出來比較好)
    data = p.deed_files()
    # print(data.shape)
    p.data_output(data, 'deed_stacked.csv')

    # p.deed_peep(data)


if __name__ == "__main__":
    main()