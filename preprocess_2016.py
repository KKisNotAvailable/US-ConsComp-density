import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re
from datetime import datetime
import csv # for csv.QUOTE_NONE, which ignores ' when reading csv
import warnings
warnings.filterwarnings("ignore")

'''HIGH LEVEL IDEA
Frist extract columns and stack all the data from the same path
into one large csv. Then we use dask to do operations on these 
large data.
'''

# NOTE:
# 1. APN (Assessor's Parcel Number) refers to specific parcel of "land", not houses

# TODO: merge the code for operations on TAX and DEED.

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
    def __init__(self, log_name: str, log_path: str = "./log/") -> None:
        self._filepath = ""
        self.__log_file = log_path + log_name

        if not os.path.exists(log_path):
            os.makedirs(log_path)

        if not os.path.exists(self.__log_file):
            with open(self.__log_file, 'w') as f:
                pass  # Just create an empty file

    def write_log(self, message):
        """Writes a log message with a timestamp to the log file."""
        with open(self.__log_file, 'a') as f:
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

        # 對於壞的row先忽略
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
    
    def stack_files(
            self, files = None, 
            deed_or_tax: str = ""
        ) -> pd.DataFrame:
        '''
        The actions done here will be based on the data type of "files"
        str: on single file
        list: on the list of files
        None: all of the files in the directory
        '''
        self._filepath = f"../Corelogic/bulk_{deed_or_tax.lower()}_fips_split/"
        
        cols_tax = [
            "FIPS_CODE", # county
            "PCL_ID_IRIS_FORMATTED", # unique key = APN
            "PROPERTY_INDICATOR_CODE", # 10-19: residential, ..., 80-89: vacant
            "ASSESSED_TOTAL_VALUE",
            "MARKET_TOTAL_VALUE",
            "APPRAISED_TOTAL_VALUE",
            "TAX_AMOUNT",
            "TAX_YEAR",
            "SELLER_NAME", # just to make sure
            "SALE_AMOUNT", # just to make sure
            "SALE_DATE", # just to make sure
            "LAND_SQUARE_FOOTAGE",
            "UNIVERSAL_BUILDING_SQUARE_FEET",
            "BUILDING_SQUARE_FEET_INDICATOR_CODE", # A: adjusted; B: building; G: gross; H: heated; L: living; M: main; R: ground floor
            "BUILDING_SQUARE_FEET", # doesn't differentiate living and non-living (if lack indicator code)
            "LIVING_SQUARE_FEET"
        ]
        cols_deed = [
            "FIPS", # county
            "PCL_ID_IRIS_FRMTD", # unique key
            "SITUS_CITY",
            "SITUS_STATE",
            "SITUS_ZIP_CODE",
            "SELLER NAME1", # name of the first seller
            "SELLER NAME2",
            "SALE AMOUNT",
            "MORTGAGE_AMOUNT",
            "MORTGAGE_INTEREST_RATE",
            "MORTGAGE_ASSUMPTION_AMOUNT", # assumption amount of existing mortgage
            "CASH/MORTGAGE_PURCHASE", # C,Q = cash; M,R = mortgage
            "SALE DATE",
            "RECORDING DATE",
            "LAND_USE", # plz refer to codebook, there are 263 different
            "SELLER_CARRY_BACK", # A,Y = Yes
            "CONSTRUCTION_LOAN",
            "PROPERTY_INDICATOR", # residential, commercial, refer to top of this code file
            "RESALE/NEW_CONSTRUCTION" # M: re-sale, N: new construction
        ]

        cols_to_keep = cols_tax if deed_or_tax.lower() == 'tax' else cols_deed

        txt_extention = ".txt"
        
        def get_data(f: str):
            if not f.endswith(txt_extention):
                f = f + txt_extention
            
            tmp_df = self._read_n_clean(
                filename=f,
                cols_to_keep=cols_to_keep
            )

            # can specify condition here, but notice column names of tax and deed are different

            return tmp_df
        
        dataframes = []

        if not files:
            for f in tqdm(os.listdir(self._filepath), desc=f"{deed_or_tax} files"):
                if f.endswith(txt_extention):
                    dataframes.append(get_data(f))
        elif isinstance(files, list):
            for f in files:
                dataframes.append(get_data(f))
        else:
            dataframes.append(get_data(files))

        self.write_log(f"Processed {deed_or_tax} files:")

        return pd.concat(dataframes, ignore_index=True)

    def deed_files(
            self, files = None, 
            filepath: str = "../Corelogic/bulk_deed_fips_split/"
        ) -> pd.DataFrame:
        '''
        The actions done here will be based on the data type of "files"
        str: on single file
        list: on the list of files
        None: all of the files in the directory
        '''
        self._filepath = filepath
        self.write_log("Processed Deed files:")
        cols_to_keep = [
            "FIPS", # county
            "PCL_ID_IRIS_FRMTD", # unique key
            "SITUS_CITY",
            "SITUS_STATE",
            "SITUS_ZIP_CODE",
            "SELLER NAME1", # name of the first seller
            "SELLER NAME2",
            "SALE AMOUNT",
            "MORTGAGE_AMOUNT",
            "MORTGAGE_INTEREST_RATE",
            "MORTGAGE_ASSUMPTION_AMOUNT", # assumption amount of existing mortgage
            "CASH/MORTGAGE_PURCHASE", # C,Q = cash; M,R = mortgage
            "SALE DATE",
            "RECORDING DATE",
            "LAND_USE", # plz refer to codebook, there are 263 different
            "SELLER_CARRY_BACK", # A,Y = Yes
            "CONSTRUCTION_LOAN",
            "PROPERTY_INDICATOR", # residential, commercial, ...
            "RESALE/NEW_CONSTRUCTION" # M: re-sale, N: new construction
        ]

        txt_extention = ".txt"
        
        def get_data(f: str):
            if not f.endswith(txt_extention):
                f = f + txt_extention
            
            tmp_df = self._read_n_clean(
                filename=f,
                cols_to_keep=cols_to_keep
            )

            # tmp_df = tmp_df[tmp_df['RESALE/NEW_CONSTRUCTION'] == "N"].reset_index(drop=True)

            return tmp_df
        
        dataframes = []

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
        This method served as a simple glimpse of distributions of some
        variables.
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
    
    def data_output(self, df: pd.DataFrame, filename: str, out_path: str = "./data/") -> None:
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        
        if not filename.endswith(".csv"):
            filename = filename + ".csv"

        if out_path not in filename:
            filename = out_path + filename

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

    def check_apn_match(self):
        dfs = {}
        for s in ['Deed', 'Tax']:
            print(f">>> {s}:")
            path = f"../Corelogic/bulk_{s.lower()}_fips_split/"
            tmp = pd.read_csv(f'{path}fips-01011-UniversityofPA_Bulk_{s}.txt', delimiter="|")
            
            print(tmp.shape)
            target_col = 'APN_NUMBER_FORMATTED' if s == 'Tax' else 'PCL_ID_IRIS_FRMTD'
            dfs[s] = tmp[target_col]

        intersection_cnts = dfs['Tax'].isin(dfs['Deed']).sum()
        print(intersection_cnts)


def deed_workflow(p: Preprocess):
    file_list = [
        "fips-01001-UniversityofPA_Bulk_Deed", 
        "fips-01003-UniversityofPA_Bulk_Deed", 
        "fips-01005-UniversityofPA_Bulk_Deed"
    ]
    # file_list = "fips-01001-UniversityofPA_Bulk_Deed"
    filepath = "../Corelogic/bulk_deed_fips_split/"
    # if provide no "files", the following method would run through
    # the filepath
    data = p.deed_files(files=file_list, filepath=filepath) # 目前覺得吐出來好 (把這個preprocess當工具箱的話應該會是吐出來比較好)
    print(data.shape)
    # p.deed_peep(data)
    # p.data_output(data, 'deed_stacked.csv')
    
def tax_workflow(p: Preprocess):
    file_list = [
        "fips-01001-UniversityofPA_Bulk_Tax", 
        "fips-01003-UniversityofPA_Bulk_Tax", 
        "fips-01005-UniversityofPA_Bulk_Tax"
    ]
    filepath = "../Corelogic/bulk_tax_fips_split/"
    data = p.tax_files(files=file_list, filepath=filepath)
    print(data.shape)

# I'm thinking put the work flow into preprocess class

def main():
    current_date = datetime.now().strftime("%m%d")
    log = f'deed_{current_date}.log'

    p = Preprocess(log_name=log)

    # p.check_apn_match()

    # ===========================
    # Generate Stacked Deed Files
    # ===========================
    # deed_workflow(p)

    # ==========================
    # Generate Stacked Tax Files
    # ==========================
    # tax_workflow(p)
    

if __name__ == "__main__":
    main()