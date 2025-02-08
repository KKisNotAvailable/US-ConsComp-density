'''
This program is to do file-wise cleaning for the Corelogic files 2016 version.
'''

import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re
import platform
import time
from datetime import datetime
import csv # for csv.QUOTE_NONE, which ignores ' when reading csv
import warnings
warnings.filterwarnings("ignore")

CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
EXT_DISK += "Homebuilder/2016_files/"
BULK_PATH = EXT_DISK + "Corelogic/"
OUT_PATH = EXT_DISK + "cleaned_by_county/"

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
    def __init__(self, log_name: str = "", log_path: str = "./log/") -> None:
        self.__ref_filepath = f'{BULK_PATH}bulk_deed_fips_split/'
        self.__log_file = log_path + log_name

        if log_name:
            if not os.path.exists(log_path):
                os.makedirs(log_path)

            if not os.path.exists(self.__log_file):
                with open(self.__log_file, 'w') as f:
                    pass  # Just create an empty file

    def write_log(self, message):
        """Writes a log message with a timestamp to the log file."""
        with open(self.__log_file, 'a') as f:
            f.write(f"{message}\n")

    def _filter(self, data, deed_or_tax: str = ""):
        '''
        This largely follows the 2023 data filter
        '''
        if deed_or_tax.lower() == 'deed':
            data['BUYER_NAME_1'] = data['OWNER_1_LAST_NAME'] + " " + data['OWNER_1_FIRST_NAME&MI']

            # keep the numeric and empty records
            data = data[(data['SALE AMOUNT'].str.isnumeric()) | (data['SALE AMOUNT'] == "")]

            # keep nonempty and valid sale date and then keep only after year 2000
            data = data[data['SALE DATE'].str.isnumeric()]
            data = data[data['SALE DATE'].str[:4].astype(int) >= 2000]

            # Remove Non-Arms Length Transactions
            data = data[data['INTER_FAMILY'] != 'Y']
            # A: 'Arms Length Transaction', B-C: 'Non Arms Length' (pri-cat-code in old codebook)
            data = data[data['PRI-CAT-CODE'] == 'A']
            # Remove if first 10 characters are identical for seller and buyer name
            # what we want is seller != buyer, but not sure about OWNER RECORD
            data = data[(data['SELLER NAME1'].str[0:10] != data['BUYER_NAME_1'].str[0:10])]

            # Remove foreclosure and government-to-private party transactions (REO-nominal, REO Sale: Gov to private party)
            data = data[~data['FORECLOSURE'].isin(['Y', 'P'])]
            # Remove (foreclosure & in lieu of foreclosure deed) and (nominal deed)
            data = data[~data['DOCUMENT TYPE'].isin(['U', 'DL', 'Z', '^'])]
            # Remove if seller is related to Federal Homes Loan Mortgage (Fannie Mae) -- foreclosure homes --
            data = data[~data['SELLER NAME1'].isin(['FEDERAL|FEDL'])]

            # Remove empty rows in RESALE/NEW_CONSTRUCTION
            data = data[data['RESALE/NEW_CONSTRUCTION'].isin(['M', 'N'])]

            # Remove mobile and manufactured homes
            data = data[~data['SELLER NAME1'].str.contains('MOBILE HOME|MANUFACTURED HOME', na=False)]
            data = data[~data['LAND_USE'].isin(['135','137','138','454','775'])]

            # Remove equity loan (增貸)
            data = data[data['EQUITY_FLAG'] != 'Y']

            # Remove Re-finance

            # Here we include all the reasonable LAND_USE_CODE that could
            # be available on the residential housing market. (plz refer to the deed codebook)
            # NOTE: but we might need to exclude the multi-family ones later...
            keep_list = [
                '100', '102', '103', '106', '109', '111', '112', '113', '114',
                '115', '116', '117', '118', '131', '132', '133', '134', '148',
                '151', '163', '165', '245', '248', '281', '460', '465'
            ]
            data = data[data['LAND_USE'].isin(keep_list)]

            # Remove Deed Duplicate Data
            # rows with same first two values but different in third got different sale amount
            dup_list = ['APN_UNFORMATTED', 'BATCH-ID', 'BATCH-SEQ']
            data = data.drop_duplicates(subset=dup_list)

            # Drop columns who had done their responsibility
            to_drop = [
                'OWNER_1_LAST_NAME', 'OWNER_1_FIRST_NAME&MI', 'INTER_FAMILY', "PRI-CAT-CODE",
                'FORECLOSURE', 'DOCUMENT TYPE', 'EQUITY_FLAG'
            ]
            data = data.drop(columns=to_drop)

        # No filter required for tax data, since we are left joining with deed later, and APN in tax file is unique

        return data

    def _read_n_clean(self, filename: str, deed_or_tax: str = "") -> pd.DataFrame:
        '''

        Parameters
        ----------
        filename: str.
            Should be the full path to the file.

        Return
        ------
            the cleaned dataframe.
        '''
        cols_tax = {
            # "FIPS_CODE": 'category', # county
            "APN_NUMBER_UNFORMATTED": 'str', # APN
            # "PROPERTY_INDICATOR_CODE", # 10-19: residential, ..., 80-89: vacant
            # "ASSESSED_TOTAL_VALUE",
            # "MARKET_TOTAL_VALUE",
            # "APPRAISED_TOTAL_VALUE",
            # "TAX_AMOUNT",
            "LAND_SQUARE_FOOTAGE": 'str',
            "UNIVERSAL_BUILDING_SQUARE_FEET": 'str',
            "BUILDING_SQUARE_FEET_INDICATOR_CODE": 'category', # A: adjusted; B: building; G: gross; H: heated; L: living; M: main; R: ground floor
            "BUILDING_SQUARE_FEET": 'str', # doesn't differentiate living and non-living (if lack indicator code)
            "LIVING_SQUARE_FEET": 'str'
        }
        cols_deed = {
            "FIPS": 'category', # county
            "APN_UNFORMATTED": 'str', # APN
            "BATCH-ID": 'str', # actually in the form of yyyymmdd, not the same as sale date
            "BATCH-SEQ": 'str',
            "OWNER_1_LAST_NAME": 'str',
            "OWNER_1_FIRST_NAME&MI": 'str',
            "SELLER NAME1": 'str', # name of the first seller
            "SALE AMOUNT": 'str',
            "SALE DATE": 'str',
            "LAND_USE": 'category', # plz refer to codebook, there are 263 different
            "PROPERTY_INDICATOR": 'category', # residential, commercial, refer to top of this code file
            "RESALE/NEW_CONSTRUCTION": 'category', # M: re-sale, N: new construction
            "INTER_FAMILY": 'category',
            "PRI-CAT-CODE": 'category',
            "FORECLOSURE": 'category',
            "DOCUMENT TYPE": 'category',
            "EQUITY_FLAG": 'category'
            # "REFI_FLAG": 'category' # all NaN
        }

        data_type_spec = cols_tax if deed_or_tax.lower() == 'tax' else cols_deed

        def handle_bad_line(bad_line):
            # this filename is the full path
            to_log= f"{filename} skipping line: {bad_line}"
            self.write_log(to_log)
            return None  # Returning None tells pandas to skip this line

        dfs = []

        for chunk in pd.read_csv(
            filename, delimiter="|",
            chunksize=CHUNK_SIZE, # can only work with engine = c or python
            usecols=data_type_spec.keys(), dtype=data_type_spec,
            # on_bad_lines=handle_bad_line, # callable can only work with engine = python or pyarrow
            on_bad_lines='skip', # skip the bad rows for now, guess there won't be many (cannot check tho)
            engine='c',
            quoting=csv.QUOTE_NONE
        ):
            dfs.append(self._filter(chunk, deed_or_tax=deed_or_tax))

        out_df = pd.concat(dfs, ignore_index=True)

        # align the unique key column
        if 'APN_NUMBER_UNFORMATTED' in out_df.columns:
            out_df.rename(columns={'APN_NUMBER_UNFORMATTED': 'APN_UNFORMATTED'}, inplace=True)

        # replace spaces in column names with "_"
        out_df.columns = out_df.columns.str.replace(' ', '_')

        return out_df

    def stack_files(self, files = None) -> pd.DataFrame:
        '''
        The actions done here will be based on the data type of "files"
        str: on single file
        list: on the list of files
        None: all of the files in the directory
        '''
        txt_extention = ".txt"

        def get_data(fips: str):
            cleaned_data = {}
            for cur_type in ['deed', 'tax']:
                cleaned_data[cur_type] = self._read_n_clean(
                    filename=f'{BULK_PATH}bulk_{cur_type}_fips_split/fips-{fips}-UniversityofPA_Bulk_{cur_type.capitalize()}.txt',
                    deed_or_tax=cur_type
                )
            return cleaned_data['deed'].merge(cleaned_data['tax'],
                                              on='APN_UNFORMATTED', how='left')

        dataframes = []

        if not files:
            # loop through one of the folder to get
            for f in tqdm(os.listdir(self.__ref_filepath), desc=f"Merging files"):
                if f.endswith(txt_extention):
                    # use only the FIPS to run
                    dataframes.append(get_data(f[5:10]))
        elif isinstance(files, list):
            for f in files:
                dataframes.append(get_data(f))
        else:
            dataframes.append(get_data(files))

        return pd.concat(dataframes, ignore_index=True)

    def clean_one_save_one(self, out_path: str, files = None) -> pd.DataFrame:
        '''
        The actions done here will be based on the data type of "files"
        str: on single file
        list: on the list of files
        None: all of the files in the directory
        '''
        txt_extention = ".txt"

        def get_data(fips: str):
            cleaned_data = {}
            for cur_type in ['deed', 'tax']:
                cleaned_data[cur_type] = self._read_n_clean(
                    filename=f'{BULK_PATH}bulk_{cur_type}_fips_split/fips-{fips}-UniversityofPA_Bulk_{cur_type.capitalize()}.txt',
                    deed_or_tax=cur_type
                )
            return cleaned_data['deed'].merge(cleaned_data['tax'],
                                              on='APN_UNFORMATTED', how='left')


        if not files:
            # loop through one of the folder to get
            for f in tqdm(os.listdir(self.__ref_filepath), desc=f"Merging files"):
                if f.endswith(txt_extention):
                    # use only the FIPS to run
                    fips = f[5:10]
                    self.data_output(
                        df=get_data(fips),
                        filename=f"{fips}.csv",
                        out_path=out_path
                    )
        elif isinstance(files, list):
            for f in files:
                self.data_output(
                    df=get_data(f),
                    filename=f"{f}.csv",
                    out_path=out_path
                )
        else:
            self.data_output(
                df=get_data(files),
                filename=f"{files}.csv",
                out_path=out_path
            )

        return

    def data_output(self, df: pd.DataFrame, filename: str, out_path: str) -> None:
        if not os.path.exists(out_path):
            os.makedirs(out_path)

        if not filename.endswith(".csv"):
            filename = filename + ".csv"

        if out_path not in filename:
            filename = out_path + filename

        df.to_csv(f'{filename}', index=False)
        # print(f"{filename} generated.")
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

    def deed_peep(self):
        data_type_spec = {
            "APN_UNFORMATTED": 'str', # APN
            "BATCH-ID": 'str', # actually in the form of yyyymmdd, not the same as sale date
            "BATCH-SEQ": 'category',
            # "OWNER_1_LAST_NAME": 'str',
            # "OWNER_1_FIRST_NAME&MI": 'str',
            # "SELLER NAME1": 'str', # name of the first seller
            "SALE AMOUNT": 'str',
            # "SALE DATE": 'str',
            # "LAND_USE": 'category', # plz refer to codebook, there are 263 different
            # "PROPERTY_INDICATOR": 'category', # residential, commercial, refer to top of this code file
            # "RESALE/NEW_CONSTRUCTION": 'category', # M: re-sale, N: new construction
            # "INTER_FAMILY": 'category',
            # "PRI-CAT-CODE": 'category',
            # "FORECLOSURE": 'category',
            # "DOCUMENT TYPE": 'category',
            # "LAND_USE": 'category',
            # "EQUITY_FLAG": 'category', # not sure what to rule out
            # "REFI_FLAG": 'category' # not sure what to rule out
        }
        dfs = []
        filename = f"{BULK_PATH}bulk_deed_fips_split/fips-06037-UniversityofPA_Bulk_Deed.txt"
        for chunk in pd.read_csv(
            filename, delimiter="|",
            chunksize=CHUNK_SIZE, # can only work with engine = c or python
            usecols=data_type_spec.keys(), dtype=data_type_spec,
            # on_bad_lines=handle_bad_line, # callable can only work with engine = python or pyarrow
            on_bad_lines='skip', # skip the bad rows for now, guess there won't be many (cannot check tho)
            engine='c',
            quoting=csv.QUOTE_NONE
        ):
            dfs.append(chunk)

        tmp = pd.concat(dfs, ignore_index=True)

        tmp = tmp.sort_values(by=['APN_UNFORMATTED', 'BATCH-ID', 'BATCH-SEQ', 'SALE AMOUNT']).reset_index(drop=True)

        print(tmp.head(35))

    def tax_peep(self):
        data_type_spec = {
            "APN_NUMBER_UNFORMATTED": 'str', # APN
            "BATCH_ID": 'str', # actually in the form of yyyymmdd
            "BATCH_SEQ": 'category',
            # "OWNER_1_LAST_NAME": 'str',
            # "OWNER_1_FIRST_NAME": 'str',
            # "TAX_YEAR": 'category',
            # "SELLER_NAME": 'str', # just to make sure
            # "SALE_AMOUNT": 'str', # just to make sure
            # "SALE_DATE": 'str', # just to make sure
            # "LAND_SQUARE_FOOTAGE": 'str',
            # "UNIVERSAL_BUILDING_SQUARE_FEET": 'str',
            # "BUILDING_SQUARE_FEET_INDICATOR_CODE": 'category', # A: adjusted; B: building; G: gross; H: heated; L: living; M: main; R: ground floor
            # "BUILDING_SQUARE_FEET": 'str', # doesn't differentiate living and non-living (if lack indicator code)
            # "LIVING_SQUARE_FEET": 'str'
        }
        dfs = []
        filename = f"{BULK_PATH}bulk_tax_fips_split/fips-06037-UniversityofPA_Bulk_Tax.txt"
        for chunk in pd.read_csv(
            filename, delimiter="|",
            chunksize=CHUNK_SIZE, # can only work with engine = c or python
            usecols=data_type_spec.keys(), dtype=data_type_spec,
            # on_bad_lines=handle_bad_line, # callable can only work with engine = python or pyarrow
            on_bad_lines='skip', # skip the bad rows for now, guess there won't be many (cannot check tho)
            engine='c',
            quoting=csv.QUOTE_NONE
        ):
            dfs.append(chunk)

        tmp = pd.concat(dfs, ignore_index=True)

        tmp = tmp.sort_values(by=['APN_NUMBER_UNFORMATTED', 'BATCH_ID', 'BATCH_SEQ']).reset_index(drop=True)

        print(tmp.head(50))


def main():
    start_t = time.time()
    current_date = datetime.now().strftime("%m%d")
    log = f'{current_date}.log'

    # =============
    #  Experiments
    # =============
    # check the duplication of apn in tax and deed
    p = Preprocess()
    # p.tax_peep()
    # p.deed_peep()

    # ======================
    #  Stack files together
    # ======================
    p = Preprocess()

    file_list = [
        '01001',
        '01003',
        # '01005',
        # '06037',
        # '01009',
        # '01011',
        # '01013',
        # '01015',
        # '01017',
        # '04019',
    ]
    # if provide no "files", the function would run through whole path
    p.clean_one_save_one(files=None, out_path=OUT_PATH)

    # Currently don't consider generating stack file, because the outfile would
    # be about 7GB, making later analysis difficult, where invole plenty of
    # operations that explode the memory...
    # data = p.stack_files(files=None)
    # p.data_output(data, f'merged_stacked.csv', out_path=EXT_DISK+"processed_data/")

    end_t = time.time()
    time_spent = end_t - start_t
    print(f"Done!! Time spent: {time_spent:.4f} seconds")

if __name__ == "__main__":
    main()