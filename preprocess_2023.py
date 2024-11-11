import dask.dataframe as dd
import dask
import dask.bag as db
from dask.distributed import Client
import numpy as np
import os
from tqdm import tqdm
import re
from datetime import datetime
import platform
import time
import csv
import warnings
warnings.filterwarnings("ignore")

# FOR MAC
# virtual env: source env/bin/activate
# if no pip: curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
#      then: python3 get-pip.py

CUR_SYS  = platform.system()

BULK_PATH = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
BULK_PATH += "CoreLogic/"
BLOCKSIZE = "100MB"


# NOTE:
#   The old deed code book still got useful info, such as the land use code

class Preprocess():
    def __init__(
            self, log_name: str = "", log_path: str = "./log/", out_path: str = './output/',
            bulk_deed: str = "duke_university_ownertransfer_v3_dpc_01465911_20230803_072211_data.txt",
            bulk_tax: str = "duke_university_property_basic2_dpc_01465909_20230803_072103_data.txt"
        ) -> None:
        self.bulk_deed = f"{BULK_PATH}{bulk_deed}"
        self.bulk_tax = f"{BULK_PATH}{bulk_tax}"
        self.__log_file = log_path + log_name
        self.__out_path = out_path

        if not os.path.exists(log_path):
            os.makedirs(log_path)

        if not os.path.exists(out_path):
            os.makedirs(out_path)

        if log_name:
            if not os.path.exists(self.__log_file):
                with open(self.__log_file, 'w') as f:
                    pass  # Just create an empty file

    def write_log(self, message):
        """Writes message to the log file."""
        with open(self.__log_file, 'a') as f:
            f.write(f"{message}\n")


    def split_file(self, which_file: str):
        '''

        '''
        filename = self.bulk_deed if which_file.lower() == 'deed' else self.bulk_tax

        # ddf = dd.read_csv(
        #     filename, blocksize='1GB',
        #     header=None, sep='\n', engine='python',
        #     on_bad_lines='skip', quoting=csv.QUOTE_NONE
        # )
        # split to 1GB files and save. TODO: might also want this to be argument
        dask_bag = db.read_text(
            filename, blocksize='300MB', encoding='utf-8', errors='ignore'
        )

        # Save each partition to separate files
        # TODO: should add an argument: dest
        dask_bag.to_textfiles('/Volumes/KINGSTON/CoreLogic/deed_2023/deed_2023_part_*.txt')

        # Split and save each partition to a new file
        # for i, partition in enumerate(ddf.to_delayed()):
        #     partition.compute().to_csv(f'/Volumes/KINGSTON/CoreLogic/deed_2023/deed_2023_{i+1:04d}.txt', index=False, header=False)

    def deed_filter(self, data):
        '''
        This largely follows Jaimie's (jaimie.choi@duke.edu) NewClean.py
            > remove Non-Arms Length (interfamily)
            > restrict to only single family, condominium, and duplex homes
            > remove foreclosure sales and any transaction made against the home

        Parameters
        ----------
            data: pandas dataframe or dask dataframe

        Return
        ------
            filtered data, same data type as the input.
        '''
        # Remove Non-Arms Length Transactions
        data = data[data['INTERFAMILY_RELATED_IND'] == '0']
        # A: 'Arms Length Transaction', B-C: 'Non Arms Length' (pri-cat-code in old codebook)
        data = data[data['PRIMARY_CATEGORY_CODE'] == 'A']
        # Remove if first 10 characters are identical for seller and buyer name
        data = data[
            (data['SELLER_1_FULL_NAME'].str[0:10] != data['BUYER_1_FULL_NAME'].str[0:10]) |
            ((data['SELLER_1_FULL_NAME'] == 'OWNER RECORD') | (data['BUYER_1_FULL_NAME'] == 'OWNER RECORD'))
        ] # 其實有點不懂為甚麼要特別把owner record放這

        # Remove foreclosure and government-to-private party transactions (REO-nominal, REO Sale: Gov to private party)
        data = data[(data['FORECLOSURE_REO_IND'] == '0') & (data['FORECLOSURE_REO_SALE_IND'] == '0')]
        # Remove foreclosure & in lieu of foreclosure deed
        data = data[(data['DEED_CATEGORY_TYPE_CODE'] != 'U') & (data['DEED_CATEGORY_TYPE_CODE'] != 'DL')]
        # Remove nominal deed
        data = data[(data['DEED_CATEGORY_TYPE_CODE'] != 'Z') & (data['DEED_CATEGORY_TYPE_CODE'] != '^')]
        # Remove if seller is related to Federal Homes Loan Mortgage (Fannie Mae) -- foreclosure homes --
        data = data[~data['SELLER_1_FULL_NAME'].isin(['FEDERAL|FEDL'])]

        # Remove short sales (typically by financially distressed homeowner trying to evade property seizure)
        data = data[data['SHORT_SALE_IND'] == '0']

        # Remove if neither resale or new construction
        data = data[~((data['NEW_CONSTRUCTION_IND'] == '0') & (data['RESALE_IND'] == '0'))]
        # Column for resale and new construction
        data = data.assign(NEW_HOME_ORIG=(data['NEW_CONSTRUCTION_IND'] == '1') & (data['RESALE_IND'] == '0'))

        # Remove if sale price is less than 1,000 to avoid nominal transactions
        data = data[(data['SALE_AMOUNT'] >= 1000) | (data['SALE_AMOUNT'].isna())]

        # Remove mobile and manufactured homes
        data = data[data['MOBILE_HOME_IND'] != 'Y']
        data = data[~data['SELLER_1_FULL_NAME'].str.contains('MOBILE HOME|MANUFACTURED HOME|CONDOMINIUM|CONDO|APARTMENT', na=False)]
        data = data[~data['LAND_USE_CODE'].isin(['135','137','138','454','775'])]

        # Here we include all the reasonable LAND_USE_CODE that could
        # be available on the residential housing market. (plz refer to the old deed codebook)
        # (different with Jaimie because of different research target)
        # NOTE: but we might need to exclude the multi-family ones later...
        keep_list = [
            '100', '102', '103', '106', '109', '111', '112', '113', '114',
            '115', '116', '117', '118', '131', '132', '133', '134', '148',
            '151', '163', '165', '245', '248', '281', '460', '465'
        ]
        data = data[data['LAND_USE_CODE'].isin(keep_list)]

        # Remove Deed Duplicate Data (same date, same seller, same buyer, same parcel, same price)
        dup_list = ['CLIP','SALE_DATE','SELLER_1_FULL_NAME','BUYER_1_FULL_NAME','SALE_AMOUNT']
        data = data.drop_duplicates(subset=dup_list)

        # Drop columns who had done their responsibility
        to_drop = [
            'INTERFAMILY_RELATED_IND', 'RESALE_IND', 'NEW_CONSTRUCTION_IND',
            'SHORT_SALE_IND', 'FORECLOSURE_REO_IND', 'FORECLOSURE_REO_SALE_IND'
        ]
        data = data.drop(columns=to_drop)

        return data

    def deed_clean(
        self, filename: str,
        save_file: bool = False,
        outfile = 'deed_cleaned'
    ):
        '''
        Assume in this function can handle all the column thing...
        '''
        data_type_spec = {
            'CLIP': 'str', # 1006407533
            'LAND USE CODE - STATIC': 'category',
            'MOBILE HOME INDICATOR': 'category',
            'TRANSACTION FIPS CODE': 'category',
            'SALE AMOUNT': 'float64',
            'SALE DERIVED DATE': 'str', # 20210908
            'SALE DERIVED RECORDING DATE': 'str',
            'PRIMARY CATEGORY CODE': 'category',
            'DEED CATEGORY TYPE CODE': 'category',
            'INTERFAMILY RELATED INDICATOR': 'category',
            'RESALE INDICATOR': 'category',
            'NEW CONSTRUCTION INDICATOR': 'category',
            'SHORT SALE INDICATOR': 'category',
            'FORECLOSURE REO INDICATOR': 'category',
            'FORECLOSURE REO SALE INDICATOR': 'category',
            'BUYER 1 FULL NAME': 'str',
            'SELLER 1 FULL NAME': 'str'
        }

        # TODO: read the file for setting header
        # Check if the file has header, if not then get header file and
        # attach to the first row
        # OR I JUST MANUALLY DO THIS??
        # ** might need to read file twice, first for checking if header exist
        #    the second time do the real process thing.

        # [AFTER SPLITTING] first need to check if each file could be opened.

        # planned second open
        ddf = dd.read_csv(
            filename, delimiter="|", blocksize=BLOCKSIZE,
            usecols=data_type_spec.keys(), dtype=data_type_spec,
            on_bad_lines='skip', quoting=csv.QUOTE_NONE
        )

        old_col = ddf.columns
        new_col = [
            s\
                .replace(r' - STATIC','')\
                .replace('DEED SITUS ','')\
                .replace('INDICATOR','IND')\
                .replace('CORPORATE','CORP')
            for s in old_col
        ]
        new_col = ['_'.join(s.split()) for s in new_col]
        ddf = ddf.rename(columns=dict(zip(old_col, new_col)))

        # In this version, CLIP can identify unique property
        # I saw some of the buyer 1 full name includes name of buyer 1 and 2
        keep_cols = [
            'CLIP', 'LAND_USE_CODE', 'TRANSACTION_FIPS_CODE', 'SALE_AMOUNT',
            'SALE_DERIVED_DATE', 'SALE_DERIVED_RECORDING_DATE',
            'PRIMARY_CATEGORY_CODE', 'DEED_CATEGORY_TYPE_CODE',
            'INTERFAMILY_RELATED_IND', 'RESALE_IND', 'NEW_CONSTRUCTION_IND',
            'SHORT_SALE_IND', 'FORECLOSURE_REO_IND', 'FORECLOSURE_REO_SALE_IND',
            'MOBILE_HOME_IND',
            'BUYER_1_FULL_NAME', 'SELLER_1_FULL_NAME'
        ]

        # ddf = ddf[keep_cols] # might not need this, since we already does this when reading csv

        # ========================
        #  Make SALE_DATE & Clean
        # ========================
        # Date: SALE_DERIVED_DATE, if '0' then SALE_DERIVED_RECORDING_DATE
        ddf['SALE_DATE'] = ddf['SALE_DERIVED_DATE']\
            .where(ddf['SALE_DERIVED_DATE'] != '0', ddf['SALE_DERIVED_RECORDING_DATE'])
        # drop NA and '0' in the end
        ddf = ddf.dropna(subset=['SALE_DATE'])
        ddf = ddf[ddf['SALE_DATE'] != '0']

        ddf = ddf.drop(columns=['SALE_DERIVED_DATE', 'SALE_DERIVED_RECORDING_DATE'])

        # ==================
        #  Do the filtering
        # ==================
        # Convert columns to numeric types, coercing errors to NaN
        # ddf['SALE_AMOUNT'] = dd.to_numeric(ddf['SALE_AMOUNT'], errors='coerce')

        ddf = self.deed_filter(ddf)

        # only 'CLIP', 'LAND_USE_CODE', 'TRANSACTION_FIPS_CODE', 'SALE_AMOUNT', 'DATE',
        # 'BUYER_1_FULL_NAME', 'SELLER_1_FULL_NAME' left

        if save_file:
            ddf.to_csv(self.__out_path+f'{outfile}_*.csv', single_file=True)
        else:
            return ddf.compute()

    def batch_process(self, inpath: str, outpath = ""):
        '''

        '''
        save_file = True if outpath else False

        to_cat = [] # only works if no outpath specified
        # 1. for each file in the path, do deed_clean
        for filename in os.listdir(path=inpath):
            if filename.endswith('.txt'):
                file_path = os.path.join(inpath, filename)

                # 2. the out name should follow the input filename
                #    plus, which path is it going to be saved to
                outname = outpath + 'deed_cleaned'
                to_cat.append(self.deed_clean(
                    filename, save_file=save_file, outfile=outname))

    def _deed_check(self, cleaned_deed):
        '''
        This function is to do some simple analysis on the cleaned deed file.
        '''
        ddf = dd.read_csv("")

        # the following columns need to be checked for dist. after filtering
        to_check = [
            'CLIP', 'LAND_USE_CODE', 'TRANSACTION_FIPS_CODE', 'SALE_AMOUNT'
        ]

        # 1. check non NA amount
        row_cnt = ddf['CLIP'].shape[0].compute()
        good_counts = ddf[to_check].count().compute()
        print("Non NA pct for ...")
        for i, c in enumerate(to_check):
            print(f"  {c:<11}: {good_counts.iloc[i]*100/row_cnt:>10.5f}%")

        # 2. exclude NA then check distribution (exclude SALE_AMOUNT)
        for c in to_check:
            if c != 'SALE_AMOUNT':
                print(f'Now checking {c}')
                print(ddf[c].value_counts()) # exclude NA automatically

    def just_checking(self, file_type: str):
        '''

        Parameters
        ----------
        file_type: str
            can only be "deed" or "tax"
        '''
        cur_file = self.bulk_deed if file_type.lower() == 'deed' else self.bulk_tax
        ddf = dd.read_csv(cur_file, delimiter="|", dtype='str', on_bad_lines='skip')

        # =======================================
        #  Save a small dataset for clearer view
        # =======================================
        # top_n = 100
        # tmp = ddf.head(top_n)
        # tmp.to_csv(f"./log/{cur_file}_2023_first{top_n}.txt", index=False)

        # =====================
        #  Basic checking Deed
        # =====================
        # cols_to_check = [
        #     'PROPERTY INDICATOR CODE - STATIC',
        #     'MULTI OR SPLIT PARCEL CODE',
        #     'PRIMARY CATEGORY CODE',
        #     'DEED CATEGORY TYPE CODE',
        #     'RECORD ACTION INDICATOR'
        # ]
        # for c in cols_to_check:
        #     print(f"==> Current col: {c}")
        #     print(ddf[c].value_counts(dropna=False)) # counting include NA

        # ttl_rows = ddf['FIPS'].shape[0].compute() # but why len(ddf['FIPS']) won't work?
        # print(ttl_rows)

        return

def main():
    dask.config.set({'distributed.worker.memory.spill': True})

    # to speed up the Dask computation
    if CUR_SYS == 'Darwin':
        # Configure client settings for local computation:
        #    number of machines, core, half of memory
        print("Now on Mac!")
        client = Client(n_workers=1, threads_per_worker=8, memory_limit='10GB')
    else:
        client = Client(n_workers=1, threads_per_worker=2, memory_limit='4GB')


    # print(client.dashboard_link)

    current_date = datetime.now().strftime("%m%d")
    log = f'data_process.log'

    p = Preprocess()

    # p.just_checking()

    # ======================
    #  Break the large file
    # ======================
    p.split_file(which_file='deed')

    # ========================
    #  Process splitted files
    # ========================
    # if outpath specified, the cleaned files would be saved to that dir
    p.batch_process(BULK_PATH+'deed_2023', outpath='output/deed_2023_cleaned')

    # ===================
    #  Filter Bulk Files
    # ===================
    # [avoid run this unless your CPU is more than 8 cores and memory is larger than 64GB]
    # p.deed_clean(filename=p.bulk_deed, save_file=True)

    client.close()

    # Records
    # died on 186265

    print("Done!!")


if __name__ == "__main__":
    main()