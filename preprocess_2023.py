import dask.dataframe as dd
import numpy as np
import os
from tqdm import tqdm
import re
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

BULK_PATH = "F:/CoreLogic/"

# NOTE:
#   The old deed code book still got useful info, such as the land use code

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
            filtered data.
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
        data = data[~((data['NEW_CONSTRUCTION_IND'] == 0) & (data['RESALE_IND'] == 0))]
        # Column for resale and new construction
        data['NEW_HOME_ORIG'] = np.where((data['NEW_CONSTRUCTION_IND'] == '1') & (data['RESALE_IND'] == '0'), True, False)

        # Remove if sale price is less than 1,000 to avoid nominal transactions
        data = data[(data['SALE_AMOUNT'].astype('float') >= 1000) | (data['SALE_AMOUNT'].isna())]

        # Remove mobile and manufactured homes
        data = data[data['MOBILE_HOME_IND'] != 'Y']   
        data = data[~data['SELLER_1_FULL_NAME'].str.contains('MOBILE HOME|MANUFACTURED HOME|CONDOMINIUM|CONDO|APARTMENT', na=False)]
        data = data[~data['LAND_USE_CODE'].isin(['135','137','138','454','775'])]

        # Here we include all the reasonable LAND_USE_CODE that could
        # be available on the residential housing market. (plz refer to the old deed codebook)
        # (different with Jaimie because of different research target)
        keep_list = [
            '100', '102', '103', '106', '109', '111', '112', '113', '114', 
            '115', '116', '117', '118', '131', '132', '133', '134', '148', 
            '151', '163', '165', '245', '248', '281', '460', '465'
        ]

        # Remove Deed Duplicate Data (same date, same seller, same buyer, same parcel, same price)
        dup_list = ['CLIP','SALE_DATE','SELLER_1_FULL_NAME','BUYER_1_FULL_NAME','SALE_AMOUNT']
        data.drop_duplicates(subset=dup_list, inplace=True)

        # Drop columns who had done their responsibility
        to_drop = [
            'INTERFAMILY_RELATED_IND', 'RESALE_IND', 'NEW_CONSTRUCTION_IND',
            'SHORT_SALE_IND', 'FORECLOSURE_REO_IND', 'FORECLOSURE_REO_SALE_IND'
        ]
        data = data.drop(columns=to_drop)

        return data
    
    def deed_clean(self):
        ddf = dd.read_csv(self.bulkfile, delimiter="|", dtype='str')

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
        ddf.rename(columns=dict(zip(old_col, new_col)), inplace=True)

        # In this version, CLIP can identify unique property
        # I saw some of the buyer 1 full name includes name of buyer 1 and 2
        keep_cols = [
            'CLIP', 'LAND_USE_CODE', 'TRANSACTION_FIPS_CODE', 'SALE_AMOUNT',
            'SALE_DERIVED_DATE', 'SALE_DERIVED_RECORDING_DATE',
            'INTERFAMILY_RELATED_IND', 'RESALE_IND', 'NEW_CONSTRUCTION_IND',
            'SHORT_SALE_IND', 'FORECLOSURE_REO_IND', 'FORECLOSURE_REO_SALE_IND',
            'BUYER_1_FULL_NAME', 'SELLER_1_FULL_NAME'
        ]

        ddf = ddf[keep_cols]

        ddf = self.deed_filter(ddf)

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

        # ==============
        #  Make columns
        # ==============
        # Date: SALE_DERIVED_DATE, if '0' then SALE_DERIVED_RECORDING_DATE
        # drop NA and '0' in the end


        return

    def just_checking(self):
        ddf = dd.read_csv(self.bulkfile, delimiter="|", dtype='str')

        # =======================================
        #  Save a small dataset for clearer view
        # =======================================
        # tmp = ddf.head(30)
        # tmp.to_csv("./log/deed_2023_first30.txt", index=False)

        # ================
        #  Basic checking
        # ================
        cols_to_check = [
            'PROPERTY INDICATOR CODE - STATIC',
            'MULTI OR SPLIT PARCEL CODE',
            'PRIMARY CATEGORY CODE',
            'DEED CATEGORY TYPE CODE',
            'RECORD ACTION INDICATOR'
        ]
        for c in cols_to_check:
            print(f"==> Current col: {c}")
            print(ddf[c].value_counts(dropna=False)) # counting include NA

        ttl_rows = ddf['FIPS'].shape[0].compute() # but why len(ddf['FIPS']) won't work?
        print(ttl_rows)

        return

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