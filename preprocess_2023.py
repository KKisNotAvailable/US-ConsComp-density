import pandas as pd
import numpy as np
import os
import glob
from tqdm import tqdm
import re
from datetime import datetime
import platform
import time
import csv
from typing import Tuple
import warnings
warnings.filterwarnings("ignore")

# FOR MAC
# virtual env: source env/bin/activate
# if no pip: curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
#      then: python3 get-pip.py

CUR_SYS  = platform.system()

ROOT_PATH = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
DATA_PATH = ROOT_PATH + "CoreLogic/"
CLEANED_PATH = "./output/cleaned_by_county/"

DEED_FOLDER = DATA_PATH + "deed_split_2023/"
TAX_FOLDER = DATA_PATH + "tax_split_2023/"
HIST_TAX_FOLDER = DATA_PATH + "hist_tax_split_2023/"


CHUNK_SIZE = 1000000 # this for Deed files is about 500MB


# NOTE:
#   The old deed code book still got useful info, such as the land use code

class Utils():
    def __init__(self):
        pass

    def split_clip_apn(self, data: pd.DataFrame, col_clip: str, col_apn: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        '''
        This function is aimed at sperating data into three pieces:
        the one that has CLIP, one has no CLIP but has APN, one has neither.

        Parameters
        ----------
        data: pd.DataFrame.
            the data we wish to split.
        col_clip: str.
            the column name for CLIP.
        col_apn: str.
            the column name for APN.

        Return
        ------
            three pd.DateFrames, that use CLIP as ID, use APN as ID, and no ID.
        '''
        # (1) with CLIP
        cond_clip = data[col_clip].notna()
        clip_df = data[cond_clip].drop(columns=[col_apn])

        # (2) without CLIP (having NA values) but with APN (unformatted)
        cond_apn = data[col_apn].notna()
        apn_df = data[(~cond_clip) & cond_apn].drop(columns=[col_clip])

        # (3) with neither CLIP nor APN
        remaining = data[(~cond_clip) & (~cond_apn)]

        return clip_df, apn_df, remaining

    def get_serial_num(self, hist_tax_fname: str) -> str:
        '''This function is to extract the serial number in the historical tax'''
        idx = hist_tax_fname.find("202308")
        return hist_tax_fname[idx-3:idx-1]

    def tax_file_fullname(self, fips) -> str:
        return f"FIPS_{fips}_duke_university_property_basic2_dpc_01465909_20230803_072103_data.txt"

    def deed_file_fullname(self, fips) -> str:
        return f"FIPS_{fips}_duke_university_ownertransfer_v3_dpc_01465911_20230803_072211_data.txt"


class CheckData():
    def __init__(self, deed_path, tax_path, hist_tax_path):
        self.data_paths = {
            "deed": deed_path,
            "tax": tax_path,
            "hist_tax": hist_tax_path
        }
        self.util = Utils()

    def fips_coverage(self, base_src='deed', cover_srcs=['tax']):
        '''
        This function checks whether the cover_srcs contain all of the FIPS
        in the base_src. Will present coverage rate and number of FIPS not
        covered.
        Result: only 2 FIPS in deed not covered.

        Parameters
        ----------
        base_src: str.
            accept only 'deed', 'tax', 'hist_tax'.
        cover_src: list of str.
            for the strings in list, accept only 'deed', 'tax', 'hist_tax'.

        Return
        ------
            None. But will print result on the terminal output.
        '''
        base_fips, cover_fips = [], []

        for base_fname in os.listdir(self.data_paths[base_src]):
            if base_fname[:4] != "FIPS":
                continue
            base_fips.append(base_fname[5:10])

        for c_src in cover_srcs:
            for cover_name in os.listdir(self.data_paths[c_src]):
                if cover_name[:4] != "FIPS":
                    continue
                cover_fips.append(cover_name[5:10])

        base_fips = set(base_fips)
        cover_fips = set(cover_fips)

        if not base_fips.issubset(cover_fips):
            no_property_info = list(base_fips - cover_fips)  # Convert set difference to list
            print(len(no_property_info), "FIPS not covered.")
            print(f"Coverage rate is {100*(1 - len(no_property_info)/len(base_fips)):.2f}%.")

    def property_info_consistency(
            self, with_tax: bool = True, tax_spec: dict = None,
            check_amt: int = 5
        ):
        '''
        This function checks whether the property info (currently only building
        square feet) in historical tax, for the same CLIP or APN, is consistent
        across files/years.
        Optionally, this check could include the tax data.
        Result: checked only one FIPS, and the discrepancy rate is high enough
                to stop (80%).
                => the property info does change over time, so we have to
                   consider year when merging tax and deed.

        Parameters
        ----------
        with_tax: bool.
            if would like to include tax data when checking.
        tax_spec: dict.
            the column data type specification, for memory efficiency.
        check_amt: int.
            the amount of FIPS we wish to check (since total of 3000 counties
            is overwhelming)

        Return
        ------
            None. Results will be printed.
        '''
        fullnames = {
            "deed": [],
            "tax": [],
            "hist_tax": []
        }
        fips = {
            "deed": [],
            "tax": [],
            "hist_tax": []
        }

        # store the file full names and extracted FIPS in seperate lists
        for src in fullnames.keys():
            for fname in os.listdir(self.data_paths[src]):
                if fname[:4] != "FIPS":
                    continue
                fullnames[src].append(fname)
                fips[src].append(fname[5:10])

        def has_difference(group):
            '''works on multiple columns'''
            # group.nunique() returns the number of unique values in each column
            return (group.nunique() > 1).any()

        # start counting discrepant CLIP or APN for each county
        county_id_cnt, county_discrep_cnt = {}, {}

        ctr = check_amt

        for d_fips in fips['deed']:
            if d_fips not in fips['tax'] and d_fips not in fips['hist_tax']:
                print(f"FIPS {d_fips} does not have property information.")
                continue

            # if has corresponding FIPS, read all the files and stack them up
            hist_tax_files = [f for f in fullnames['hist_tax'] if d_fips in f]
            tax_files = [f for f in fullnames['tax'] if d_fips in f] if with_tax else []

            to_cat = []

            for f in hist_tax_files:
                fpath = os.path.join(self.data_paths['hist_tax'], f)
                cur_file = pd.read_csv(
                    fpath, delimiter="|",
                    usecols=tax_spec.keys(), dtype=tax_spec,
                    on_bad_lines='skip', quoting=csv.QUOTE_NONE
                )
                to_cat.append(cur_file)

            for f in tax_files:
                fpath = os.path.join(self.data_paths['tax'], f)
                cur_file = pd.read_csv(
                    fpath, delimiter="|",
                    usecols=tax_spec.keys(), dtype=tax_spec,
                    on_bad_lines='skip', quoting=csv.QUOTE_NONE
                )
                to_cat.append(cur_file)

            stacked_tax = pd.concat(to_cat, ignore_index=True)

            # since some of the rows has no CLIP or has neither, we split the
            # data accordingly
            by_clip, by_apn, _ = self.util.split_clip_apn(
                data=stacked_tax, col_clip='CLIP',
                col_apn='APN (PARCEL NUMBER UNFORMATTED)'
            )

            # we save the number of total id (CLIP or APN)
            # and number of ids with inconsistent record for later deciding
            # how to merge the data
            res_clip = by_clip.groupby('CLIP').apply(has_difference)
            clip_id_cnt = len(res_clip)
            # ids_with_diff = res_clip[res_clip].index.tolist()
            clip_discrep_cnt = res_clip.sum()

            res_apn = by_apn.groupby('APN (PARCEL NUMBER UNFORMATTED)').apply(has_difference)
            apn_id_cnt = len(res_apn)
            apn_discrep_cnt = res_apn.sum()

            county_id_cnt[d_fips] = clip_id_cnt + apn_id_cnt
            county_discrep_cnt[d_fips] = clip_discrep_cnt + apn_discrep_cnt

            print(county_discrep_cnt[d_fips] / county_id_cnt[d_fips])

            if check_amt:
                ctr -= 1
                if ctr == 0:
                    break

        return  # I figure with result from one county is enough, discrepency rate is about 80%


        # want to check the number of FIPS that has discrepancy count > 0
        # and total rate of discrepancy (and maybe the highest rate among counties)

        # Step 1: Count FIPS with discrepancy > 0
        num_fips_with_disc = sum(1 for fips, disc in county_discrep_cnt.items() if disc > 0)

        # Step 2: Calculate overall discrepant ratio
        total_disc = sum(county_discrep_cnt.values())
        total_fips = sum(county_id_cnt.values())
        overall_disc_ratio = total_disc / total_fips if total_fips > 0 else 0

        # Step 3: Find FIPS with top 3 discrepant ratio
        male_ratios = {
            fips: county_discrep_cnt[fips] / county_id_cnt[fips]
            for fips in county_id_cnt if county_id_cnt[fips] > 0
        }

        top_3_fips = sorted(male_ratios.items(), key=lambda x: x[1], reverse=True)[:3]

        # Output results
        print("Number of FIPS with men > 0:", num_fips_with_disc)
        print("Overall male ratio:", round(overall_disc_ratio, 1))
        print("Top 3 FIPS with highest male ratio:")
        for fips, ratio in top_3_fips:
            print(f"FIPS: {fips}, Ratio: {round(ratio, 2)}")

    def tax_year_column(self, checking_what: str, tax_spec: dict, fips_to_check: list = []):
        '''
        This function only checks the historical tax data for whether the
        columns "TAX YEAR" and "ASSESSED YEAR" are always the same (if both
        have value), also want to check if they together 100% cover the rows.
        Notice that the missing percentage is only for checking hist_tax files.

        Parameters
        ----------
        checking_what: str.
            only available for tax or hist_tax.
        tax_spec: dict.
            the specified columns and corresponding type.
        fips_to_check: list.
            list of fips wish to check.

        Return
        ------
            None. Results would be printed on the terminal.
        '''
        if not fips_to_check:
            print("No FIPS provided, ending...")
            return

        if checking_what not in ['tax', 'hist_tax']:
            print("Data not supported, ending...")
            return

        hist_tax_files = []

        for fname in os.listdir(self.data_paths['hist_tax']):
            if fname[:4] != "FIPS":
                continue
            hist_tax_files.append(fname)

        for fips in fips_to_check:
            print(f"Currently checking FIPS: {fips}")
            if checking_what == 'hist_tax':
                cur_hist_tax_files = [f for f in hist_tax_files if fips in f]
                cur_path = self.data_paths['hist_tax']
            elif checking_what == 'tax':
                cur_hist_tax_files = [self.util.tax_file_fullname(fips)]
                cur_path = self.data_paths['tax']

            for cur_fname in cur_hist_tax_files:
                serial_num = self.util.get_serial_num(cur_fname)

                fpath = os.path.join(cur_path, cur_fname)
                cur_file = pd.read_csv(
                    fpath, delimiter="|",
                    usecols=tax_spec.keys(), dtype=tax_spec,
                    on_bad_lines='skip', quoting=csv.QUOTE_NONE
                )

                # 1. check if the two columns are always the same (if both have value) => 100%
                check1 = cur_file.dropna()

                matching_percentage = (check1['ASSESSED YEAR'] == check1['TAX YEAR']).mean() * 100
                print(f"Percentage of matching years: {matching_percentage:.2f}%")

                # 2. check the min and max year in each file.
                print("Min and max assessed year: ", end='')
                print(min(cur_file['ASSESSED YEAR']), max(cur_file['ASSESSED YEAR']))
                print("Min and max tax year: ", end='')
                print(min(cur_file['TAX YEAR']), max(cur_file['TAX YEAR']))

                # 3. check if they together fully cover the rows => result shows almost 100% (missing -> 0%)
                cur_file['YEAR'] = cur_file['ASSESSED YEAR'].fillna(cur_file['TAX YEAR'])

                missing_pct = cur_file['YEAR'].isna().mean() * 100
                print(f"  {serial_num} Year missing percentage: {missing_pct:.5f}%")

    def clip_apn_bijection(self, fips_to_test=[]):
        '''
        This checks if the CLIP and APN in Deed files are bijection for given
        FIPS.
        Result: yes.

        Parameters
        ----------
        fips_to_test: list.
            the list of FIPS wish to test.

        Return
        ------
            None. Prints the result to terminal.
        '''
        deed_spec = {
            'CLIP': 'str', # 1006407533
            'APN (PARCEL NUMBER UNFORMATTED)': 'str'
        }

        for fips in fips_to_test:
            print(f"Currently testing {fips}")

            fname = self.util.deed_file_fullname(fips)
            fpath = os.path.join(self.data_paths['deed'], fname)

            cur_file = pd.read_csv(
                fpath, delimiter="|", usecols=deed_spec.keys(), dtype=deed_spec,
                on_bad_lines='skip', quoting=csv.QUOTE_NONE
            )
            # Drop the missing in either columns
            cur_file = cur_file.dropna()

            # Check Injectivity: No duplicates in A -> B mapping
            is_injective = cur_file.groupby('CLIP')['APN (PARCEL NUMBER UNFORMATTED)'].nunique().max() == 1

            # Check Surjectivity: No duplicates in B -> A mapping
            is_surjective = cur_file.groupby('APN (PARCEL NUMBER UNFORMATTED)')['CLIP'].nunique().max() == 1

            # Check if bijection (both injective and surjective)
            is_bijection = is_injective and is_surjective

            # Output results
            print("Is Injective (One-to-One)?", is_injective)
            print("Is Surjective (Onto)?", is_surjective)
            print("Is Bijection?", is_bijection)

    def bulkcheck_assessed_year_range(self, checking_what: str, data_spec: dict):
        '''
        This function checks the assessed year range for tax data.
        Result: 2021~2023
        TODO: This might could also work for other numeric type data, just to
              specify which column to check.

        Parameters
        ----------
        checking_what: str.
            could be either deed, tax, or hist_tax
        checking_item: str.
            currently support year_range.
        data_spec: dict.
            to specify what columns to use and data type for each column.

        Return
        ------
            None. Print result to the terminal.
        '''
        cur_path = self.data_paths[checking_what]
        cur_min, cur_max = float('inf'), float('-inf')

        for fname in tqdm(os.listdir(cur_path), desc="Bulk checking year range"):
            if fname[:4] != "FIPS":
                continue

            fpath = os.path.join(cur_path, fname)
            cur_file = pd.read_csv(
                fpath, delimiter="|", usecols=data_spec.keys(), dtype=data_spec,
                on_bad_lines='skip', quoting=csv.QUOTE_NONE
            )
            cur_file['ASSESSED YEAR'] = cur_file['ASSESSED YEAR'].astype(float)
            cur_file['TAX YEAR'] = cur_file['TAX YEAR'].astype(float)

            cur_min = min(min(cur_file['ASSESSED YEAR']), min(cur_file['TAX YEAR']), cur_min)
            cur_max = max(max(cur_file['ASSESSED YEAR']), max(cur_file['TAX YEAR']), cur_max)

        print(f"The assessed year range in {checking_what} is {cur_min}~{cur_max}.")

class Preprocess():
    def __init__(
            self, deed_path, tax_path, hist_tax_path, log_name="",
            log_path="log/", type_spec_d = {}, type_spec_t = {}
        ) -> None:
        self.__log_file = os.path.join(log_path, log_name)

        # For tax and historical tax, we use the same columns
        self.__type_spec_deed = type_spec_d
        self.__type_spec_tax = type_spec_t
        self.data_paths = {
            "deed": deed_path,
            "tax": tax_path,
            "hist_tax": hist_tax_path
        }
        self.util = Utils()

        if not os.path.exists(log_path):
            os.makedirs(log_path)

        if log_name:
            if not os.path.exists(self.__log_file):
                with open(self.__log_file, 'w') as f:
                    pass  # Just create an empty file

    def write_log(self, message):
        """Writes message to the log file."""
        with open(self.__log_file, 'a') as f:
            f.write(f"{message}\n")

    def __deed_filter(self, data) -> pd.DataFrame:
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
        # Ignore the rows without valid date data
        data['SALE_DATE'] = data['SALE_DERIVED_DATE']\
            .where(data['SALE_DERIVED_DATE'] != '0', data['SALE_DERIVED_RECORDING_DATE'])
        # drop dates being NA or '0'
        data = data.dropna(subset=['SALE_DATE'])
        data = data[data['SALE_DATE'] != '0']

        data = data.drop(columns=['SALE_DERIVED_DATE', 'SALE_DERIVED_RECORDING_DATE'])

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

        # Drop columns who had done their job
        to_drop = [
            'INTERFAMILY_RELATED_IND', 'RESALE_IND', 'NEW_CONSTRUCTION_IND',
            'SHORT_SALE_IND', 'FORECLOSURE_REO_IND', 'FORECLOSURE_REO_SALE_IND'
        ]
        data = data.drop(columns=to_drop)

        return data

    def single_deed_clean(self, fpath: str) -> pd.DataFrame:
        '''
        For each fips file, this function first do the column name cleaning,
        and then apply the __deed_filter() to the data.
        Notice that we read each file in chunks, because some of the files
        are over 1GB and up to 6GB, processing the whole file at once might
        be an overly intense task for memory.
        '''
        data_type_spec = self.__type_spec_deed
        to_cat = []
        for chunk in pd.read_csv(
            fpath, delimiter="|",
            chunksize=CHUNK_SIZE, # can only work with engine = c or python
            usecols=data_type_spec.keys(), dtype=data_type_spec,
            on_bad_lines='skip', # skip the bad rows for now, guess there won't be many (cannot check tho)
            # engine='c',
            quoting=csv.QUOTE_NONE
        ):
            # Make column names more readable
            old_col = chunk.columns
            new_col = [
                s.replace(r' - STATIC','').replace('INDICATOR','IND')
                for s in old_col
            ]
            new_col = ['_'.join(s.split()) for s in new_col]
            chunk = chunk.rename(columns=dict(zip(old_col, new_col)))
            chunk.rename(columns={"APN_(PARCEL_NUMBER_UNFORMATTED)": 'APN'}, inplace=True)

            data = self.__deed_filter(chunk)

            to_cat.append(data)

        cleaned_deed = pd.concat(to_cat, ignore_index=True)

        # combine APN and its seq. number to make new APN (not sure if needed tho)
        cleaned_deed['APN'] = cleaned_deed['APN'] + cleaned_deed['APN_SEQUENCE_NUMBER']
        cleaned_deed = cleaned_deed.drop(['APN_SEQUENCE_NUMBER'], axis=1)

        # the year column for match closest assess year in tax files; sale_date eg. 20210908
        cleaned_deed = cleaned_deed.dropna(subset=['SALE_DATE'])
        cleaned_deed['SALE_YEAR'] = cleaned_deed['SALE_DATE'].str[:4]
        cleaned_deed['SALE_YEAR'] = cleaned_deed['SALE_YEAR'].astype(int)

        # Those has only APN need to be filled with CLIP, using mapping from APN to CLIP.
        # since CLIP and APN are bijective, we can use transform('first') to get
        # the first non-null value of CLIP for each APN group
        cleaned_deed['CLIP'] = cleaned_deed['CLIP'].fillna(cleaned_deed.groupby('APN')['CLIP'].transform('first'))

        # give each record a unique id, for later process after merging with tax
        cleaned_deed['uid'] = cleaned_deed.index

        return cleaned_deed

    def single_tax_clean(self, fpath: str) -> pd.DataFrame:
        '''
        This function filters only columns but not rows. We only need the basic
        information of the properties, such as number of bedrooms and living
        area square feet.
        '''
        data_type_spec = self.__type_spec_tax
        to_cat = []
        for chunk in pd.read_csv(
            fpath, delimiter="|",
            chunksize=CHUNK_SIZE,
            on_bad_lines='skip',
            usecols=data_type_spec.keys(), dtype=data_type_spec,
            quoting=csv.QUOTE_NONE
        ):
            old_col = chunk.columns
            new_col = [
                s.replace(' -','').replace('SQUARE FEET', 'SQ FT')
                for s in old_col]
            new_col = ['_'.join(s.split()) for s in new_col]
            chunk.rename(columns=dict(zip(old_col, new_col)), inplace=True)
            chunk.rename(columns={"APN_(PARCEL_NUMBER_UNFORMATTED)": 'APN'}, inplace=True)

            to_cat.append(chunk)

        cleaned_tax = pd.concat(to_cat, ignore_index=True)

        # combine APN and its seq. number to make new APN (not sure if needed tho)
        cleaned_tax['APN'] = cleaned_tax['APN'] + cleaned_tax['APN_SEQUENCE_NUMBER']
        cleaned_tax = cleaned_tax.drop(['APN_SEQUENCE_NUMBER'], axis=1)

        # based on analysis, these two year column should be the same
        cleaned_tax['ASSESSED_YEAR'] = cleaned_tax['ASSESSED_YEAR'].fillna(cleaned_tax['TAX_YEAR'])
        # the assessed year range for Tax data is 2021~2023, so pick 2022 to fill na
        cleaned_tax['ASSESSED_YEAR'] = cleaned_tax['ASSESSED_YEAR'].fillna(2022)
        cleaned_tax['ASSESSED_YEAR'] = cleaned_tax['ASSESSED_YEAR'].astype(int)

        cleaned_tax['CLIP'] = cleaned_tax['CLIP'].fillna(cleaned_tax.groupby('APN')['CLIP'].transform('first'))
        cleaned_tax['APN'] = cleaned_tax['APN'].fillna(cleaned_tax.groupby('CLIP')['APN'].transform('first'))

        return cleaned_tax

    def hist_tax_clean(self, folder: str, fips: str, file_catcher=[]):
        '''
        This function would read and clean the given FIPS files from the folder.
        Notice that there are multiple files for a given FIPS, so the way of
        cleaning is different from deed and tax.

        Parameters
        ----------
        folder: str.
            The folder where historical tax files at.
        fips: str.
            The current FIPS we wish to work with.
        file_catcher: list, optional.
            Since the folder has too many files, specifying a FIPS and check
            every file names in the folder would be inefficient. Thus, this
            catcher serves as the proxy folder to search FIPS files in, and
            remove the files from this list after cleaning.

        Return
        ------
            data: cleaned historical tax dataframe for merging with deed.
            new_catcher: an updated catcher for the next FIPS cleaning.
        '''
        fips_related_files = [fname for fname in file_catcher if fips in fname]
        new_catcher = [fname for fname in file_catcher if fips not in fname]

        # this step saves the checking step in the merging with deed part
        if not fips_related_files:
            return None, new_catcher

        to_cat = []

        for fname in fips_related_files:
            fpath = os.path.join(folder, fname)

            cur_file = self.single_tax_clean(fpath)

            # ===== For debug: checking if the max and min year are the same.
            # print(f"Working on {self.util.get_serial_num(fname)}")
            # temp = cur_file[cur_file['ASSESSED_YEAR'] < 9999]
            # print(min(cur_file['ASSESSED_YEAR']), max(temp['ASSESSED_YEAR']))
            # =====

            # in each hist tax files, all entries should have the same ASSESSED_YEAR
            # therefore we set the entire column to the same year value.
            # but in fact, some of the files have two years, especially before 2015 (or serial number 07)
            cur_file['ASSESSED_YEAR'] = min(cur_file['ASSESSED_YEAR'])

            to_cat.append(cur_file)

        stacked_files = pd.concat(to_cat, ignore_index=True)

        # repeat this process here to catch the cases that some records have
        # both CLIP and APN in some file but have only either one of them in
        # other files.
        stacked_files['CLIP'] = stacked_files['CLIP'].fillna(stacked_files.groupby('APN')['CLIP'].transform('first'))
        stacked_files['APN'] = stacked_files['APN'].fillna(stacked_files.groupby('CLIP')['APN'].transform('first'))

        return stacked_files, new_catcher

    def merge_n_save_by_fips(self, out_path, test_n_fips = 0):
        '''
        This function will read the deed and tax file, clean them, and then
        merge deed and tax.
        Notice that some of the records does not have CLIP, for those we use
        APN to merge. For records that has neither CLIP not APN, we ignore them.
        (only a few has no CLIP)

        Parameters
        ----------
        out_path: str.
            the folder path for merged files.

        Return
        ------
            merged deed and tax dataframe.
        '''
        # These are the fips I found in the data folder but not in the fips2county list (ignoring fips above 56XXX)
        updated_fips = {       # old -> new
            "02232": '02230',  # SKAGWAY (same name)
            "02261": '02063',  # VALDEX-CORDOVA -> CHUGACH
            "02280": '02195',  # WRANGELL-PETERSBURG -> PETERSBURG (no clear info about this change, so assign this to Petersburg based on higher population)
            "12025": '12086'   # DADE -> MIAMI-DADE
        }

        # this serves as a cache for hist tax file names, for there are too many files.
        remaining_hist_tax_files = []

        for fname in os.listdir(self.data_paths['hist_tax']):
            if fname[:4] != "FIPS":
                continue
            remaining_hist_tax_files.append(fname)

        # for test
        ctr = test_n_fips

        # ===== Start merging =====
        for deed_fname in tqdm(os.listdir(self.data_paths['deed']), desc="Merging files"):
            if deed_fname[:4] != "FIPS":
                continue

            cur_fips = deed_fname[5:10]

            # ======= Deed files =======
            deed_fpath = os.path.join(self.data_paths['deed'], deed_fname)
            deed_data = self.single_deed_clean(deed_fpath)

            # ======= Tax files =======
            # The idea is to stack tax with hist tax, so we need to check if
            # both files exist, and then do the stack, or just use either one
            # of them. If both not exist, then ignore this FIPS.
            tax_to_cat = []

            # Tax
            tax_fname = self.util.tax_file_fullname(cur_fips)
            tax_fpath = os.path.join(self.data_paths['tax'], tax_fname)

            # if the tax has this FIPS, merge it later
            if os.path.exists(tax_fpath):
                tax_data = self.single_tax_clean(tax_fpath)
                tax_to_cat.append(tax_data)

            # Hist Tax
            # "data" would return None if the hist tax does not have this FIPS
            hist_tax_data, remaining_hist_tax_files = self.hist_tax_clean(
                self.data_paths['hist_tax'], cur_fips,
                file_catcher=remaining_hist_tax_files
            )
            if hist_tax_data is not None:
                tax_to_cat.append(hist_tax_data)

            # if there are no corresponding tax or hist tax, ignore this FIPS
            if not tax_to_cat:
                self.write_log(f"{cur_fips} has neither tax not hist tax data.")
                continue

            complete_tax_data = pd.concat(tax_to_cat, ignore_index=True)

            # ======= Start merging files =======
            # Divide deed into: with CLIP, only APN, neither
            deed_clip, deed_apn, deed_remaining = self.util.split_clip_apn(deed_data, col_clip='CLIP', col_apn='APN')
            # notice that we've fill CLIP and APN with each other in tax data,
            # and there might still have only-CLIP and only-APN exist,
            # so the following two data might be overlapping
            tax_clip = complete_tax_data[complete_tax_data['CLIP'].notna()]
            tax_apn = complete_tax_data[complete_tax_data['APN'].notna()]

            # just checking
            non_mergable_cnt = deed_remaining.shape[0]
            ttl_row_cnt = deed_clip.shape[0] + deed_apn.shape[0] + non_mergable_cnt
            if non_mergable_cnt > 0:
                self.write_log(f"{cur_fips} deed has {non_mergable_cnt} rows unmergable out of {ttl_row_cnt} rows.")

            # Merge
            by_clip = deed_clip.merge(tax_clip, on='CLIP', how='left')
            by_apn = deed_apn.merge(tax_apn, on='APN', how='left')

            out_data = pd.concat([by_clip, by_apn], ignore_index=True)

            # Some of the deed records does not have correpsonding property
            # info, need to drop them.
            # -> since assessed year, in our tax file process, has guaranteed
            #    to have no missing, we drop those with missing here
            out_data = out_data.dropna(subset=['ASSESSED_YEAR'])

            # since there are multiple property records in the tax data, but
            # we need only the one that is closest to the transaction date.
            # so we keep the smallest year diff (in absolute value) record.
            out_data['year_diff'] = abs(out_data['SALE_YEAR'] - out_data['ASSESSED_YEAR'])
            out_data = out_data.loc[out_data.groupby('uid')['year_diff'].idxmin()]

            # if the FIPS is in the update list, change both the filename and
            # the record in the data.
            cur_fips = updated_fips.get(cur_fips, cur_fips)
            out_data['FIPS_CODE'] = cur_fips

            # final column clean
            out_data = out_data.drop(['year_diff'], axis=1)

            # save each processed FIPS to folder
            out_fpath = f"{out_path}FIPS_{cur_fips}_merged.txt"
            out_data.to_csv(out_fpath, index=False)

            # for debug
            ctr -= 1
            if ctr == 0:
                break
        return

        '''
        Index(['CLIP', 'LAND_USE_CODE', 'MOBILE_HOME_IND', 'PRIMARY_CATEGORY_CODE',
            'DEED_CATEGORY_TYPE_CODE', 'SALE_AMOUNT', 'BUYER_1_FULL_NAME',
            'SELLER_1_FULL_NAME', 'SALE_DATE', 'NEW_HOME_ORIG', 'SALE_YEAR', 'uid',
            'APN', 'TAX_YEAR', 'ASSESSED_YEAR', 'YEAR_BUILT',
            'BEDROOMS_ALL_BUILDINGS', 'TOTAL_ROOMS_ALL_BUILDINGS',
            'TOTAL_BATHROOMS_ALL_BUILDINGS', 'NUMBER_OF_BATHROOMS',
            'NUMBER_OF_UNITS', 'UNIVERSAL_BUILDING_SQ_FT', 'BUILDING_SQ_FT',
            'LIVING_SQ_FT_ALL_BUILDINGS', 'BUILDING_GROSS_SQ_FT',
            'ADJUSTED_GROSS_SQ_FT', 'year_diff']
        '''


def data_checking():
    '''
    This function does some basic checking of the downloaded data before
    preprocessing, items including
    * if FIPS are matching for deed and tax (with historical tax)
    * in historical tax, does each record have the same value
    '''
    ck = CheckData(
        deed_path=DEED_FOLDER,
        tax_path=TAX_FOLDER,
        hist_tax_path=HIST_TAX_FOLDER
    )

    # ck.fips_coverage(base_src='deed', cover_srcs=['tax', 'hist_tax'])

    tax_spec = {
        "CLIP": 'str',
        "APN (PARCEL NUMBER UNFORMATTED)": 'str',
        "UNIVERSAL BUILDING SQUARE FEET": 'float64'
    }
    # ck.property_info_consistency(tax_spec=tax_spec, check_amt=10)

    tax_spec = {
        "CLIP": 'str',
        "APN (PARCEL NUMBER UNFORMATTED)": 'str',
        "ASSESSED YEAR": 'float',
        "TAX YEAR": 'float'
    }
    # 06037, 04013, 17031
    # ck.tax_year_column(
    #     checking_what='tax', tax_spec=tax_spec, fips_to_check=['06037'])

    # ck.bulkcheck_assessed_year_range('tax', tax_spec)

    # ck.clip_apn_bijection(['06037'])

    print("Checking done!!!")


def data_processing(deed_spec, tax_spec):
    '''
    This is the function for cleaning and merging deed and tax data, notice
    that setting test_n_fips in the merging step to 0 will work on the whole
    files, other positive integers would work as testing the code.
    '''
    current_date = datetime.now().strftime("%y%m%d")
    log = f'{current_date}_data_process.log'

    p = Preprocess(
        deed_path=DEED_FOLDER, tax_path=TAX_FOLDER, hist_tax_path=HIST_TAX_FOLDER,
        log_name=log, type_spec_d=deed_spec, type_spec_t=tax_spec
    )

    p.merge_n_save_by_fips(CLEANED_PATH, test_n_fips=0)

    print("Process done!!")


def main():
    deed_spec = {
        'CLIP': 'str', # 1006407533
        # 'FIPS CODE': 'category',  # will read fips code from the file name
        "APN (PARCEL NUMBER UNFORMATTED)": 'str',  # some of the fips has no CLIP in certain years, need to use this to merge
        "APN SEQUENCE NUMBER": 'str',  # mostly 1
        'LAND USE CODE - STATIC': 'category',
        'MOBILE HOME INDICATOR': 'category',
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

    tax_spec = {
        "CLIP": 'str', # 1006407533
        # "FIPS CODE": 'category',
        "APN (PARCEL NUMBER UNFORMATTED)": 'str',  # some of the fips has no CLIP in certain years, need to use this to merge
        "APN SEQUENCE NUMBER": 'str',  # mostly 1
        "TAX YEAR": 'float',
        "ASSESSED YEAR": 'float',  # 2010
        "YEAR BUILT": 'float',  # 2002
        "BEDROOMS - ALL BUILDINGS": 'float64',
        "TOTAL ROOMS - ALL BUILDINGS": 'float64',
        "TOTAL BATHROOMS - ALL BUILDINGS": 'float64',
        "NUMBER OF BATHROOMS": 'float64',
        "NUMBER OF UNITS": 'float64',
        "UNIVERSAL BUILDING SQUARE FEET": 'float64',
        # in hist tax, there's no indicator code.
        # "UNIVERSAL BUILDING SQUARE FEET SOURCE INDICATOR CODE": 'category',  # A: Adjusted, B: Total, D: Ground floor, G: Gross, L: Living, M: Base/Main
        "BUILDING SQUARE FEET": 'float64',
        "LIVING SQUARE FEET - ALL BUILDINGS": 'float64',
        "BUILDING GROSS SQUARE FEET": 'float64',
        "ADJUSTED GROSS SQUARE FEET": 'float64',
    }

    # ===============
    #  Data Checking
    # ===============

    # data_checking()

    # =================
    #  Data Processing
    # =================
    data_processing(deed_spec, tax_spec)


if __name__ == "__main__":
    main()