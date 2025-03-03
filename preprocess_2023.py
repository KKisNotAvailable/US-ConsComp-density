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
import warnings
warnings.filterwarnings("ignore")

# FOR MAC
# virtual env: source env/bin/activate
# if no pip: curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
#      then: python3 get-pip.py

CUR_SYS  = platform.system()

ROOT_PATH = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
DATA_PATH = ROOT_PATH + "CoreLogic/"
CLEANED_PATH = ROOT_PATH + "Homebuilder/2023_files/cleaned_by_county/"

DEED_FOLDER = DATA_PATH + "deed_split_2023/"
TAX_FOLDER = DATA_PATH + "tax_split_2023/"
HIST_TAX_FOLDER = DATA_PATH + "hist_tax_split_2023/"


CHUNK_SIZE = 1000000 # this for Deed files is about 500MB


# NOTE:
#   The old deed code book still got useful info, such as the land use code

class Utils():
    def __init__(self):
        pass

    def split_clip_apn(self, data: pd.DataFrame, col_clip: str, col_apn: str):
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

    def property_info_consistency(self, with_tax: bool = True, tax_spec: dict = None):
        '''
        This function basically check the property info in historical tax for
        the same CLIP or APN. Optionally, this check could include the tax
        data.

        Parameters
        ----------
        with_tax: bool.
            if would like to include tax data when checking.

        Return
        ------
            None. Results will be printed.
        '''
        # 2. check if all the wanted columns share same values across different
        #    files for the same CLIP or APN, could also check with tax file
        #    in this step
        # TODO: This would be feeding a FIPS at a time, and then stack all FIPS files.
        # TODO: Need to distinguish with CLIPS and without CLIPS

        # loop through deed folder, for each fips, check if the hist_tax and tax file exist.
        # if yes, then read the files with the data spec and stack them up.
        # check if all rows are the same, if not, then mark as inconsistent.
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

        # store the file full names and extracted FIPS as seperate lists
        for src in fullnames.keys():
            for fname in os.listdir(self.data_paths[src]):
                if fname[:4] != "FIPS":
                    continue
                fullnames[src].append(fname)
                fips[src].append(fname[5:10])

        def has_difference(group):
            # group.nunique() returns the number of unique values in each column
            return (group.nunique() > 1).any()

        # start counting discrepant CLIP or APN for each county
        county_id_cnt, county_discrep_cnt = {}, {}

        for d_fips in fips['deed']:
            if d_fips not in fips['tax'] and d_fips not in fips['hist_tax']:
                print(f"FIPS {d_fips} does not have property information.")
                continue

            # if has corresponding FIPS, read all the files and stack them up
            hist_tax_files = [f for f in fullnames['hist_tax'] if d_fips in f]  # might have multiple
            tax_files = [f for f in fullnames['tax'] if d_fips in f] if with_tax else []

            to_cat = []

            for f in hist_tax_files:
                cur_file = pd.read_csv(
                    f"{self.data_paths['hist_tax']}{f}", delimiter="|",
                    usecols=tax_spec.keys(), dtype=tax_spec,
                    on_bad_lines='skip', quoting=csv.QUOTE_NONE
                )
                to_cat.append(cur_file)

            for f in tax_files:
                cur_file = pd.read_csv(
                    f"{self.data_paths['tax']}{f}", delimiter="|",
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
            clip_discrep_cnt = (res_clip == True).sum()

            res_apn = by_apn.groupby('APN (PARCEL NUMBER UNFORMATTED)').apply(has_difference)
            apn_id_cnt = len(res_apn)
            # ids_with_diff = res_apn[res_apn].index.tolist()
            apn_discrep_cnt = (res_apn == True).sum()

            county_id_cnt[d_fips] = clip_id_cnt + apn_id_cnt
            county_discrep_cnt[d_fips] = clip_discrep_cnt + apn_discrep_cnt

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


class Preprocess():
    def __init__(
            self, log_name: str = "", log_path: str = "log/", out_path: str = './output/',
            type_spec_d = {}, type_spec_t = {}
        ) -> None:
        self.__log_file = log_path + log_name
        self.__out_path = out_path

        # For tax and historical tax, we use the same columns
        self.__type_spec_deed = type_spec_d
        self.__type_spec_tax = type_spec_t

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
        # drop NA and '0' in the end
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

        # Drop columns who had done their responsibility
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
        new_catcher = file_catcher.copy()

        fips_related_files = [fname for fname in new_catcher if fips in fname]
        new_catcher = [fname for fname in new_catcher if fips not in fname]

        for fname in fips_related_files:
            fpath = folder + fname

    def merge_n_save_by_fips(self, deed_path, tax_path, out_path):
        '''
        This function will read the deed and tax file, clean them, and then
        merge deed and tax.
        Notice that some of the records does not have CLIP, for those we use
        APN to merge. For records that has neither CLIP not APN, we ignore them.
        (only a few has no CLIP)

        Parameters
        ----------
        deed_path: str.
            the folder path of deed files.
        tax_path: str.
            the folder path of tax files.
        out_path: str.
            the folder path for merged files.

        Return
        ------
            merged deed and tax dataframe.
        '''
        # These are the fips I found in the data folder but not in the fips2county list (ignoring fips above 56XXX)
        updated_fips = {  # old: new
            "02232": '02230',  # SKAGWAY (same name)
            "02261": '02063',  # VALDEX-CORDOVA -> CHUGACH
            "02280": '02195',  # WRANGELL-PETERSBURG -> PETERSBURG (no clear info about this change, so assign this to Petersburg based on higher population)
            "12025": '12086'   # DADE -> MIAMI-DADE
        }
        for deed_fname in tqdm(os.listdir(deed_path), desc="Merging files"):
            if deed_fname[:4] != "FIPS":
                continue

            cur_fips = deed_fname[5:10]
            deed_fpath = os.path.join(deed_path, deed_fname)
            deed_data = self.single_deed_clean(deed_fpath)

            # find the corresponding tax file
            # file_list = glob.glob(os.path.join(tax_path, f"FIPS_{cur_fips}*.txt"))

            # if file_list:
            #     tax_fpath = file_list[0]  # Select the first matching file

            tax_fname = f'FIPS_{cur_fips}_duke_university_property_basic2_dpc_01465909_20230803_072103_data.txt'
            tax_fpath = os.path.join(tax_path, tax_fname)

            # check if the fips is in tax_path
            if not os.path.exists(tax_fpath):
                self.write_log(
                    f"{cur_fips} has no tax file, deed size {os.path.getsize(deed_fpath) // (1024 * 1024)} MB."
                )
                continue

            tax_data = self.single_tax_clean(tax_fpath)

            # Divide both deed and tax into:
            # (1) with CLIP
            cond_deed_clip = deed_data['CLIP'].notna()
            deed_clip = deed_data[cond_deed_clip].drop(columns=['APN'])
            cond_tax_clip = tax_data['CLIP'].notna()
            tax_clip = tax_data[cond_tax_clip].drop(columns=['APN'])

            # (2) without CLIP (having NA values) but with APN (unformatted)
            cond_deed_apn = deed_data['APN'].notna()
            deed_apn = deed_data[(~cond_deed_clip) & cond_deed_apn].drop(columns=['CLIP'])
            cond_tax_apn = tax_data['APN'].notna()
            tax_apn = tax_data[(~cond_tax_clip) & cond_tax_apn].drop(columns=['CLIP'])

            # (3) with neither CLIP nor APN
            deed_remaining = deed_data[(~cond_deed_clip) & (~cond_deed_apn)]
            tax_remaining = tax_data[(~cond_tax_clip) & (~cond_tax_apn)]

            non_mergable_cnt = deed_remaining.shape[0]
            if non_mergable_cnt != 0:
                self.write_log(f"{cur_fips} deed has {non_mergable_cnt} rows unmergable.")

            # for 1, use CLIP to merge; for 2, use APN; and for 3, filter them out (but check how many of them)
            by_clip = deed_clip.merge(tax_clip, on='CLIP', how='left')
            by_apn = deed_apn.merge(tax_apn, on='APN', how='left')

            out_data = pd.concat([by_clip, by_apn], ignore_index=True)

            # if the FIPS is in the update list, change both the filename and
            # the record in the data.
            cur_fips = updated_fips.get(cur_fips, cur_fips)
            out_data['FIPS_CODE'] = cur_fips

            out_fpath = f"{out_path}FIPS_{cur_fips}_merged.txt"
            out_data.to_csv(out_fpath, index=False)

        return

    def _deed_check(self, cleaned_deed):
        '''
        This function is to do some simple analysis on the cleaned deed file.
        '''
        data = pd.read_csv(cleaned_deed)

        # the following columns need to be checked for dist. after filtering
        to_check = [
            'CLIP', 'LAND_USE_CODE', 'TRANSACTION_FIPS_CODE', 'SALE_AMOUNT'
        ]

        # 1. check non NA amount
        row_cnt = data['CLIP'].shape[0]
        good_counts = data[to_check].count()
        print("Non NA pct for ...")
        for i, c in enumerate(to_check):
            print(f"  {c:<11}: {good_counts.iloc[i]*100/row_cnt:>10.5f}%")

        # 2. exclude NA then check distribution (exclude SALE_AMOUNT)
        for c in to_check:
            if c != 'SALE_AMOUNT':
                print(f'Now checking {c}')
                print(data[c].value_counts()) # exclude NA automatically


def data_checking(tax_spec):
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

    ck.property_info_consistency(tax_spec=tax_spec)

    print("Checking done!!!")

def data_processing(deed_spec, tax_spec):
    '''
    '''
    current_date = datetime.now().strftime("%m%d")
    log = f'data_process.log'

    p = Preprocess(
        log_name=log, type_spec_d=deed_spec, type_spec_t=tax_spec
    )

    p.merge_n_save_by_fips(DEED_FOLDER, TAX_FOLDER, CLEANED_PATH)

    print("Process done!!")


def main():
    deed_spec = {
        'CLIP': 'str', # 1006407533
        'FIPS CODE': 'category',
        "APN (PARCEL NUMBER UNFORMATTED)": 'str',  # some of the fips has no CLIP in certain years, need to use this to merge
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
        "ASSESSED YEAR": 'category',  # 2010
        "YEAR BUILT": 'category',  # 2002
        "BEDROOMS - ALL BUILDINGS": 'float64',
        "TOTAL ROOMS - ALL BUILDINGS": 'float64',
        "TOTAL BATHROOMS - ALL BUILDINGS": 'float64',
        "NUMBER OF BATHROOMS": 'float64',
        "NUMBER OF UNITS": 'float64',
        "UNIVERSAL BUILDING SQUARE FEET": 'float64',
        # "UNIVERSAL BUILDING SQUARE FEET SOURCE INDICATOR CODE": 'category',  # A: Adjusted, B: Total, D: Ground floor, G: Gross, L: Living, M: Base/Main
        "BUILDING SQUARE FEET": 'float64',
        "LIVING SQUARE FEET - ALL BUILDINGS": 'float64',
        "BUILDING GROSS SQUARE FEET": 'float64',
        "ADJUSTED GROSS SQUARE FEET": 'float64',
    }

    # ===============
    #  Data Checking
    # ===============
    data_checking(tax_spec)

    # =================
    #  Data Processing
    # =================
    # data_processing(deed_spec, tax_spec)


if __name__ == "__main__":
    main()