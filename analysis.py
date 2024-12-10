import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import re
import os
import platform
from tqdm import tqdm
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, Normalize, BoundaryNorm
from matplotlib.cm import ScalarMappable
import numpy as np
from rapidfuzz import process, fuzz
import json
import warnings
warnings.filterwarnings("ignore")

CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
VAR_PATH = EXT_DISK + "Homebuilder/Variables/"
MAP_PATH = EXT_DISK + "Homebuilder/Maps/"
IMG_PATH = EXT_DISK + "Homebuilder/Figures/"
EXT_DISK += "Homebuilder/2016_files/"
COUNTY_FILE_PATH = EXT_DISK + "cleaned_by_county/"
STACKED_PATH = EXT_DISK + "processed_data/"

FIPS_MAP_FILE = '../NewCoreLogic_Codes/Data/fips2county.tsv.txt'

TXT_EXT = '.txt'
CSV_EXT = '.csv'

# setting image theme with seaborn
edge_color = "#30011E"
background_color = "#fafafa"

sns.set_style({
    "font.family": "serif",
    "figure.facecolor": background_color,
    "axes.facecolor": background_color,
})

# Some univeral string
COL_YR = 'YEAR'
COL_QTR = 'QUARTER'
COL_RESALE_NEW = 'RESALE/NEW_CONSTRUCTION'
COL_REGION = 'REGION' # TODO: need to make sure how they actually call this. West, North...
COL_UNIT_P = 'UNIT_PRICE'  # per square feet
COL_UNIT_P_MEAN = 'MEAN_UNIT_PRICE'
COL_UNIT_P_MDN = 'MEDIAN_UNIT_PRICE'
COL_SELLER = 'SELLER_NAME1'
COL_SALE_AMT = 'SALE_AMOUNT'
COL_CASE_CNT = 'CASE_CNT'
COL_RK_FLAG = 'RANK_FLAG'
COL_BLDG_AREA = 'UNIVERSAL_BUILDING_SQUARE_FEET'

'''NOTES
DEED COLUMNS:
        ['FIPS', 'PCL_ID_IRIS_FRMTD', 'BLOCK LEVEL LATITUDE',
       'BLOCK LEVEL LONGITUDE', 'SITUS_CITY', 'SITUS_STATE', 'SITUS_ZIP_CODE',
       'SELLER NAME1', 'SALE AMOUNT', 'SALE DATE', 'RECORDING DATE',
       'PROPERTY_INDICATOR', 'RESALE/NEW_CONSTRUCTION']

GEO COLUMNS:
        counties: ['STATEFP', 'COUNTYFP', 'COUNTYNS', 'AFFGEOID', 'GEOID', 'NAME', 'LSAD', 'ALAND',
       'AWATER', 'geometry']
       states: ['STATEFP', 'STATENS', 'AFFGEOID', 'GEOID', 'STUSPS', 'NAME', 'LSAD',
       'ALAND', 'AWATER', 'geometry']
'''

# TODO: write replication documentation? I'm currently having too many code.

class Analysis():
    def __init__(self, out_path: str, plot_path: str) -> None:
        self.__out_path = out_path
        self.__plot_path = plot_path
        self.threshold = 20
        self.sizex = 16
        self.sizey = 12

        if not os.path.isdir(out_path):
            os.mkdir(out_path)

    def deed_test_analysis(self):
        '''
        This program is checking
        1. if the cities in states are unique
        2. whether the cities in the geo data for plot is consistent with
           the cities we have

        STATEFP (2) + COUNTYFP (3) (from goe data) = county_fips (4 or 5) (from cities list)
        states like CA has STATEFP "06", the 5 digit version would ignore the first 0
        '''
        # put all the analysis or data for check generation here as functions
        # and since different functions might use the same data,
        # when first time read the data save it as class attribute, then we can save time on data reading.
        def counties_in_state(out=True):
            '''
            The counties are from geo data.
            '''
            # check the geo data
            counties = gpd.read_file("data/cb_2018_us_county_500k/")
            counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
            counties = counties[['STATEFP', 'COUNTYFP', 'COUNTYNS', 'NAME']]

            states = gpd.read_file("./data/cb_2018_us_state_500k/")
            states = states[~states.STATEFP.isin(["72", "69", "60", "66", "78"])]
            states = states[['STATEFP', 'STUSPS']]

            counties = pd.merge(counties, states, on='STATEFP', how='left')

            out_json = {}

            for s in sorted(set(counties['STUSPS'])):
                out_json[s] = sorted(
                    counties.loc[counties['STUSPS'] == s, 'NAME']
                )

            if out:
                with open(f'{self.__out_path}counties.json', 'w') as fp:
                    json.dump(
                        out_json, fp,
                        sort_keys=False, indent=4, separators=(',', ': ')
                    )
            else:
                print(counties.loc[counties['STUSPS'] == "CA", ['STATEFP', 'COUNTYFP', 'NAME']])

        def cities_in_state(out=True):
            '''
            The cities here are from our deed data.
            '''
            ddf = dd.read_csv('data/deed_stacked.csv')

            cols_to_clean = [
                "SITUS_CITY",
                "SITUS_STATE",
                "SALE AMOUNT",
                "SALE DATE"
            ]

            # 1. ignore rows if any of those columns is empty
            ddf = ddf.dropna(subset=cols_to_clean)

            # 2. keep only one data per state-city pair
            state_city_gp = ddf.groupby(['SITUS_STATE', 'SITUS_CITY']).size().reset_index()
            state_city_gp = state_city_gp.compute()

            # NOTE: 'ABERDEEN' has appeared in 6 states

            out_json = {}

            for s in sorted(set(state_city_gp['SITUS_STATE'])):
                out_json[s] = sorted(
                    state_city_gp.loc[state_city_gp['SITUS_STATE'] == s, 'SITUS_CITY']
                )

            if out:
                with open(f'{self.__out_path}cities.json', 'w') as fp:
                    json.dump(
                        out_json, fp,
                        sort_keys=False, indent=4, separators=(',', ': ')
                    )

            return

        def check_deed_var_distribution():
            ddf = dd.read_csv('data/deed_stacked.csv')

            ttl_rows = ddf['FIPS'].shape[0].compute() # but why len(ddf['FIPS']) won't work?

            # check if PCL_ID_IRIS_FRMTD is distinct (it is not, but should it be?)
            # unique_count = ddf['PCL_ID_IRIS_FRMTD'].nunique().compute()
            # print(f"PCL_ID_IRIS_FRMTD is{' not' if ttl_rows != unique_count else ''} unique key.")

            # check the nan distribution of the following columns
            cols_to_clean = [
                "FIPS",
                "SITUS_CITY",
                "SITUS_STATE",
                "SALE AMOUNT",
                "RECORDING DATE",
                "SALE DATE"
            ]
            # good_counts = ddf[cols_to_clean].count().compute()

            # print("Non NA pct for ...")
            # for i, c in enumerate(cols_to_clean):
            #     print(f"  {c:<11}: {good_counts.iloc[i]*100/ttl_rows:>10.5f}%")

            ddf = ddf[cols_to_clean]

            ddf['SALE DATE'] = ddf['SALE DATE'].mask(ddf['SALE DATE'] == 0, ddf['RECORDING DATE'])
            ddf['year'] = ddf['SALE DATE'] // 10000

            # res = ddf[ddf['year'] == 0].compute()

            # there are 1618508 rows with calculated year = 0???
            res = ddf['year'].value_counts().compute().sort_index()

            print(res)
            # print(res.head(10))

            # print(ddf['SALE DATE'].dtypes) # int64

        def check_city_list():
            # read the state-county-city data
            scc_map = pd.read_csv("data/simplemaps_uscities_basicv1.79/uscities.csv")
            scc_map = scc_map[['state_id', 'county_fips', 'zips']]
            print(scc_map.head(5))

        def check_abbreviation():
            states = gpd.read_file("data/cb_2018_us_state_500k/")
            # "Puerto Rico", "American Samoa", "United States Virgin Islands"
            # "Guam", "Commonwealth of the Northern Mariana Islands"
            states = states[states.STATEFP.isin(["72", "69", "60", "66", "78"])]

            print(states[['NAME', 'STATEFP', 'STUSPS']])

        def check_other_data():
            '''
            'CLIP', 'DEED_ID', 'AS_ASSESSED_YEAR', 'SALE_DATE', 'SALE_YEAR',
            'SALE_MONTH', 'SALE_AMOUNT', 'SELLER_NAME', 'BUYER_NAME', 'NEW_HOME',
            'YEAR_BUILT', 'EFFECTIVE_YEAR_BUILT', 'AS_EFFECTIVE_YEAR_BUILT',
            'VACANT_FLAG', 'SFH_IND', 'TOWNHOME_IND', 'CONDO_IND', 'PLEX_IND',
            'APARTMENT_IND', 'AS_LAND_SQUARE_FOOTAGE', 'AS_ACRES',
            'AS_BEDROOMS_-_ALL_BUILDINGS', 'AS_TOTAL_ROOMS_-_ALL_BUILDINGS',
            'AS_TOTAL_BATHROOMS_-_ALL_BUILDINGS', 'AS_NUMBER_OF_BATHROOMS',
            'AS_FULL_BATHS_-_ALL_BUILDINGS', 'AS_NUMBER_OF_PARKING_SPACES',
            'AS_STORIES_NUMBER', 'AS_NUMBER_OF_UNITS',
            'AS_UNIVERSAL_BUILDING_SQUARE_FEET', 'LAND_USE_CODE',
            'PROPERTY_IND_CODE', 'AS_LAND_USE_CODE', 'AS_PROPERTY_IND_CODE',
            'AS_BUILDING_STYLE_CODE', 'ADDRESS_MAIN', 'ADDRESS', 'CITY', 'STATE',
            'ZIP_CODE', 'AS_CENSUS_ID', 'AS_SUBDIVISION_NAME',
            'AS_SUBDIVISION_TRACT_NUMBER', 'AS_PARCEL_LEVEL_LAT',
            'AS_PARCEL_LEVEL_LON', 'MORTGAGE_PURCHASE_IND', 'CASH_PURCHASE_IND',
            'NOT_SFH', 'FIPS_CODE', 'TEMP_ID'
            '''
            ddf = dd.read_csv(
                '../from_dropbox/NewCoreLogic_Codes/Data/CoreLogic_Full_Post2000Sales.csv',
                dtype={'AS_SUBDIVISION_TRACT_NUMBER': 'object'}
            )

            out_peep = ddf.head(10)
            out_peep.to_csv(f'{self.__out_path}check_newCL.csv', index=False)

        check_other_data()

        return

    def __clean_firm_names(self, data):
        '''
        Current version does not include the info from compustat and top 200
        builders, now simply clean the seller names in the file to make similar
        names able to be grouped together.

        Note that this version is a bit different with the name analysis file.

        The high level idea is to first use fuzzy match to clean typo, then
        align words with abbrviations, and finally remove some abbrv and suffixes
        to match firm names.
        '''
        # Keep rows starts with an alphabet or a number, this also prevents code below from directly modify original data.
        data = data[data[COL_SELLER].str.match(r'^[A-Za-z0-9]', na=False)]

        # If leading is 0 and second is alphabet, replace 0 with O
        data[COL_SELLER] = data[COL_SELLER].map(
            lambda x: re.sub(r'^0([A-Za-z])', r'O\1', x)
        )

        # Some of the words have spaces, explicitly clean them
        data[COL_SELLER] = data[COL_SELLER].str.replace(r'\bHOME\s+BUILDER', 'HOMEBUILDER', regex=True)

        # "-" to spaces
        data[COL_SELLER] = data[COL_SELLER].str.replace('-', ' ', regex=False)

        # Apply fuzzy match on specific words for fixing 'er' or 's' in the end of words
        # TODO: currently including 'HOME' because it was matched to HOMEBUILDERS rather than HOMES
        abbrvs = {
            "LANE": "LN", "AVENUE": "AVE", "STREET": "ST", "ROAD": "RD",
            "DEVELOPER": "DEV", "DEVELOPMENT": "DEV", "REALTY": "RLTY",
            "ASSOCS": "ASSOC", "ASSOCIATES": "ASSOC", "HOMES": "HMS",
            "HOME": "HMS", "COMPANY": "CO", "GROUP": "GRP", "COPRORATION": "CORP",
            "LIMITED": "LTD", "CENTER": "CTR", "PARTNERSHIP": "PTSHP",
            "LIABILITY": "L", "HOUSING": "HSNG", "PACIFIC": "PAC",
            "HOLDINGS": "HLDNGS", "CONSTRUCTION": "CONST", "PROPERTIES": "PROP",
            "MANAGEMENT": "MGMT", "VENTURE": "VENTURES",
            "HOMEBUILDERS": "BUILDERS", "BUILDERS": "BUILDERS"
        }
        score_threshold = 90

        def fuzzy_match(word):
            # Fuzzy match each word against the list of correct words
            match, score, *_ = process.extractOne(word, abbrvs.keys())
            if score >= score_threshold:
                return match
            else:
                return word

        def fuzzy_correct(name: str):
            name = re.sub(r'[^A-Za-z0-9\s]', '', name)  # Keeps letter, numbers, spaces
            corrected_words = [fuzzy_match(word) for word in name.split()]
            return ' '.join(corrected_words)

        data[COL_SELLER] = data[COL_SELLER].map(fuzzy_correct)

        # [Extra step]: delete everything behind ' HOMES'
        # (this was by obervation, when firm names include HOMES, their names
        #  normally end with HOMES, things behind HOMES are just redundant)
        data[COL_SELLER] = data[COL_SELLER].str.replace(r'( HOMES) (.*)', r'\1', regex=True)

        # 2-3. replace with abbreviation, eg. "LANE" => "LN"
        def use_abbrv(name: str):
            '''This corrects words in single company name'''
            abbrved_words = [abbrvs.get(w, w) for w in name.split()]
            return ' '.join(abbrved_words)

        data[COL_SELLER] = data[COL_SELLER].map(use_abbrv)

        # [Extra step]: turn HMS back to HOMES (this was designed for matching with top200)
        data[COL_SELLER] = data[COL_SELLER].str.replace(' HMS', ' HOMES', regex=False)

        # 2-4. remove surfix and numbers at the end of name
        to_remove = [
            'LLC', 'LL', 'LLLP', 'INC', 'INS', 'IN', 'LTD', 'LP', 'L', 'P', 'C',
            'CO', 'AVE', 'PTSHP', "I", "II", "III", "IV", "V", "VI", "VII",
            "VIII", "IX", "X", "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII",
            "XVIII", "XIX", "XX", 'KTD', 'LC', 'CORP', 'LN', 'NC'
        ]
        # Create regex pattern that matches these suffixes at the end
        pattern = r'\b(?:' + '|'.join(to_remove) + r'|\d+)\b(?:\s+|$)$'

        def clean_suffixes(name):
            # Repeatedly remove suffixes until none are left at the end
            while re.search(pattern, name):
                name = re.sub(pattern, '', name).strip()  # Remove suffix and any trailing spaces
            return name

        data[COL_SELLER] = data[COL_SELLER].map(clean_suffixes)

        return data

    def __county_seller_file_clean(self, cur_fips, types_spec: dict):
        '''
        This is for generating data for Prof. to test plots. Therefore, this will
        aggregate to county-seller level, rather than county level.
        '''
        # the county file could include no new construction or even no data at all
        df = pd.read_csv(
            f"{COUNTY_FILE_PATH}{cur_fips}.csv",
            usecols=types_spec.keys(),
            dtype=types_spec
        )
        # in the loop calling this function, returned empty will be ignored.
        if df.empty: return df

        to_num = [
            "SALE_AMOUNT", "LAND_SQUARE_FOOTAGE", "UNIVERSAL_BUILDING_SQUARE_FEET",
            "BUILDING_SQUARE_FEET", "LIVING_SQUARE_FEET"
        ]
        for col in to_num:
            if col in types_spec.keys():
                # if not number, turn to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')


        # ====================
        #  General preprocess
        # ====================
        # 0. Include only the sellers of new construction.
        df = df[df[COL_RESALE_NEW] == 'N']

        if df.empty: return df  # some counties might include no new const.

        # 1. PROPERTY_INDICATOR: keep single family (10), condo (11), vacant (80),
        #                        duplex & tri... (21), apartment (22), misc (00)
        #    => since we've already filtered with land_use for broadly speaking residential use,
        #       we'll keep vacant and miscellaneous here.
        keep_list = ['10', '11', '80', '21', '22', '00']
        df = df[df['PROPERTY_INDICATOR'].isin(keep_list)]

        # 2. make year and quarter from SALE_DATE (str, eg. 20101231), drop those not numeric
        df = df[df['SALE_DATE'].str.isnumeric()].reset_index(drop=True)
        df[COL_YR] = df['SALE_DATE'].str[:4]  # still string
        # (month-1) // 3 + 1 = Q
        # df[COL_QTR] = (pd.to_numeric(df['SALE_DATE'].str[4:6]) - 1) // 3 + 1  # int

        # 3. calculate unit price
        df[COL_SALE_AMT] = df[COL_SALE_AMT] // 100 # the original data shows extra two 0s in the back.
        df[COL_UNIT_P] = df[COL_SALE_AMT] / df[COL_BLDG_AREA]

        # 4. process the seller names
        df = self.__clean_firm_names(df)

        # ===================
        #  Start aggregation
        # ===================
        key_set = [COL_YR, COL_SELLER]

        # 1. group by year and seller (discard OWNER RECORD), get
        #    (1) sum of sale_amt (2) count occurance => case_cnt (3) mean of unit price
        #    and sort by year (asc), case count (desc)
        grouped_new = df.groupby(key_set).agg(
            SALE_AMOUNT=(COL_SALE_AMT, 'sum'),
            CASE_CNT=(COL_SELLER, 'count')
        ).reset_index()

        # some records have empty building area, ignore them when getting avg
        df_unit_price = df[df[COL_BLDG_AREA] != 0]

        gped_new_unit_p = df_unit_price.groupby(key_set).agg(
            MEAN_UNIT_PRICE=(COL_UNIT_P, 'mean'),
            MEDIAN_UNIT_PRICE=(COL_UNIT_P, 'median')
        ).reset_index()

        grouped_new = grouped_new.merge(gped_new_unit_p, on=key_set, how='left')

        # Sort by year (ascending) and case_cnt (descending)
        grouped_new = grouped_new\
            .sort_values(by=[COL_YR, COL_CASE_CNT], ascending=[True, False])\
            .reset_index(drop=True)

        # 2. make ranking flag column
        def assign_rank_flags(group: pd.DataFrame):
            # Sort sellers by case_cnt and sale_amt in descending order
            group = group\
                .sort_values(by=[COL_CASE_CNT, COL_SALE_AMT], ascending=[False, False])\
                .reset_index(drop=True)

            # Calculate thresholds for top and bottom x%
            n, x_pct = len(group), 0.33
            top_threshold = int(n * x_pct)
            bot_threshold = n - top_threshold

            # Create a new column for rank flag
            group[COL_RK_FLAG] = ""
            group.loc[:top_threshold - 1, COL_RK_FLAG] = 'top'  # Top x%
            group.loc[bot_threshold:, COL_RK_FLAG] = 'bot'  # Bottom x%

            return group

        # In this dataframe, it is expected to have year sorted (asc), and each seller
        # is assigned 'top', '', or 'bot' at the rank_flag, given its ranking position
        # in each year.
        grouped_new = grouped_new.groupby(COL_YR, group_keys=False)\
            .apply(assign_rank_flags)\
            .reset_index(drop=True)

        tmp_cols = grouped_new.columns
        grouped_new['FIPS'] = cur_fips

        grouped_new = grouped_new[['FIPS'] + list(tmp_cols)]

        return grouped_new

    def corelogic_seller_panel_prep(self, folder_path, out_fname='corelogic_seller_panel.csv'):
        '''
        This is for generating data for Prof. to test plots. Therefore, this will
        aggregate to county-seller level, rather than county level.
        '''
        # here list all available columns and commented out the unused ones.
        types_spec = {
            "SELLER_NAME1": 'str',
            "SALE_DATE": 'str',
            "SALE_AMOUNT": 'str',
            "PROPERTY_INDICATOR": 'category',
            "RESALE/NEW_CONSTRUCTION": 'category', # M: re-sale, N: new construction
            "UNIVERSAL_BUILDING_SQUARE_FEET": 'str'
        }

        to_cat = []
        # loop through the folder
        for fname in tqdm(os.listdir(folder_path), desc=f"Processing FIPS files"):
            if not fname.endswith(CSV_EXT): continue

            fips = fname.replace(CSV_EXT, "")

            # Rows will be each year, columns include:
            #   FIPS, YEAR,
            cur_df = self.__county_seller_file_clean(cur_fips=fips, types_spec=types_spec)

            # print(cur_df)
            # return
            if cur_df.empty: continue

            to_cat.append(cur_df)

        panel_df = pd.concat(to_cat, ignore_index=True)

        panel_df.to_csv(f"{self.__out_path}{out_fname}", index=False)

        return

    def __county_stackyear_clean(self, cur_fips, types_spec: dict):
        '''
        The resulting data for each county would be just a row, same columns
        across counties: FIPS, HHI, case_cnt
        '''
        # the county file could include no new construction or even no data at all
        df = pd.read_csv(
            f"{COUNTY_FILE_PATH}{cur_fips}.csv",
            usecols=types_spec.keys(),
            dtype=types_spec
        )
        # in the loop calling this function, returned empty will be ignored.
        if df.empty: return df

        # make numeric
        df[COL_SALE_AMT] = pd.to_numeric(df[COL_SALE_AMT], errors='coerce')

        # use only the new constructions
        df = df[df[COL_RESALE_NEW] == 'N']

        # keep type of houses we need
        keep_list = ['10', '11', '80', '21', '22', '00']
        df = df[df['PROPERTY_INDICATOR'].isin(keep_list)]

        # clean seller
        df = self.__clean_firm_names(df)

        # We are stacking yearly data, so only need to group by seller
        grouped_df = df.groupby(COL_SELLER).agg(
            SALE_AMOUNT=(COL_SALE_AMT, 'sum'),
            CASE_CNT=(COL_SELLER, 'count')
        ).reset_index()

        # 1. Get total case count (or simply df.shape[0] works)
        ttl_case_cnt = sum(grouped_df[COL_CASE_CNT])

        # 2. HHI = sum((x_i/X)^2), notice the base is top 50
        TOP_N = 50
        hhi_prep = grouped_df[COL_SALE_AMT].nlargest(TOP_N) / \
            grouped_df[COL_SALE_AMT].sum()

        data = {
            "FIPS": cur_fips,
            "HHI": sum(hhi_prep ** 2),
            COL_CASE_CNT: ttl_case_cnt
        }

        return pd.DataFrame([data])

    def corelogic_stack_year_prep(self, folder_path, out_fname='for_heat_map.csv'):
        '''
        This data is basically solely for the use of plotting heat map for counties.
        Since all other plots do not require aggregation on the years.
        '''
        types_spec = {
            "SELLER_NAME1": 'str',
            "SALE_AMOUNT": 'str',
            "PROPERTY_INDICATOR": 'category',
            "RESALE/NEW_CONSTRUCTION": 'category' # M: re-sale, N: new construction
        }

        to_cat = []
        # loop through the folder
        for fname in tqdm(os.listdir(folder_path), desc=f"Processing FIPS files"):
            if not fname.endswith(CSV_EXT): continue

            fips = fname.replace(CSV_EXT, "")

            # Rows will be each year, columns include:
            #   FIPS, HHI, CASE_CNT
            cur_df = self.__county_stackyear_clean(cur_fips=fips, types_spec=types_spec)

            # print(cur_df)
            # return
            if cur_df.empty: continue

            to_cat.append(cur_df)

        panel_df = pd.concat(to_cat, ignore_index=True)

        panel_df.to_csv(f"{self.__out_path}{out_fname}", index=False)

        return

    def __county_file_clean(self, cur_fips, types_spec: dict):
        # the county file could include no new construction or even no data at all
        df = pd.read_csv(
            f"{COUNTY_FILE_PATH}{cur_fips}.csv",
            usecols=types_spec.keys(),
            dtype=types_spec
        )
        # in the loop calling this function, returned empty will be ignored.
        if df.empty: return df

        to_num = [
            "SALE_AMOUNT", "LAND_SQUARE_FOOTAGE", "UNIVERSAL_BUILDING_SQUARE_FEET",
            "BUILDING_SQUARE_FEET", "LIVING_SQUARE_FEET"
        ]
        for col in to_num:
            if col in types_spec.keys():
                # if not number, turn to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')


        # ====================
        #  General preprocess
        # ====================
        # 1. PROPERTY_INDICATOR: keep single family (10), condo (11), vacant (80),
        #                        duplex & tri... (21), apartment (22), misc (00)
        #    => since we've already filtered with land_use for broadly speaking residential use,
        #       we'll keep vacant and miscellaneous here.
        keep_list = ['10', '11', '80', '21', '22', '00']
        df = df[df['PROPERTY_INDICATOR'].isin(keep_list)]

        # 2. make year and quarter from SALE_DATE (str, eg. 20101231), drop those not numeric
        df = df[df['SALE_DATE'].str.isnumeric()].reset_index(drop=True)
        df[COL_YR] = df['SALE_DATE'].str[:4]  # still string
        # (month-1) // 3 + 1 = Q
        # df[COL_QTR] = (pd.to_numeric(df['SALE_DATE'].str[4:6]) - 1) // 3 + 1  # int

        # 3. calculate unit price
        df[COL_SALE_AMT] = df[COL_SALE_AMT] // 100 # the original data shows extra two 0s in the back.
        df[COL_UNIT_P] = df[COL_SALE_AMT] / df[COL_BLDG_AREA]

        # 4. process the seller names
        df = self.__clean_firm_names(df)


        # ========
        #  Resale
        # ========
        df_resale = df[df[COL_RESALE_NEW] != 'N']

        grouped_resale = df_resale.groupby(COL_YR).agg(
            RESALE_SALE_AMOUNT=(COL_SALE_AMT, 'sum'),
            RESALE_CASE_CNT=(COL_SELLER, 'count')
        ).reset_index()

        no_new_out_df = grouped_resale.copy()
        no_new_out_df['FIPS'] = cur_fips


        # ==================
        #  New construction
        # ==================
        df_new = df[df[COL_RESALE_NEW] == 'N']

        # if there are no entry of new constructions, return just the aggregated
        # sale_amt and case_cnt of resale
        if df_new.empty: return no_new_out_df

        key_set = [COL_YR, COL_SELLER]

        # 1. group by year and seller (discard OWNER RECORD), get
        #    (1) sum of sale_amt (2) count occurance => case_cnt (3) mean of unit price
        #    and sort by year (asc), case count (desc)
        grouped_new = df_new.groupby(key_set).agg(
            SALE_AMOUNT=(COL_SALE_AMT, 'sum'),
            CASE_CNT=(COL_SELLER, 'count')
        ).reset_index()

        # some records have empty building area, ignore them when getting avg
        df_new_unit_price = df_new[df_new[COL_BLDG_AREA] != 0]

        # NOTE: I think for each seller, we don't need to get their median unit price
        gped_new_unit_p = df_new_unit_price.groupby(key_set).agg(
            MEAN_UNIT_PRICE=(COL_UNIT_P, 'mean')
        ).reset_index()

        grouped_new = grouped_new.merge(gped_new_unit_p, on=key_set, how='left')

        # Sort by year (ascending) and case_cnt (descending)
        grouped_new = grouped_new\
            .sort_values(by=[COL_YR, COL_CASE_CNT], ascending=[True, False])\
            .reset_index(drop=True)

        # 2. make ranking flag column
        def assign_rank_flags(group: pd.DataFrame):
            # Sort sellers by case_cnt and sale_amt in descending order
            group = group\
                .sort_values(by=[COL_CASE_CNT, COL_SALE_AMT], ascending=[False, False])\
                .reset_index(drop=True)

            # Calculate thresholds for top and bottom x%
            n, x_pct = len(group), 0.2
            top_threshold = int(n * x_pct)
            bot_threshold = n - top_threshold

            # Create a new column for rank flag
            group[COL_RK_FLAG] = ""
            group.loc[:top_threshold - 1, COL_RK_FLAG] = 'top'  # Top x%
            group.loc[bot_threshold:, COL_RK_FLAG] = 'bot'  # Bottom x%

            return group

        # In this dataframe, it is expected to have year sorted (asc), and each seller
        # is assigned 'top', '', or 'bot' at the rank_flag, given its ranking position
        # in each year.
        grouped_new = grouped_new.groupby(COL_YR, group_keys=False)\
            .apply(assign_rank_flags)\
            .reset_index(drop=True)

        # 3. make the aggregation columns
        def agg_data(sub_df: pd.DataFrame):
            '''
            For each sub_df, group by year and make four columns
            1. sum of sale_amt; 2. sum of case count; 3. mean of unit price
            4. the yoy of mean unit price (%)
            '''
            # The NaN's in the unit_price would be ignored when getting mean
            grouped = sub_df.groupby(COL_YR).agg(
                SALE_AMOUNT=(COL_SALE_AMT, 'sum'),
                CASE_CNT=(COL_CASE_CNT, 'sum'),
                MEAN_UNIT_PRICE=(COL_UNIT_P_MEAN, 'mean'),
                MEDIAN_UNIT_PRICE=(COL_UNIT_P_MEAN, 'median')
            ).reset_index()

            return grouped

        # [Whole county] YEAR, SALE_AMOUNT, CASE_CNT, MEAN_UNIT_PRICE, MEDIAN_UNIT_PRICE
        whole_county = agg_data(grouped_new)
        whole_county.columns = [f'COUNTY_{col}' if col != COL_YR else col for col in whole_county.columns]

        # [Ranking - top 1/3]
        top_ranking = agg_data(grouped_new[grouped_new[COL_RK_FLAG] == 'top'])
        top_ranking.columns = [f'TOP_{col}' if col != COL_YR else col for col in top_ranking.columns]

        # [Ranking - bot 1/3]
        bot_ranking = agg_data(grouped_new[grouped_new[COL_RK_FLAG] == 'bot'])
        bot_ranking.columns = [f'BOT_{col}' if col != COL_YR else col for col in bot_ranking.columns]

        # HHI = sum((x_i/X)^2), notice the base is top 50; YEAR, HHI
        TOP_N = 50
        hhi_per_year = grouped_new.groupby(COL_YR)[COL_SALE_AMT].apply(
            lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
        ).rename('HHI').reset_index()

        # ===============
        #  Out dataframe
        # ===============
        # 1. create the base, TODO: how to include Q into the out_df base?
        # (opt.) join with fips2county to get state abbrv and county name
        # (opt.) use state abbrv to get region (not yet get the data)
        years = sorted(set(df[COL_YR]))

        out_df = pd.DataFrame({
            'FIPS': [cur_fips] * len(years),
            COL_YR: years
        })

        for cur_df in [whole_county, top_ranking, bot_ranking, grouped_resale, hhi_per_year]:
            out_df = out_df.merge(cur_df, on=COL_YR, how='left')

        # make yoy after ensuring all years are present
        for lvl in ['COUNTY', 'TOP', 'BOT']:
            out_df[f'{lvl}_YOY_{COL_UNIT_P_MEAN}'] = out_df[f'{lvl}_{COL_UNIT_P_MEAN}'].pct_change() * 100
            out_df[f'{lvl}_YOY_{COL_UNIT_P_MDN}'] = out_df[f'{lvl}_{COL_UNIT_P_MDN}'].pct_change() * 100

        out_df['TOP_MEAN_PRICE_DIFF'] = out_df[f'TOP_{COL_UNIT_P_MEAN}'] - out_df[f'COUNTY_{COL_UNIT_P_MEAN}']
        out_df['BOT_MEAN_PRICE_DIFF'] = out_df[f'BOT_{COL_UNIT_P_MEAN}'] - out_df[f'COUNTY_{COL_UNIT_P_MEAN}']
        out_df['TOP_MEDIAN_PRICE_DIFF'] = out_df[f'TOP_{COL_UNIT_P_MDN}'] - out_df[f'COUNTY_{COL_UNIT_P_MDN}']
        out_df['BOT_MEDIAN_PRICE_DIFF'] = out_df[f'BOT_{COL_UNIT_P_MDN}'] - out_df[f'COUNTY_{COL_UNIT_P_MDN}']

        return out_df

    def corelogic_panel_prep(self, folder_path, out_fname='corelogic_panel.csv'):
        '''
        This function will loop through every fips file in the folder and collect
        needed info one by one, finally aggregate them and save for later analysis.
        '''
        # here list all available columns and commented out the unused ones.
        types_spec = {
            # "FIPS": 'category',
            # "APN_UNFORMATTED": 'str',
            # "BATCH-ID": 'str', # in the form of yyyymmdd, but not the same as sale date
            # "BATCH-SEQ": 'str',
            "SELLER_NAME1": 'str',
            "SALE_DATE": 'str',
            "SALE_AMOUNT": 'str',
            # "LAND_USE": 'category',
            "PROPERTY_INDICATOR": 'category',
            "RESALE/NEW_CONSTRUCTION": 'category', # M: re-sale, N: new construction
            # "BUYER_NAME_1": 'str',
            # "LAND_SQUARE_FOOTAGE": 'str',
            "UNIVERSAL_BUILDING_SQUARE_FEET": 'str',
            # "BUILDING_SQUARE_FEET_INDICATOR_CODE": 'category', # A: adjusted; B: building; G: gross; H: heated; L: living; M: main; R: ground floor
            # "BUILDING_SQUARE_FEET": 'str', # doesn't differentiate living and non-living (if lack indicator code)
            # "LIVING_SQUARE_FEET": 'str',
        }

        to_cat = []
        # loop through the folder
        for fname in tqdm(os.listdir(folder_path), desc=f"Processing FIPS files"):
            if not fname.endswith(CSV_EXT): continue

            fips = fname.replace(CSV_EXT, "")

            # Rows will be each year, columns include:
            #   FIPS, YEAR, COUNTY_{SALE_AMOUNT, CASE_CNT, UNIT_PRICE, YOY_UNIT_PRICE},
            #   TOP_{SALE_AMOUNT, CASE_CNT, UNIT_PRICE, YOY_UNIT_PRICE},
            #   BOT_{SALE_AMOUNT, CASE_CNT, UNIT_PRICE, YOY_UNIT_PRICE},
            #   RESALE_SALE_AMOUNT, RESALE_CASE_ENT, TOP_P_DIFF, BOT_P_DIFF
            cur_df = self.__county_file_clean(cur_fips=fips, types_spec=types_spec)

            # print(cur_df.columns)
            # return
            if cur_df.empty: continue

            to_cat.append(cur_df)

        panel_df = pd.concat(to_cat, ignore_index=True)

        panel_df.to_csv(f"{self.__out_path}{out_fname}", index=False)

        return

    def make_full_panel(self, base_panel, out_fname='homebuilder_panel.csv'):
        '''
        Currently don't want to include the feature of adding variables, will
        default include all.

        Parameters
        ----------
        base_panel: str.
            the corelogic panel.
        '''

        type_spec = {
            'FIPS': 'str'
        }
        panel_df = pd.read_csv(self.__out_path+base_panel, dtype=type_spec)
        panel_df = panel_df.rename(columns={"YEAR": 'Year'})
        panel_df['StateFIPS'] = panel_df['FIPS'].str[:2]

        # >> Vacancy starts from 2005, and is state level: StateFIPS,Year,Vacancy_rate
        va_data = pd.read_csv(f"{VAR_PATH}vacancy_panel.csv", dtype={"StateFIPS": 'category'})
        panel_df = panel_df.merge(va_data, on=['StateFIPS', 'Year'], how='left')

        # >> WRLURI only has 2006: PLACEFP,WRLURI,STATE,FIPS
        wi_data = pd.read_csv(f"{VAR_PATH}WRLURI_2006.csv", dtype={"FIPS": 'category'})
        wi_data = wi_data[['FIPS', 'WRLURI']]
        wi_data = wi_data.dropna()
        # drop rows with duplicates in FIPS, keeping the row with the max value in WRLURI
        wi_data = wi_data.loc[wi_data.groupby('FIPS', observed=True)['WRLURI'].idxmax()]

        panel_df = panel_df.merge(wi_data, on='FIPS', how='left')

        # >> Unemployment: FIPS,Year,Unemployment
        unemp_data = pd.read_csv(f"{VAR_PATH}unemployment_panel.csv", dtype={"FIPS": 'category'})
        unemp_data['Unemployment'] = unemp_data['Unemployment'].replace('-', '0').astype(int)
        panel_df = panel_df.merge(unemp_data, on=['FIPS', 'Year'], how='left')

        # >> Unemployment_rate: FIPS,Year,Unemployment_rate
        unemp_data = pd.read_csv(f"{VAR_PATH}unemployment_rate_panel.csv", dtype={"FIPS": 'category'})
        unemp_data['Unemployment_rate'] = unemp_data['Unemployment_rate'].replace('-', '0').astype(float)
        panel_df = panel_df.merge(unemp_data, on=['FIPS', 'Year'], how='left')

        # >> Population: FIPS,Year,Population
        pop_data = pd.read_csv(f"{VAR_PATH}pop_panel.csv", dtype={"FIPS": 'category'})
        pop_data = pop_data.sort_values(by=['FIPS', 'Year'])
        pop_data['Pop_yoy'] = pop_data.groupby('FIPS')['Population'].pct_change() * 100

        panel_df = panel_df.merge(pop_data, on=['FIPS', 'Year'], how='left')

        # >> Natural disaster: FIPS,year,Count
        nd_data = pd.read_csv(f"{VAR_PATH}natural_disaster_panel.csv", dtype={"FIPS": 'category'})
        nd_data = nd_data.rename(columns={"year": 'Year', "Count": 'disaster_cnt'})
        panel_df = panel_df.merge(nd_data, on=['FIPS', 'Year'], how='left')
        panel_df['disaster_cnt'] = panel_df['disaster_cnt'].fillna(0)

        # >> Median hh income: FIPS,Year,Median_HH_income
        mhhi_data = pd.read_csv(f"{VAR_PATH}med_hh_income_panel.csv", dtype={"FIPS": 'category'})
        panel_df = panel_df.merge(mhhi_data, on=['FIPS', 'Year'], how='left')

        # >> House stock: FIPS,Year,House_stock
        hs_data = pd.read_csv(f"{VAR_PATH}house_stock_panel.csv", dtype={"FIPS": 'category'})
        panel_df = panel_df.merge(hs_data, on=['FIPS', 'Year'], how='left')

        # >> State HPI (use not-seasonal adjusted, index_sa is empty for states)
        hs_data = pd.read_csv(f"{VAR_PATH}us_hpi.csv",
                    usecols=['hpi_flavor', 'level', 'place_id', 'yr', 'period', 'index_nsa'])

        fips_map = pd.read_csv(FIPS_MAP_FILE, delimiter='\t',
                    usecols=['StateAbbr', 'StateFIPS'], dtype={"StateFIPS": 'str'})
        fips_map = fips_map.drop_duplicates().reset_index(drop=True)

        # take the year end
        hs_data = hs_data[hs_data['hpi_flavor'] == 'purchase-only']
        hs_data = hs_data[hs_data['level'] == 'State']
        hs_data = hs_data[hs_data['period'] == 4]
        hs_data = hs_data.rename(
            columns={"yr": 'Year', "index_nsa": 'HPI', "place_id": 'StateAbbr'})

        hs_data = hs_data.merge(fips_map, on='StateAbbr', how='left')
        hs_data = hs_data[['Year', 'StateFIPS', 'HPI']]
        hs_data = hs_data.sort_values(by=['StateFIPS', 'Year'])

        hs_data['HPI_yoy'] = hs_data.groupby('StateFIPS')['HPI'].pct_change() * 100

        panel_df = panel_df.merge(hs_data, on=['StateFIPS', 'Year'], how='left')

        # ============================
        #  Other derivative variables
        # ============================
        # is consecutive year
        def check_consecutive(group):
            # Check if current and previous years both have non-null values
            is_consec = group['COUNTY_MEAN_UNIT_PRICE'].notna() & group['COUNTY_MEAN_UNIT_PRICE'].shift(1).notna()
            return is_consec.replace({True: 'Y', False: None})

        # Apply the function for each FIPS group
        panel_df['is_consec_year'] = panel_df.groupby('FIPS', group_keys=False).apply(check_consecutive)

        panel_df.to_csv(f"{self.__out_path}{out_fname}", index=False)

    def plot_hhi_heat_map(self, filename, is_hhi, save_fig=False):
        '''
        REF: https://dev.to/oscarleo/how-to-create-data-maps-of-the-united-states-with-matplotlib-p9i
        plot the non yearly result on maps
        1. map points?
        2. current only has abbrev. of states
        '''
        def translate_geometries(df, x, y, scale, rotate):
            df.loc[:, "geometry"] = df.geometry.translate(yoff=y, xoff=x)
            center = df.dissolve().centroid.iloc[0]
            df.loc[:, "geometry"] = df.geometry.scale(xfact=scale, yfact=scale, origin=center)
            df.loc[:, "geometry"] = df.geometry.rotate(rotate, origin=center)
            return df

        def adjust_maps(df):
            df_main_land = df[~df.STATEFP.isin(["02", "15"])]
            df_alaska = df[df.STATEFP == "02"]
            df_hawaii = df[df.STATEFP == "15"]

            df_alaska = translate_geometries(df_alaska, 1300000, -4900000, 0.5, 32)
            df_hawaii = translate_geometries(df_hawaii, 5400000, -1500000, 1, 24)

            return pd.concat([df_main_land, df_alaska, df_hawaii])

        hhi_data = pd.read_csv(self.__out_path+filename, dtype={"FIPS": 'str'})
        # assign na and 0 to be -1
        hhi_data['HHI'] = hhi_data['HHI']\
            .apply(lambda x: -1 if pd.isna(x) or x == 0 else x)
        hhi_data['ENI'] = 1 / hhi_data['HHI']

        cur_target = 'HHI' if is_hhi else 'ENI'

        # Get map
        counties = gpd.read_file(f"{MAP_PATH}cb_2018_us_county_500k/")
        counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
        counties = counties.rename(columns={"GEOID": 'FIPS'})

        states = gpd.read_file(f"{MAP_PATH}cb_2018_us_state_500k/")
        # remove "unincorporated territories":
        # "Puerto Rico", "American Samoa", "United States Virgin Islands"
        # "Guam", "Commonwealth of the Northern Mariana Islands"
        states = states[~states.STATEFP.isin(["72", "69", "60", "66", "78"])]

        # map projection to be centered on United States.
        counties = counties.to_crs("ESRI:102003")
        states = states.to_crs("ESRI:102003")

        # place Alaska and Hawaii on the bottom left side on the graph.
        counties = adjust_maps(counties)
        states = adjust_maps(states)

        # add data with color to the states df.
        low_case_cnt_color = "lightgrey"
        data_breaks_hhi = [
            (0.25, "#B22222", f"{cur_target} ≥ 0.25"),        # Deep Red
            (0.2, "#ff3333", f"0.2 ≤ {cur_target} < 0.25"),   # Lighter Red
            (0.15, "#ff6666", f"0.15 < {cur_target} ≤ 0.2"),  # Medium Red
            (0.1, "#ff9999", f"0.1 < {cur_target} ≤ 0.15"),   # Light Red
            (0.05, "#ffcccc", f"0.05 < {cur_target} ≤ 0.1"),  # Pale Red
            (0, "#ffe6e6", f"0 ≤ {cur_target} < 0.05")        # Very Light Red
        ]
        # TODO: create this from above
        data_breaks_eni = [
            (20, "#ffe6e6", f"{cur_target} ≥ 0.25"),          # Very Light Red
            (10, "#ffcccc", f"0.2 ≤ {cur_target} < 0.25"),    # Pale Red
            (6.67, "#ff9999", f"0.15 < {cur_target} ≤ 0.2"),  # Light Red
            (5, "#ff6666", f"0.1 < {cur_target} ≤ 0.15"),     # Medium Red
            (4, "#ff3333", f"0.05 < {cur_target} ≤ 0.1"),     # Lighter Red
            (1, "#B22222", f"0 ≤ {cur_target} < 0.05")        # Deep Red
        ]

        data_breaks = data_breaks_hhi if is_hhi else data_breaks_eni
        upper_bound = 1 if is_hhi else 40
        lab = 'Herfindahl–Hirschman index' if is_hhi else 'Effective Number index'

        def create_color(county_df, data_breaks, target):
            colors = []
            for i, row in county_df.iterrows():
                for threshold, c, _ in data_breaks:
                    if row[target] >= threshold:
                        colors.append(c)
                        break
                else:
                    colors.append(low_case_cnt_color)
            return colors

        to_plot = counties.merge(hhi_data, on='FIPS', how='left')
        to_plot.loc[:, "color"] = create_color(to_plot, data_breaks, target=cur_target)

        # if 'case_cnt' is smaller than threshold, we set its color to 'low_case_cnt_color'
        cur_thresh = self.threshold
        to_plot.loc[to_plot[COL_CASE_CNT] < cur_thresh, 'color'] = low_case_cnt_color

        colors = [b[1] for b in data_breaks[::-1]]
        boundaries = [b[0] for b in data_breaks[::-1]] + [upper_bound]
        cmap = ListedColormap(colors)
        norm = BoundaryNorm(boundaries, ncolors=len(colors))

        # Plot with corrected logic
        fig, ax = plt.subplots(figsize=(self.sizex, self.sizey))
        counties.plot(ax=ax, edgecolor=edge_color + "55", color="none")
        to_plot.plot(ax=ax, edgecolor=edge_color, color=to_plot['color'], linewidth=0.5)

        # Add color bar
        sm = ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, orientation='vertical', shrink=0.4, pad=0.01)
        cbar.set_label(lab)
        # cbar.set_ticklabels()

        plt.axis("off")

        if save_fig:
            plt.savefig(f"{self.__plot_path}{cur_target}_heat_map.png", dpi=300, transparent=True)
            # plt.savefig(f"{self.__plot_path}{cur_target}_heat_map.svg", format="svg")
        else:
            plt.show()
        return

    def plot_hhi_time_srs(self, filename, save_fig=False):
        '''
        This originally only designed to plot states.
        Pass for now.
        '''

    def plot_yoy_hhi_price_scatter(self, filename, is_hhi, is_mean, regime="", save_fig=False):
        '''
        This function plots the scatter plot of HHI, with x-axis being the
        county-level HHI and the y-axis being the house price change.

        Currently, since we have only the aggregated case count and sale amt
        for counties, we assume all the properties have the same size, and use
        the average sale value as the housing price for each county.
        Still, we'll ignore the data with limited case count to avoided biased HHI.
        '''
        cur_mid = 'MEAN' if is_mean else 'MEDIAN'
        cur_target = 'HHI' if is_hhi else 'ENI'
        price_col = f'COUNTY_YOY_{cur_mid}_UNIT_PRICE'

        panel_df = pd.read_csv(self.__out_path+filename, dtype={'FIPS': 'str'})

        # make sure the yoy is really year by year
        panel_df = panel_df[panel_df['is_consec_year'] == 'Y']

        # drop if the COUNTY_CASE_CNT is less than threshold (notice this is for each year)
        panel_df = panel_df[panel_df['COUNTY_CASE_CNT'] >= self.threshold]

        # drop if COUNTY_YOY_UNIT_PRICE and HHI is NaN, and HHI should be > 0
        exclude_na = [price_col, 'HHI']
        panel_df = panel_df.dropna(subset=exclude_na)
        panel_df = panel_df[panel_df['HHI'] > 0]

        # confine yoy to be within 100%
        panel_df = panel_df[panel_df[price_col].abs() <= 100]

        # define good time and bad time
        p_chg_mean = panel_df[price_col].mean()
        p_chg_std = panel_df[price_col].std()
        if regime.lower() == 'good':
            panel_df = panel_df[panel_df[price_col] >= (p_chg_mean)]
            regime = '_' + regime # for plot name
        elif regime.lower() == 'bad':
            panel_df = panel_df[panel_df[price_col] < (p_chg_mean)]
            regime = '_' + regime # for plot name
        else:
            regime = ''

        panel_df['ENI'] = 1 / panel_df['HHI']

        # ======
        #  Plot
        # ======
        fig, ax1 = plt.subplots(figsize=(self.sizex, self.sizey))
        dot_size = 10

        # Solid points for TOP
        ax1.scatter(
            panel_df[cur_target],
            panel_df[price_col],
            color='black',
            s=dot_size,
            alpha=0.8
        )
        ax1.set_xlabel(cur_target, fontsize=12)
        ax1.set_ylabel(f'YOY {cur_mid} UNIT PRICE', fontsize=12)
        # ax1.set_title('Scatter Plot with Solid and Hollow Points', fontsize=14)
        # ax1.legend()
        ax1.grid(True)

        if save_fig:
            plt.savefig(f"{self.__plot_path}yoy_{cur_mid}_price_{cur_target}{regime}_scatter_plot.png",
                        dpi=300, transparent=True)
            # plt.savefig(f"{self.__plot_path}yoy_{cur_mid}_price_{cur_target}_scatter_plot.svg", format="svg")
        else:
            plt.show()

        return

    def plot_hhi_period_price_chg_scatter(self, filename, is_hhi, is_mean, period: list, save_fig=False):
        '''
        This function plots the scatter plot of HHI, with x-axis being the
        county-level HHI and the y-axis being the house price change.

        Currently, since we have only the aggregated case count and sale amt
        for counties, we assume all the properties have the same size, and use
        the average sale value as the housing price for each county.
        Still, we'll ignore the data with limited case count to avoided biased HHI.
        '''
        cur_mid = 'MEAN' if is_mean else 'MEDIAN'
        cur_target = 'HHI' if is_hhi else 'ENI'
        cur_price = f'COUNTY_{cur_mid}_UNIT_PRICE'

        panel_df = pd.read_csv(self.__out_path+filename,
                        dtype={'FIPS': 'str', 'YEAR': 'int'})

        # drop if the COUNTY_CASE_CNT is less than threshold (notice this is for each year)
        panel_df = panel_df[panel_df['COUNTY_CASE_CNT'] >= self.threshold]

        # drop if  and HHI is NaN, and HHI should be > 0
        exclude_na = [cur_price, 'HHI']
        panel_df = panel_df.dropna(subset=exclude_na)
        panel_df = panel_df[panel_df['HHI'] > 0]

        panel_df['ENI'] = 1 / panel_df['HHI']

        # make sure county data exist in both year, i.e. should have two entries
        panel_df = panel_df[panel_df['YEAR'].isin(period)]
        panel_df = panel_df[panel_df['FIPS'].map(panel_df['FIPS'].value_counts()) > 1]

        # cols = ['YEAR', cur_price, cur_target]
        cols = ['FIPS', cur_price]
        df_year1 = panel_df.loc[panel_df['YEAR'] == period[0], cols]
        df_year2 = panel_df.loc[panel_df['YEAR'] == period[1], cols]
        price_data = df_year1.rename(columns={cur_price: 'p1'})\
            .merge(df_year2.rename(columns={cur_price: 'p2'}), on='FIPS', how='left')

        # TODO: for now, use the HHI from entire data, not period nor year specific
        type_spec = {'FIPS': 'str', 'HHI': 'float'}
        allyear_hhi = pd.read_csv(self.__out_path+'for_heat_map.csv',
                        usecols=type_spec.keys(), dtype=type_spec)

        for_plot = price_data.merge(allyear_hhi, on='FIPS', how='left')
        for_plot['price_chg'] = for_plot['p2'] / for_plot['p1'] - 1
        for_plot['ENI'] = 1 / for_plot['HHI']

        # filter outliers
        for_plot = for_plot[for_plot['price_chg'] < 3]
        # for_plot = for_plot[for_plot[cur_target] < 100]  # for checking ENI

        # ======
        #  Plot
        # ======
        fig, ax1 = plt.subplots(figsize=(self.sizex, self.sizey))
        dot_size = 10

        # Solid points for TOP
        ax1.scatter(
            for_plot[cur_target],
            for_plot['price_chg'],
            color='black',
            s=dot_size,
            alpha=0.8
        )
        ax1.set_xlabel(cur_target, fontsize=12)
        ax1.set_ylabel(f'{period[0]} to {period[1]} {cur_mid} UNIT PRICE CHG', fontsize=12)
        # ax1.set_title('Scatter Plot with Solid and Hollow Points', fontsize=14)
        # ax1.legend()
        ax1.grid(True)

        if save_fig:
            plt.savefig(f"{self.__plot_path}yoy_{cur_mid}_price_{cur_target}_scatter_plot.svg", format="svg")
        else:
            plt.show()

        return

    def plot_county_price_chg_based_scatter(self, panel_fname, save_fig=False):
        '''
        The county-level price change is the average unit price in each county, set as x-axis.
        On the y-axis, there are two options:
          1. Quantity sold
          2. Mean unit price difference between [top or bottom groups] and county

        Each year, each county would contribute two dots,
        one for top 1/3 as solid dot, the other for bottom 1/3 as hollow dot

        (opt.) Maybe different region (North, West...) with different color.

        Parameters
        ----------
        filename
            The stacked and cleaned county yearly panel data.

        Return
        ------
            None.
        '''
        type_spec = {
            'FIPS': 'str'
        }
        panel_df = pd.read_csv(self.__out_path+panel_fname, dtype=type_spec)

        # ================
        #  Basic Checking
        # ================
        # print(panel_df['TOP_CASE_CNT'].max(skipna=True))
        # idx = panel_df['COUNTY_YOY_UNIT_PRICE'].idxmax()
        # print(panel_df.loc[idx, :])

        # ======
        #  Prep
        # ======
        # make 'IS_CONSEC' column to avoid false YOY
        def check_consecutive(group):
            # Check if current and previous years both have non-null values
            is_consec = group['COUNTY_UNIT_PRICE'].notna() & group['COUNTY_UNIT_PRICE'].shift(1).notna()
            return is_consec.replace({True: 'Y', False: None})

        # Apply the function for each FIPS group
        panel_df['IS_CONSEC'] = panel_df.groupby('FIPS', group_keys=False).apply(check_consecutive)

        # (opt.) get the state abbrv and region as a new column for different coloring

        # drop if the COUNTY_CASE_CNT is less than threshold (notice this is for each year)
        plot_base = panel_df[panel_df['COUNTY_CASE_CNT'] >= self.threshold]

        # drop if COUNTY_YOY_UNIT_PRICE is NaN and not consecutive year
        exclude_na = ['COUNTY_YOY_UNIT_PRICE', 'TOP_UNIT_PRICE', 'BOT_UNIT_PRICE']
        plot_base = plot_base.dropna(subset=exclude_na)
        plot_base = plot_base[plot_base['COUNTY_YOY_UNIT_PRICE'].abs() <= 100]
        plot_base = plot_base[plot_base['IS_CONSEC'] == 'Y']

        # ================
        #  Extra Checking
        # ================
        # idx = plot_base['TOP_CASE_CNT'].idxmax()
        # print(plot_base.loc[idx, :])
        # print(plot_base.loc[plot_base['TOP_CASE_CNT'] >= 25000, ['FIPS', 'YEAR']])


        # ==========
        #  Plotting
        # ==========
        dot_size = 10

        # 1. Quantity of top and bot as y-axis
        def figure_1():
            plot_1_data = plot_base.dropna(subset=['TOP_CASE_CNT', 'BOT_CASE_CNT'])
            plot_1_data = plot_1_data[['COUNTY_YOY_UNIT_PRICE','TOP_CASE_CNT', 'BOT_CASE_CNT']]

            plot_1_data = plot_1_data[plot_1_data['TOP_CASE_CNT'] <= 10000]

            fig, ax1 = plt.subplots(figsize=(8, 6))

            # Solid points for TOP
            ax1.scatter(
                plot_1_data['COUNTY_YOY_UNIT_PRICE'],
                plot_1_data['TOP_CASE_CNT'],
                label='TOP 33%',
                color='black',
                s=dot_size,
                alpha=0.8
            )

            # Hollow points for BOT
            ax1.scatter(
                plot_1_data['COUNTY_YOY_UNIT_PRICE'],
                plot_1_data['BOT_CASE_CNT'],
                label='BOT 33%',
                edgecolors='Red',
                facecolors='none',
                s=dot_size,
                alpha=0.8
            )

            ax1.set_xlabel('County Avg. Price Change', fontsize=12)
            ax1.set_ylabel('Quantity Sold', fontsize=12)
            # ax1.set_title('Scatter Plot with Solid and Hollow Points', fontsize=14)
            ax1.legend()
            ax1.grid(True)


        # 2. Relative quantity as y-axis
        def figure_2():
            plot_2_data = plot_base.dropna(subset=['TOP_CASE_CNT', 'BOT_CASE_CNT'])
            plot_2_data = plot_2_data[['COUNTY_YOY_UNIT_PRICE','TOP_CASE_CNT', 'BOT_CASE_CNT']]
            plot_2_data['quantity_ratio'] = plot_2_data['TOP_CASE_CNT'] / plot_2_data['BOT_CASE_CNT']

            plot_2_data = plot_2_data[plot_2_data['quantity_ratio'] <= 200]

            fig, ax2 = plt.subplots(figsize=(8, 6))

            # Solid points for TOP
            ax2.scatter(
                plot_2_data['COUNTY_YOY_UNIT_PRICE'],
                plot_2_data['quantity_ratio'],
                label='TOP 33%',
                color='black',
                s=dot_size,
                alpha=0.8
            )

            ax2.set_xlabel('County Avg. Price Change', fontsize=12)
            ax2.set_ylabel('Top-Bot Quantity Sold Ratio', fontsize=12)
            # ax2.set_title('Scatter Plot', fontsize=14)
            ax2.legend()
            ax2.grid(True)


        # 3. Top and bot price diff as y-axis
        def figure_3():
            plot_3_data = plot_base.dropna(subset=['TOP_P_DIFF', 'BOT_P_DIFF'])
            plot_3_data = plot_3_data[['COUNTY_YOY_UNIT_PRICE','TOP_P_DIFF', 'BOT_P_DIFF']]

            # drop if top are negative
            cond = (plot_3_data['TOP_P_DIFF'] >= 0) & (plot_3_data['TOP_P_DIFF'] < 25000)
            plot_3_data = plot_3_data[cond]

            # some filtering
            plot_3_data = plot_3_data[plot_3_data['TOP_P_DIFF'] <= 1000]

            fig, ax3 = plt.subplots(figsize=(8, 6))

            # Solid points for TOP
            ax3.scatter(
                plot_3_data['COUNTY_YOY_UNIT_PRICE'],
                plot_3_data['TOP_P_DIFF'],
                label='TOP 33%',
                color='black',
                s=dot_size,
                alpha=0.8
            )

            # Hollow points for BOT
            ax3.scatter(
                plot_3_data['COUNTY_YOY_UNIT_PRICE'],
                plot_3_data['BOT_P_DIFF'],
                label='BOT 33%',
                edgecolors='Red',
                facecolors='none',
                s=dot_size,
                alpha=0.8
            )

            ax3.set_xlabel('County Avg. Price Change (%)', fontsize=12)
            ax3.set_ylabel('Price Diff', fontsize=12)
            # ax3.set_title('Scatter Plot with Solid and Hollow Points', fontsize=14)
            ax3.legend()
            ax3.grid(True)

        figure_3()

        # Show the plot
        plt.show()

    def plot_hhi_county_state_price_yoy_ratio_scatter(self, filename, is_hhi, is_mean, save_fig=False):
        cur_mid = 'MEAN' if is_mean else 'MEDIAN'
        cur_target = 'HHI' if is_hhi else 'ENI'
        price_col = f'COUNTY_YOY_{cur_mid}_UNIT_PRICE'

        panel_df = pd.read_csv(self.__out_path+filename, dtype={'FIPS': 'str'})

        panel_df = panel_df[panel_df['is_consec_year'] == 'Y']

        # drop if the COUNTY_CASE_CNT is less than threshold (notice this is for each year)
        panel_df = panel_df[panel_df['COUNTY_CASE_CNT'] >= self.threshold]

        # drop if COUNTY_YOY_UNIT_PRICE and HHI is NaN, and HHI should be > 0
        exclude_na = [price_col, 'HHI']
        panel_df = panel_df.dropna(subset=exclude_na)
        panel_df = panel_df[panel_df['HHI'] > 0]

        # report same direction pct for each state and the US
        panel_df['is_same_direction'] = panel_df[price_col] * panel_df['HPI_yoy'] > 0

        overall_percentage = panel_df['is_same_direction'].value_counts(normalize=True) * 100
        print(f"Overall Percentage:\n{overall_percentage}")

        county_percentage = panel_df.groupby('StateFIPS')['is_same_direction'].value_counts(normalize=True).unstack() * 100
        print(f"Percentage by StateFIPS:\n{county_percentage}")

        # for plotting, restrict to only same direction, already True and False
        panel_df = panel_df[panel_df['is_same_direction']]

        # TODO: change this, currently for checking only the positive time
        panel_df = panel_df[panel_df[price_col] > 0]

        panel_df['ENI'] = 1 / panel_df['HHI']
        panel_df['yoy_ratio'] = panel_df[price_col] / panel_df['HPI_yoy']

        # confine the ratio
        panel_df = panel_df[panel_df['yoy_ratio'] <= 50]

        # ======
        #  Plot
        # ======
        fig, ax1 = plt.subplots(figsize=(self.sizex, self.sizey))
        dot_size = 10

        # Solid points for TOP
        ax1.scatter(
            panel_df[cur_target],
            panel_df['yoy_ratio'],
            color='black',
            s=dot_size,
            alpha=0.8
        )
        ax1.set_xlabel(cur_target, fontsize=12)
        ax1.set_ylabel('County to State Relative PRICE GROWTH Magnitude', fontsize=12)
        # ax1.set_title('Scatter Plot with Solid and Hollow Points', fontsize=14)
        # ax1.legend()
        ax1.grid(True)

        if save_fig:
            plt.savefig(f"{self.__plot_path}yoy_{cur_mid}_p_chg_rel_mag_{cur_target}_scatter_plot.png",
                        dpi=300, transparent=True)
            # plt.savefig(f"{self.__plot_path}yoy_{cur_mid}_p_chg_rel_mag_{cur_target}_scatter_plot.svg", format="svg")
        else:
            plt.show()

        return

def csv_to_dta(filepath, filename):
    data = pd.read_csv(f'{filepath}{filename}.csv')

    data.to_stata(f'{filepath}{filename}.dta')


def main():
    a = Analysis(out_path=STACKED_PATH, plot_path=IMG_PATH)

    # csv_to_dta(STACKED_PATH, 'corelogic_seller_panel')
    # csv_to_dta(STACKED_PATH, 'homebuilder_panel')

    # ======
    #  Data
    # ======
    # Seller data
    # a.corelogic_seller_panel_prep(COUNTY_FILE_PATH)

    # Get corelogic_panel
    # a.corelogic_panel_prep(COUNTY_FILE_PATH)

    # Get data for heat map
    # a.corelogic_stack_year_prep(COUNTY_FILE_PATH)

    # Get homebuilder_panel
    # a.make_full_panel("corelogic_panel_33pct_v3.csv")  # combine corelogic panel with other variables

    # =======
    #  Plots
    # =======
    # Plot the new series of figure
    # a.plot_county_price_chg_based_scatter("corelogic_panel_33pct_v3.csv")

    # a.plot_hhi_heat_map('for_heat_map.csv',is_hhi=False, save_fig=True)

    a.plot_yoy_hhi_price_scatter(
        "homebuilder_panel.csv",
        is_hhi=False, is_mean=True, regime='good', save_fig=True
    )

    # a.plot_hhi_period_price_chg_scatter(
    #     "corelogic_panel_33pct_v3.csv", period=[2006, 2012],
    #     is_hhi=False, is_mean=True, save_fig=False
    # )

    # a.plot_hhi_period_price_chg_scatter(
    #     "corelogic_panel_33pct_v3.csv", period=[2012, 2016],
    #     is_hhi=True, is_mean=True, save_fig=False
    # )

    # a.plot_hhi_county_state_price_yoy_ratio_scatter(
    #     "homebuilder_panel.csv",
    #     is_hhi=False, is_mean=True, save_fig=True
    # )

    # ===============
    #  Wild Thoughts
    # ===============
    # so I'm thinking maybe we could do the animation of yearly HHI heatmap
    # change since some 1987?

    # seller's market and buyer's market
    # https://fred.stlouisfed.org/series/PRIINCCOU18163
    # https://fred.stlouisfed.org/series/PRIREDCOU18163

if __name__ == "__main__":
    main()