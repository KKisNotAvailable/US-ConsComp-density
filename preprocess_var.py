import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re
import platform
from datetime import datetime
import chardet
import csv # for csv.QUOTE_NONE, which ignores ' when reading csv
import warnings
warnings.filterwarnings("ignore")


CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
FIPS_MAP_PATH = EXT_DISK + "Homebuilder/"
EXT_DISK += "Homebuilder/Variables/"
RAW_PATH = EXT_DISK + "raw/"


class VarProcessor():
    def __init__(self) -> None:
        pass

    def get_population(self, out_fname="pop_panel.csv", to_file=True):
        '''
        State data is where COUNTY == '000', so the FIPS would be '##000'
        data column looks like POPESTIMATE2000
        '''
        type_spec = {"STATE": 'category', "COUNTY": 'category'}
        to_cat = []
        filenames = ['co-est2009-alldata', 'co-est2019-alldata', 'co-est2023-alldata']

        for fname in filenames:
            cur_file = pd.read_csv(f"{RAW_PATH}{fname}.csv", encoding="latin1", dtype=type_spec)

            # filter out non county
            cur_file = cur_file[cur_file["COUNTY"] != '000']

            data_cols = list(cur_file.filter(like="POPESTIMATE").columns)
            cur_file['FIPS'] = [s + c for s, c in zip(cur_file['STATE'], cur_file['COUNTY'])]
            cur_file = cur_file[['FIPS'] + data_cols]
            cur_file.rename(columns=lambda x: x.replace("POPESTIMATE", ""), inplace=True)

            cur_panel = pd.melt(cur_file, id_vars=["FIPS"], var_name="Year", value_name="Population")

            to_cat.append(cur_panel)

        pop_panel = pd.concat(to_cat, ignore_index=True)

        if to_file:
            pop_panel.to_csv(EXT_DISK+out_fname, index=False)
            print("Poulation Done.")
        else:
            print(pop_panel)

    def get_house_stock(self, out_fname="house_stock_panel.csv", to_file=True):

        type_spec = {"STATE": 'category', "COUNTY": 'category'}

        filenames = ['hu-est2009-us.csv', 'HU-EST2020_ALL.csv', 'CO-EST2023-HU.xlsx']

        to_cat = []

        for fname in filenames:
            if '.xlsx' in fname:
                cur_file = pd.read_excel(f"{RAW_PATH}{fname}", header=3)
                cur_file = cur_file.loc[:3144, :]
                cur_file = cur_file.rename(
                    columns={"Unnamed: 0": 'NAMES', "Unnamed: 1": 'bye'}
                )
                cur_file = cur_file.merge(name2fips, on='NAMES', how='left')

                year_cols = [col for col in cur_file.columns if str(col).isnumeric()]
                cur_file = cur_file[['FIPS'] + year_cols]
                # print(cur_file[cur_file['FIPS'].isna()])

                cur_file = cur_file.dropna(subset=['FIPS'])

            else:
                cur_file = pd.read_csv(f"{RAW_PATH}{fname}", encoding="latin1", dtype=type_spec)

                # huest_2000~09, HUESTIMATE2010~20
                cur_file = cur_file[cur_file["COUNTY"] != '000']
                cur_file['FIPS'] = [s + c for s, c in zip(cur_file['STATE'], cur_file['COUNTY'])]

                # create a name-fips map
                if '2020' in fname:
                    cur_file['NAMES'] = [f".{c}, {s}" for s, c in zip(cur_file['STNAME'], cur_file['CTYNAME'])]
                    # might want to find other source of mapping list to deal with the Connecticut county problem
                    name2fips = cur_file[['FIPS', 'NAMES']]

                key_word = 'huest_' if '2009' in fname else 'HUESTIMATE'
                data_cols = list(cur_file.filter(like=key_word).columns)

                cur_file = cur_file[['FIPS'] + data_cols]
                cur_file.rename(columns=lambda x: x.replace(key_word, ""), inplace=True)
                data_cols = [col for col in cur_file.columns if col.isnumeric() or col == 'FIPS']
                # drop the 2020 data (use the 2023 version)
                cur_file = cur_file[[col for col in data_cols if not '2020' in col]]

            cur_panel = pd.melt(cur_file, id_vars=["FIPS"], var_name="Year", value_name="House_stock")

            to_cat.append(cur_panel)

        hs_panel = pd.concat(to_cat, ignore_index=True)

        if to_file:
            hs_panel.to_csv(EXT_DISK+out_fname, index=False)
            print("House Stock Done.")
        else:
            print(hs_panel)

    def get_vacancy(self, out_fname="vacancy_panel.csv", to_file=True):
        raw_data = pd.read_excel(f"{RAW_PATH}ann23t_5a.xlsx", header=5)
        raw_data = raw_data.rename(columns={"Unnamed: 0": 'STATE'})
        raw_data.columns = [str(col) for col in raw_data.columns]
        raw_data['STATE'] = raw_data['STATE'].str.replace(".", "")
        raw_data = raw_data.dropna(subset=['2005'])
        raw_data = raw_data.loc[:170, :]

        # start seperating years
        # 2005~2012
        wide_data_05_12 = raw_data.loc[:55, :].reset_index(drop=True)
        # 2013~2020
        wide_data_13_20 = raw_data.loc[59:114, :].reset_index(drop=True)
        wide_data_13_20.columns = ['STATE'] + [f"20{str(n)}" for n in range(13, 21)]
        # 2021~2023
        wide_data_21_23 = raw_data.loc[116:, :].reset_index(drop=True)
        wide_data_21_23 = wide_data_21_23.dropna(axis=1, how='all')
        wide_data_21_23.columns = ['STATE', '2021', '2022', '2023']

        vacant_wide = wide_data_05_12.merge(wide_data_13_20, on='STATE', how='left')
        vacant_wide = vacant_wide.merge(wide_data_21_23, on='STATE', how='left')

        # 1. drop 'STATE' == 'United States'
        vacant_wide = vacant_wide[vacant_wide['STATE'] != 'United States']

        # 2. map the 'STATE' to state codes from fips2county.tsv.txt
        state2code = pd.read_csv(
            "../NewCoreLogic_Codes/Data/fips2county.tsv.txt",
            delimiter='\t',
            usecols=['StateFIPS', 'StateName'],
            dtype='str'
        )

        state2code = state2code.rename(columns={'StateName': 'STATE'})
        state2code = state2code.drop_duplicates()
        vacant_wide = vacant_wide.merge(state2code, on='STATE', how='left')
        vacant_wide = vacant_wide.drop(columns='STATE')

        vacant_panel = pd.melt(vacant_wide, id_vars=["StateFIPS"], var_name="Year", value_name="Vacancy_rate")

        if to_file:
            vacant_panel.to_csv(EXT_DISK+out_fname, index=False)
            print("Vacancy Rate Done.")
        else:
            print(vacant_panel)

    def get_median_hh_income(self, out_fname="med_hh_income_panel.csv", to_file=True):
        to_cat = []
        folder_path = f"{RAW_PATH}median_hh_income/"
        for fname in tqdm(os.listdir(folder_path), desc=f"Processing Median HH Income files"):
            # eg. MHIAK02016A052NCEN.csv

            # metadata files created by mac
            if fname[:2] == '._': continue

            new_cols = ['Year', 'Median_HH_income']
            cur_file = pd.read_csv(f"{folder_path}{fname}")
            cur_file.columns = new_cols
            cur_file['Year'] = cur_file['Year'].str[:4]
            cur_file = cur_file[cur_file['Year'] >= '2000']

            cur_file['FIPS'] = fname[5:10]

            # rearrange column order
            cur_file = cur_file[['FIPS'] + new_cols]

            to_cat.append(cur_file)

        mhhi_panel = pd.concat(to_cat, ignore_index=True)

        if to_file:
            mhhi_panel.to_csv(EXT_DISK+out_fname, index=False)
            print("Median Household Income Done.")
        else:
            print(mhhi_panel)

    def get_unemployment(self, out_fname="unemployment_rate_panel.csv", to_file=True):
        to_cat = []
        # folder_path = f"{RAW_PATH}unemployment/"
        folder_path = f"{RAW_PATH}unemp_rate/"
        for fname in tqdm(os.listdir(folder_path), desc=f"Processing Unemployment files"):
            # eg. unemp_01001.csv

            # metadata files created by mac
            if fname[:2] == '._': continue

            rename_map = {
                "Year": 'Year',
                "Period": 'Month',
                'Value': 'Unemployment_rate'
            }
            try:
                cur_file = pd.read_csv(f"{folder_path}{fname}",
                        usecols=rename_map.keys(), dtype={'Year': 'category'})
                cur_file = cur_file.rename(columns=rename_map)

                cur_file['FIPS'] = fname[6:11]

                # take the end of each year as yearly data
                cur_file = cur_file[cur_file['Month'] == 'M12']

                # rearrange column order
                cur_file = cur_file[['FIPS', 'Year', 'Unemployment_rate']]

                to_cat.append(cur_file)
            except:
                print(f'{fname}')

        unemp_panel = pd.concat(to_cat, ignore_index=True)

        if to_file:
            unemp_panel.to_csv(EXT_DISK+out_fname, index=False)
            print("Unemployment Done.")
        else:
            print(unemp_panel)

    def get_wrluri(self, ver=2006, out_fname="WRLURI", to_file=True):
        out_fname += f'_{ver}.csv'

        type_spec_fips = {
            "STATE": 'category',
            "STATEFP": 'category',
            "COUNTYFP": 'category',
            "PLACEFP": 'str'
        }
        fips_map = pd.read_csv(
            FIPS_MAP_PATH+"city_to_county_fips.txt",
            delimiter='|', usecols=type_spec_fips.keys(), dtype=type_spec_fips
        )
        fips_map['FIPS'] = [s + c for s, c in zip(fips_map['STATEFP'], fips_map['COUNTYFP'])]
        fips_map = fips_map[['FIPS', 'STATE', 'PLACEFP']]

        type_spec_2018 = {
            'statecode': 'category',  # might be missing leading 0, need to fix
            'countycode18': 'str',  # might be missing leading 0, need to fix
            'WRLURI18': 'float'
        }

        type_spec_2006 = {
            'ufips': 'str',  # might be missing leading 0, need to fix
            'statename': 'category',
            'WRLURI': 'float'
        }

        if ver == 2006:
            wi_2006 = pd.read_stata(f"{RAW_PATH}WRLURI_1_24_2008.dta")
        elif ver == 2018:
            wi_2018 = pd.read_stata(f"{RAW_PATH}WRLURI_01_15_2020.dta")
        else:
            print("Not a valid version, only 2006 and 2018 available...")
            return

        wi_2006 = wi_2006[['ufips', 'WRLURI', 'statename']]\
            .rename(columns={"ufips": 'PLACEFP', "statename": 'STATE'})
        wi_2006['PLACEFP'] = wi_2006['PLACEFP'].astype(int).astype(str).str.zfill(5)

        wi_2006 = wi_2006.merge(fips_map, on=['STATE', 'PLACEFP'], how='left')

        if to_file:
            wi_2006.to_csv(EXT_DISK+out_fname, index=False)
            print("WRLURI Done.")
        else:
            print(wi_2006)

    def get_natural_disaster(self, out_fname="natural_disaster_panel.csv", to_file=True):
        # detect encoding since the file could not open with utf-8 and latin1
        def detect_encoding():
            with open(f"{RAW_PATH}Incident_Type_Full_Data.csv", 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                print(result)

        name_spec = {
            'Incident Category': 'incident',
            # 'Calendar Year of Declaration': 'Year',  # 2024, but not the actual happen year...
            'Incident Begin Date': 'start_date', # 7/24/2024 12:00:00 AM
            'Incident End Date': 'end_date',
            'Fips State Code': 'stateFIPS',
            'Fips County Code': 'countyFIPS'
        }

        nd_record = pd.read_csv(
            f"{RAW_PATH}Incident_Type_Full_Data.csv",
            encoding="utf-16", delimiter='\t',
            dtype='str', usecols=name_spec.keys()
        )
        nd_record = nd_record.rename(columns=name_spec)

        nd_record = nd_record.dropna(subset=['start_date', 'end_date'])

        # clean date
        nd_record['start_date'] = pd.to_datetime(nd_record['start_date'])
        nd_record['end_date'] = pd.to_datetime(nd_record['end_date'])

        nd_record['start_year'] = nd_record['start_date'].dt.year
        nd_record['start_month'] = nd_record['start_date'].dt.month
        nd_record['end_year'] = nd_record['end_date'].dt.year
        nd_record['end_month'] = nd_record['end_date'].dt.month

        # make FIPS
        nd_record['stateFIPS'] = nd_record['stateFIPS'].str.zfill(2)
        nd_record['countyFIPS'] = nd_record['countyFIPS'].str.zfill(3)

        nd_record['FIPS'] = [s + c for s, c in zip(nd_record['stateFIPS'], nd_record['countyFIPS'])]

        nd_record = nd_record[[
            'FIPS', 'start_year', 'start_month', 'end_year', 'end_month', 'incident'
        ]]

        # nd_record['start_year'] = nd_record['start_year'].astype(int)
        # nd_record['end_year'] = nd_record['end_year'].astype(int)

        nd_record = nd_record.sort_values(
            by=['FIPS', 'start_year'], ascending=[True, True]).reset_index(drop=True)

        # make the duplicate rows based on the difference between start and end year
        def expand_rows(row):
            years = range(row['start_year'], row['end_year'] + 1)
            return pd.DataFrame({
                "FIPS": [row['FIPS']] * len(years),
                "year": years
            })

        # Apply the function to expand the DataFrame
        nd_record = pd.concat([expand_rows(row) for _, row in nd_record.iterrows()], ignore_index=True)

        # grouped_record = nd_record.groupby(['FIPS', 'start_year']).size().reset_index(name='Count')
        grouped_record = nd_record.groupby(['FIPS', 'year']).size().reset_index(name='Count')

        if to_file:
            grouped_record.to_csv(EXT_DISK+out_fname, index=False)
            print("Natural Disaster Done.")
        else:
            print(grouped_record)



def main():
    vp = VarProcessor()

    # vp.get_population() # 75426
    # vp.get_house_stock() # 75400
    # vp.get_vacancy()
    # vp.get_median_hh_income()
    vp.get_unemployment()
    # vp.get_wrluri()
    # vp.get_natural_disaster()  # notice some of the FIPS has 000 in the end, eg. 01000 => might indicate entire state



if __name__ == "__main__":
    main()