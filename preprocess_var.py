import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re
import platform
from datetime import datetime
import csv # for csv.QUOTE_NONE, which ignores ' when reading csv
import warnings
warnings.filterwarnings("ignore")


CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
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

        # 1. drop 'STATE' == 'United States'

        # 2. map the 'STATE' to state codes from fips2county.tsv.txt

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
        else:
            print(mhhi_panel)

    def get_unemployment(self, out_fname="unemployment_panel.csv", to_file=True):
        to_cat = []
        folder_path = f"{RAW_PATH}unemployment/"
        for fname in tqdm(os.listdir(folder_path), desc=f"Processing Unemployment files"):
            # eg. unemp_01001.csv

            # metadata files created by mac
            if fname[:2] == '._': continue

            rename_map = {
                "Year": 'Year',
                "Period": 'Month',
                'Value': 'Unemployment'
            }
            cur_file = pd.read_csv(f"{folder_path}{fname}", usecols=rename_map.keys())
            cur_file = cur_file.rename(columns=rename_map)

            cur_file['FIPS'] = fname[6:11]

            # rearrange column order
            non_fips = [col for col in cur_file.columns if col != 'FIPS']
            cur_file = cur_file[['FIPS'] + non_fips]

            # TODO: how to combine month to year

            print(cur_file)

            return

            to_cat.append(cur_file)

        mhhi_panel = pd.concat(to_cat, ignore_index=True)

        if to_file:
            mhhi_panel.to_csv(EXT_DISK+out_fname, index=False)
        else:
            print(mhhi_panel)


def main():
    vp = VarProcessor()

    # vp.get_population(to_file=False) # 75426
    # vp.get_house_stock(to_file=False) # 75400
    # vp.get_vacancy()
    # vp.get_median_hh_income()
    vp.get_unemployment()




if __name__ == "__main__":
    main()