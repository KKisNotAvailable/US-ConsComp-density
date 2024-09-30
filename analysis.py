import dask.dataframe as dd
import pandas as pd
import os
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json

from PIL import Image
from matplotlib.patches import Patch, Circle

# setting image theme with seaborn
edge_color = "#30011E"
background_color = "#fafafa"

sns.set_style({
    "font.family": "serif",
    "figure.facecolor": background_color,
    "axes.facecolor": background_color,
})

'''NOTES

(don't need now..., the first column 'FIPS' is actually the counties) 
source of US cities list (Basic ver.): https://simplemaps.com/data/us-cities

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

class Analysis():
    def __init__(self, out_path: str = "./output/") -> None:
        self.__out_path = out_path

        if not os.path.isdir(out_path):
            os.mkdir(out_path)

    def deed_analysis(self):
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
                "SALE DATE"
            ]
            good_counts = ddf[cols_to_clean].count().compute()
            
            print("Non NA pct for ...")
            for i, c in enumerate(cols_to_clean):
                print(f"  {c:<11}: {good_counts.iloc[i]*100/ttl_rows:>10.5f}%")

            # print(ddf['SALE DATE'].dtypes) # int64

        def check_city_list():
            # read the state-county-city data
            scc_map = pd.read_csv("data/simplemaps_uscities_basicv1.79/uscities.csv")
            scc_map = scc_map[['state_id', 'county_fips', 'zips']]
            print(scc_map.head(5))
        
        check_deed_var_distribution()
        return
    
    def deed_prep(self):
        '''
        This method would prepare a deed dataset from the stacked csv for analysis.
        Specifically, this data is stacked data grouped by year, state, fips.
        And the value columns are the sum of sale amount and case count.

        Notice:
        1. we do not calculate the HHI here, since we will calculate HHI for 
           years seperated and aggregated.
        2. the unique seller count is a little hard to present in this dataset
           因為在不同county中也可能有同一個seller，以目前資料設計來看，這樣依state加總就會失真
        '''
        ddf = dd.read_csv('data/deed_stacked.csv')

        cols_to_clean = [
            "FIPS", # = counties
            "SITUS_STATE", # eg. CA
            "SALE AMOUNT", 
            "SALE DATE" # yyyymmdd, int64
        ]

        ddf = ddf[cols_to_clean]

        # 1. data prep
        #   1.1 ignore rows if any of those columns is empty
        #   1.2 make the year column
        ddf = ddf.dropna(subset=cols_to_clean)
        ddf['year'] = ddf['SALE DATE'] // 10000

        # 2. group by year, SITUS_STATE, FIPS
        #   2-1. count cases
        #   2-2. sum "SALE AMOUNT"

        
        ddf\
            .groupby(['year', 'SITUS_STATE', 'FIPS'])\
            .agg(
                case_cnt=('SALE DATE', 'count'),
                sale_amt=('SALE AMOUNT', 'sum')
            ).compute()

    def deed_prep_old(self) -> list:
        '''
        This program would prepare the files for plotting.
        And since using dask is quite slow, the thing returned would 
        be filenames rather than dataframe.
        '''
        ddf = dd.read_csv('data/deed_stacked.csv')

        cols_to_clean = [
            "FIPS", # = counties
            "SITUS_STATE", # eg. CA
            "SALE AMOUNT", 
            "SALE DATE" # yyyymmdd
        ]

        # 1. ignore rows if any of those columns is empty
        ddf = ddf.dropna(subset=cols_to_clean)

        # 2. group by FIPS / SITUS_STATE 
        #   2-1. count cases
        #   2-2. sum "SALE AMOUNT"
        #   2-3. get unique sellers
        #   2-4. top 50 seller's case count (per state)
        #   2-5. top 50 seller's sale amt (per state)
        #   2-6. HHI by case count (per state)
        #   2-7. HHI by sale amt (per state)

        grouped_results = {
            "FIPS": None, 
            "SITUS_STATE": None
        }

        for c in grouped_results.keys():
            to_cat = [
                ddf.groupby(c)\
                    .agg(
                        case_cnt=('SALE DATE', 'count'),
                        sale_amt=('SALE AMOUNT', 'sum')
                    )\
                    .compute(),
                # since the following is a series, no need to specify column when renaming
                ddf.groupby(c)['SELLER NAME1']\
                    .nunique()\
                    .rename('uniq_seller_cnt')\
                    .compute()
            ]
            
            # for computing Herfindahl-Hirschman Index
            full_list = ddf.groupby([c, 'SELLER NAME1'])\
                .agg(
                    case_cnt=('SALE DATE', 'count'),
                    sale_amt=('SALE AMOUNT', 'sum')
                )\
                .reset_index()\
                .compute()
            
            TOP_N = 50 # defined in HHI
            # since 'full_list' is not large, using apply is reasonable
            # top 50 case_cnt and sale_amt
            to_cat.extend([
                full_list.groupby(c)['case_cnt']\
                    .apply(lambda grp: grp.nlargest(TOP_N).sum())\
                    .rename(f'top_{TOP_N}_case_cnt'),
                full_list.groupby(c)['sale_amt']\
                    .apply(lambda grp: grp.nlargest(TOP_N).sum())\
                    .rename(f'top_{TOP_N}_sale_amt')
            ])
            # HHI = sum((x_i/X)^2)
            to_cat.extend([
                full_list.groupby(c)['case_cnt']\
                    .apply(
                        lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
                    )\
                    .rename('HHI_case_cnt'),
                full_list.groupby(c)['sale_amt']\
                    .apply(
                        lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
                    )\
                    .rename('HHI_sale_amt')
            ])
            
            grouped_results[c] = pd.concat(to_cat, axis=1).reset_index() # horizontally
            
        files_out = []
        for c, res in grouped_results.items():
            fname = f"agg_result_{c}.csv"
            files_out.append(fname)
            self.file_out(df=res, filename=fname)

        # do similar action above but based on SALE YEAR
        # maybe the plotting can be animated? 
        # (even is the plotting for yearly possible?)

        return files_out
    
    def deed_plot(self, filenames):
        '''
        REF: https://dev.to/oscarleo/how-to-create-data-maps-of-the-united-states-with-matplotlib-p9i
        plot the non yearly result on maps
        1. map points?
        2. current only has abbrev. of states

        counties: ['STATEFP', 'COUNTYFP', 'COUNTYNS', 'AFFGEOID', 'GEOID', 'NAME', 'LSAD', 'ALAND',
       'AWATER', 'geometry']
       states: ['STATEFP', 'STATENS', 'AFFGEOID', 'GEOID', 'STUSPS', 'NAME', 'LSAD',
       'ALAND', 'AWATER', 'geometry']
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

        if not isinstance(filenames, list):
            filenames = [filenames]

        for f in filenames:
            f = "./output/" + f
            # 這裡要改到其他地方，如果還要批量產出圖的話，目前只要畫state的
            HHI = pd.read_csv(f)
            # cur_hhi = 'HHI_case_cnt'
            cur_hhi = 'HHI_sale_amt'
            HHI = HHI[['SITUS_STATE', cur_hhi]]
            HHI.rename(
                columns={
                    'SITUS_STATE': 'STUSPS',
                    cur_hhi: 'value'
                }, 
                inplace=True
            )

        counties = gpd.read_file("./data/cb_2018_us_county_500k/")
        counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
        counties = counties.set_index("GEOID")

        states = gpd.read_file("./data/cb_2018_us_state_500k/")
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
        selected_color = "#FA26A0"
        data_breaks = [
            (90, "#ff0000", "Top 10%"),   # Bright Red
            (70, "#ff4d4d", "90-70%"),    # Light Red
            (50, "#ff9999", "70-50%"),    # Lighter Red
            (30, "#ffcccc", "50-30%"),    # Pale Red
            (0,  "#ffe6e6", "Bottom 30%") # Very Light Red
        ]

        def create_color(county_df, data_breaks):
            colors = []

            for i, row in county_df.iterrows():
                for p, c, _ in data_breaks:
                    if row.value >= np.percentile(county_df.value, p):
                        colors.append(c)
                        break

            return colors
        
        states = pd.merge(states, HHI, on='STUSPS', how='left')
        states.loc[:, "color"] = create_color(states, data_breaks)

        sizex = 8
        sizey = 6
        ax = counties.plot(edgecolor=edge_color + "55", color="None", figsize=(sizex, sizey))
        states.plot(ax=ax, edgecolor=edge_color, color=states.color, linewidth=1)

        plt.axis("off")
        plt.show()

        return
    
    def file_out(self, df, filename: str) -> None:
        # TODO: filename validity check
        # TODO: kwargs for to_csv

        print(f"Generating {filename} ...")

        filename = self.__out_path + filename
        df.to_csv(filename, index=False)
        
        print("DONE")


def main():

    a = Analysis()

    # =======================
    # Test analysis with dask
    # =======================
    # maybe can set a switch to give only filenames rather than 
    # generating the whole thing
    # files = a.deed_prep()

    a.deed_analysis()

    # agg_result_SITUS_CITY.csv -> 這個可能需要把state也留一下，不然不知道怎麼對
    # files = ['agg_result_SITUS_STATE.csv']
    # a.deed_plot(files)

if __name__ == "__main__":
    main()