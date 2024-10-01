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
        

        # check_deed_var_distribution()
        return
    
    def deed_prep(self, is_yearly=True, gen_data=False):
        '''
        This method would prepare a deed dataset from the stacked csv for analysis.
        Specifically, this data is stacked data grouped by year, state, fips.
        And the value columns are the sum of sale amount and case count.

        Notice:
        1. we do not calculate the HHI here, since we will calculate HHI for 
           years seperated and aggregated.
        2. the unique seller count is a little hard to present in this dataset
           因為在同state不同county中也可能有同一個seller, 以目前資料設計來看, 這樣依state加總就會失真
        '''
        ddf = dd.read_csv('data/deed_stacked.csv')

        cols_to_clean = [
            "FIPS", # = counties
            "SITUS_STATE", # eg. CA
            "SELLER NAME1",
            "SALE AMOUNT", 
            "RECORDING DATE", # yyyymmdd, int64
            "SALE DATE" # yyyymmdd, int64
        ]

        ddf = ddf[cols_to_clean]

        # 1. data prep
        #   1.1 ignore rows if any of those columns is empty
        #   1.2 fill the 'SALE DATE' when 0 with 'RECORDING DATE'
        #   1.3 make the 'year' column (opt.)
        #   1.3 (cont.) ignore the rows with 'year' == 0, 
        #       should be around 5831 after combining SALE and RECORDING
        ddf = ddf.dropna(subset=cols_to_clean)
        ddf['SALE DATE'] = ddf['SALE DATE'].mask(ddf['SALE DATE'] == 0, ddf['RECORDING DATE'])

        if is_yearly:
            ddf['year'] = ddf['SALE DATE'] // 10000
            ddf = ddf[ddf['year'] != 0].reset_index(drop=True)
            pre_group_base = ['year']
        else:
            pre_group_base = []

        # 2. group by year, SITUS_STATE, FIPS
        #   2-1. count cases
        #   2-2. sum "SALE AMOUNT"
        
        grouped_results = {
            "FIPS": None, 
            "SITUS_STATE": None
        }

        for c in grouped_results.keys():
            group_base = pre_group_base + [c]

            to_cat = [
                ddf.groupby(group_base)\
                    .agg(
                        case_cnt=('SALE DATE', 'count'),
                        sale_amt=('SALE AMOUNT', 'sum')
                    )\
                    .compute(),
                # since the following is a series, no need to specify column when renaming
                ddf.groupby(group_base)['SELLER NAME1']\
                    .nunique()\
                    .rename('uniq_seller_cnt')\
                    .compute()
            ]
            
            # for computing Herfindahl-Hirschman Index
            full_list = ddf.groupby(group_base + ['SELLER NAME1'])\
                .agg(
                    case_cnt=('SALE DATE', 'count'),
                    sale_amt=('SALE AMOUNT', 'sum')
                )\
                .reset_index()\
                .compute()
            
            TOP_N = 50 # defined in HHI
            # since 'full_list' is not large, using apply is reasonable
            # top 50 case_cnt and sale_amt
            # NOTE: grp means the groups during the group by process, 
            #       we can treat each of them as a pd.series and apply operations accordingly
            to_cat.extend([
                full_list.groupby(group_base)['case_cnt']\
                    .apply(lambda grp: grp.nlargest(TOP_N).sum())\
                    .rename(f'top_{TOP_N}_case_cnt'),
                full_list.groupby(group_base)['sale_amt']\
                    .apply(lambda grp: grp.nlargest(TOP_N).sum())\
                    .rename(f'top_{TOP_N}_sale_amt')
            ])
            # HHI = sum((x_i/X)^2), notice the base is top 50
            to_cat.extend([
                full_list.groupby(group_base)['case_cnt']\
                    .apply(
                        lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
                    )\
                    .rename('HHI_case_cnt'),
                full_list.groupby(group_base)['sale_amt']\
                    .apply(
                        lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
                    )\
                    .rename('HHI_sale_amt')
            ])
            
            # cat horizontally
            grouped_results[c] = pd.concat(to_cat, axis=1).reset_index()\
                .sort_values(by=group_base, ascending=[True] * len(group_base))
            
        if gen_data:
            for c, res in grouped_results.items():
                fname = f"agg_result_{c}.csv"
                if is_yearly:
                    fname = 'yearly_' + fname
                self.file_out(df=res, filename=fname)
        else:
            return "maybe return the dataframe?"
        return

    
    def deed_plot(self, filenames, save_fig=False, hhi_base='sale_amt'):
        '''
        REF: https://dev.to/oscarleo/how-to-create-data-maps-of-the-united-states-with-matplotlib-p9i
        plot the non yearly result on maps
        1. map points?
        2. current only has abbrev. of states

        TODO: need to change this to be able to plot both state and counties

        hhi_base: 'sale_amt' or 'case_cnt'
        '''
        if not hhi_base in ['sale_amt', 'case_cnt']:
            print("Please provide a valid HHI base, either sale_amt or case_cnt")
            return

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
            f = self.__out_path + f
            HHI = pd.read_csv(f)
            cur_hhi = 'HHI_sale_amt' if hhi_base == 'sale_amt' else 'HHI_case_cnt'
            keep_cols = ['SITUS_STATE', cur_hhi, 'case_cnt']
            HHI = HHI[keep_cols]
            HHI.rename(
                columns={
                    'SITUS_STATE': 'STUSPS',
                    cur_hhi: 'value'
                }, 
                inplace=True
            )

        counties = gpd.read_file("data/cb_2018_us_county_500k/")
        counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
        counties = counties.set_index("GEOID")

        states = gpd.read_file("data/cb_2018_us_state_500k/")
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
        low_case_cnt_color = "grey"
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

        # if 'case_cnt' is smaller than threshold, we set its color to 'low_case_cnt_color'
        threshold = 30
        states.loc[states['case_cnt'] < threshold, 'color'] = low_case_cnt_color

        sizex = 8
        sizey = 6
        ax = counties.plot(edgecolor=edge_color + "55", color="None", figsize=(sizex, sizey))
        states.plot(ax=ax, edgecolor=edge_color, color=states.color, linewidth=1)

        plt.axis("off")

        if save_fig:
            pass
            # do the saving thing
        else:
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

    # =========
    #  Testing
    # =========
    a.deed_analysis()

    # =====================================
    #  Generates the HHI data for plotting
    # =====================================
    # a.deed_prep(is_yearly=True, gen_data=True)
    # a.deed_prep(is_yearly=False, gen_data=True)

    # ==========
    #  plotting
    # ==========
    # TODO: check 那些太少的州真的在地圖上對的到嗎 & 那些太少的州叫啥名
    files = ['agg_result_SITUS_STATE.csv'] # 'agg_result_FIPS.csv'
    a.deed_plot(files, save_fig=False, hhi_base='sale_amt')

    # ===============
    #  Wild Thoughts
    # ===============
    # so I'm thinking maybe we could do the animation of yearly HHI heatmap
    # change since some 1970s?

if __name__ == "__main__":
    main()