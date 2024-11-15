import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import os
import platform
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
import warnings
warnings.filterwarnings("ignore")

CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
EXT_DISK += "Homebuilder/2016_files/"
STACKED_PATH = EXT_DISK + "processed_data/"

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
    def __init__(self, out_path: str = "./output/", hhi_base='sale_amt') -> None:
        self.__out_path = out_path
        self.hhi_base = hhi_base
        self.threshold = 20
        self.sizex = 16
        self.sizey = 12

        if not hhi_base in ['sale_amt', 'case_cnt']:
            print("Please provide a valid HHI base, either 'sale_amt' or 'case_cnt'")
            # maybe trigger some error here, just find a way to terminate the code.

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

    def data_prep(self, data):
        '''

        '''

        # Count non-null values per column
        resale_non_null_counts = data[data['RESALE/NEW_CONSTRUCTION'] == 'M'].count().compute()
        new_non_null_counts = data[data['RESALE/NEW_CONSTRUCTION'] == 'N'].count().compute()
        print("Resale", resale_non_null_counts)
        print("New", new_non_null_counts)

    def __archive_deed_prep(self, is_yearly=True, gen_data=False, period=[], scale=""):
        '''
        This method would prepare a deed dataset from the stacked csv for analysis.
        Specifically, this data is stacked data grouped by year, state, fips.
        And the value columns are the sum of sale amount and case count.

        Parameters
        ----------
        is_yearly:
        gen_data:
        period: list.
            if not porvided would use all the years available.
        scale: str.
            can be "states", "counties", or "" (will include both)

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
        #   1.2 make the STATEFP column, since there are some typo of SITUS_STATE
        #   1.3 fill the 'SALE DATE' when 0 with 'RECORDING DATE'
        #   1.4 make the 'year' column
        #       (opt.) ignore the rows with 'year' == 0,
        #              should be around 5831 after combining SALE and RECORDING
        #   1.5 (opt.) when period provided, filter the data to contain only the period

        ddf = ddf.dropna(subset=cols_to_clean)

        ddf['STATEFP'] = ddf['FIPS'].astype(str).str.zfill(5).str[:2]

        ddf['SALE DATE'] = ddf['SALE DATE'].mask(ddf['SALE DATE'] == 0, ddf['RECORDING DATE'])

        ddf['year'] = ddf['SALE DATE'] // 10000
        ddf = ddf[ddf['year'] != 0].reset_index(drop=True)
        if is_yearly:
            pre_group_base = ['year']
        else:
            pre_group_base = []

        if period:
            s, e = period
            ddf = ddf[ddf['year'].between(s, e)]

        # 2. group by year, STATEFP, FIPS
        #   2-1. count cases
        #   2-2. sum "SALE AMOUNT"

        if not scale:
            grouped_results = {
                "FIPS": None,
                "STATEFP": None
            }
        else:
            scale_col = "FIPS" if scale == 'counties' else "STATEFP"
            grouped_results = {scale_col: None}

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

            TOP_N = 50 # defined by HHI
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

            # concat horizontally
            grouped_results[c] = pd.concat(to_cat, axis=1).reset_index()\
                .sort_values(by=group_base, ascending=[True] * len(group_base))

            # give the states their abbreviation
            if c == 'STATEFP':
                states = gpd.read_file("data/cb_2018_us_state_500k/")
                states = states[['STUSPS', 'STATEFP']]

                tmp = pd.merge(grouped_results[c], states, on='STATEFP', how='left')
                tmp = tmp[['STUSPS'] + [col for col in tmp.columns if col != 'STUSPS']]
                if is_yearly:
                    tmp = tmp[['year'] + [col for col in tmp.columns if col != 'year']]

                grouped_results[c] = tmp
                # 這裡可以考慮把grouped_results 裡的 'STATEFP' 刪掉換成 'STUSPS'
                # (for consistancy with other functions)

        if gen_data:
            for c, res in grouped_results.items():
                fname = f"agg_result_{c}.csv"
                if is_yearly:
                    fname = 'yearly_' + fname
                self.file_out(df=res, filename=fname)
        else:
            if not scale:
                return grouped_results # this is a dict of dfs
            return grouped_results['FIPS'] if scale == 'counties' else grouped_results['STATEFP']
        return

    def deed_plot_heat_map(self, filename, scale='states', save_fig=False):
        '''
        REF: https://dev.to/oscarleo/how-to-create-data-maps-of-the-united-states-with-matplotlib-p9i
        plot the non yearly result on maps
        1. map points?
        2. current only has abbrev. of states

        TODO: need to change this to be able to plot both state and counties
        '''
        if not scale in ['states', 'counties']:
            print("Please state a valid scale, either 'states' or 'counties'")
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

        f = self.__out_path + filename # TODO: should change the way of writing this
        HHI = pd.read_csv(f) # or maybe we can combine the scale indicator with the filename

        cur_hhi = 'HHI_sale_amt' if self.hhi_base == 'sale_amt' else 'HHI_case_cnt'
        cur_scale = 'STUSPS' if scale == 'states' else 'FIPS'
        scale_new_name = 'STUSPS' if scale == 'states' else 'GEOID'

        keep_cols = [cur_scale, cur_hhi, 'case_cnt']
        HHI = HHI[keep_cols]
        HHI.rename(
            columns={
                cur_scale: scale_new_name,
                cur_hhi: 'HHI'
            },
            inplace=True
        )

        counties = gpd.read_file("data/cb_2018_us_county_500k/")
        counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
        counties["GEOID"] = counties["GEOID"].astype(int)

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
        low_case_cnt_color = "lightgrey"
        data_breaks = [
            (90, "#ff0000", "Top 10%"),   # Bright Red
            (70, "#ff4d4d", "90-70%"),    # Light Red
            (50, "#ff9999", "70-50%"),    # Lighter Red
            (30, "#ffcccc", "50-30%"),    # Pale Red
            (0,  "#ffe6e6", "Bottom 30%") # Very Light Red
        ]

        def create_color(county_df, data_breaks, target):
            colors = []

            for i, row in county_df.iterrows():
                for p, c, _ in data_breaks:
                    if row[target] >= np.percentile(county_df[target], p):
                        colors.append(c)
                        break

            return colors

        cur_geo = states if scale == 'states' else counties

        to_plot = pd.merge(cur_geo, HHI, on=scale_new_name, how='left')
        # since there will be some counties that has no HHI data, we fill them with 0
        to_plot[['HHI', 'case_cnt']] = to_plot[['HHI', 'case_cnt']].fillna(0)
        to_plot.loc[:, "color"] = create_color(to_plot, data_breaks, target='HHI')

        # if 'case_cnt' is smaller than threshold, we set its color to 'low_case_cnt_color'
        cur_thresh = self.threshold
        to_plot.loc[to_plot['case_cnt'] < cur_thresh, 'color'] = low_case_cnt_color

        ax = counties.plot(edgecolor=edge_color + "55", color="None", figsize=(self.sizex, self.sizey))
        to_plot.plot(ax=ax, edgecolor=edge_color, color=to_plot.color, linewidth=0.5)

        plt.axis("off")

        if save_fig:
            plt.savefig(f"{self.__out_path+scale}.svg", format="svg")
        else:
            plt.show()
        return

    def deed_plot_time_series(self, filename, save_fig=False):
        '''
        This function only considers plotting for all the states,
        since including all the counties in one plot would be chaos.

        Also this function might also support plotting for a given state or county.
        (the corresponding data shold be provided though)
        '''
        f = self.__out_path + filename
        df = pd.read_csv(f)

        cur_tresh = self.threshold
        df = df[df['year'] >= 1987] # since 1987 差不多HHI趨於穩定，也是FRED資料庫S&P CoreLogic Case-Shiller U.S. National Home Price Index 的起始點
        df = df[df['case_cnt'] >= cur_tresh] # 把一些量太少的年份/row踢掉
        cur_hhi = 'HHI_sale_amt' if self.hhi_base == 'sale_amt' else 'HHI_case_cnt'

        # Define the full range of years expected in the data
        full_years = np.arange(df['year'].min(), df['year'].max() + 1)

        fig, ax = plt.subplots(figsize=(self.sizex, self.sizey))

        n_color = len(set(df['STUSPS']))
        palette = sns.color_palette("icefire", n_colors=n_color)  # distinct colors
        state_colors = {state: palette[i] for i, state in enumerate(df['STUSPS'].unique())}

        # Group by state and plot each group separately
        for state, group in df.groupby('STUSPS'):
            # Reindex to include all years, filling missing HHI with NaN
            group = group.set_index('year').reindex(full_years).reset_index()
            group['state'] = state  # Refill the state column
            group[cur_hhi] = group[cur_hhi].interpolate()  # Interpolate missing HHI values (dropped in previous step)

            # Plot the reindexed and interpolated data
            ax.plot(group['year'], group[cur_hhi], label=state, marker='o', color=state_colors[state])

        # Add plot details
        ax.set_xlabel('Year')
        ax.set_ylabel('HHI')
        ax.set_title('HHI Over Time by State')
        ax.legend(title='State', loc='upper right', ncol=3, fontsize='small', frameon=True)  # Place legend outside
        plt.grid()

        if save_fig:
            plt.savefig(f"{self.__out_path}state_time_series.svg", format="svg")
        else:
            plt.show()
        return

    def deed_plot_scatter(self, period, filename, cur_index="HHI", filter_outlier=False, save_fig=False):
        '''
        This function plots the scatter plot of HHI, with x-axis being the
        county-level HHI and the y-axis being the house price change.

        Currently, since we have only the aggregated case count and sale amt
        for counties, we assume all the properties have the same size, and use
        the average sale value as the housing price for each county.
        Still, we'll ignore the data with limited case count to avoided biased HHI.

        Parameters
        ----------
        period: [start_year, end_year], list of int
            specify the time period, where the HHI will be calculated with the data
            in this range, and the price change will be the pct change from start_year
            to end_year.
        cur_index: "HHI" or "ENI", str
            HHI is what we will compute, and ENI is the reciprocal of HHI.
        '''
        # us_hpi = pd.read_csv("data/USSTHPI.csv")

        s, e = min(period), max(period)
        hhi_col = f'HHI_{self.hhi_base}'
        if 'FIPS' in filename:
            cur_scale = 'counties'
            cur_scale_col = 'FIPS'
        else:
            cur_scale = 'states'
            cur_scale_col = 'STUSPS'

        # if the file exist then read it, otherwise run the code below
        file_for_plot = self.__out_path + f'for_scatter_{s}_{e}.csv'
        if os.path.exists(file_for_plot):
            # If the file exists, read it
            sub_df = pd.read_csv(file_for_plot)
            print("File found. Data loaded from file.")
        else:
            df = pd.read_csv(self.__out_path+filename)

            # 1. make the county/state column
            sub_df = pd.DataFrame(sorted(set(df[cur_scale_col])), columns=[cur_scale_col])

            # 2. do the avg price calculation, and append the year data to the sub_df
            cols = [cur_scale_col, 'case_cnt', 'sale_amt']
            for y in period:
                tmp = df.loc[df['year'] == y, cols].reset_index(drop=True)
                tmp['avg_price'] = tmp['sale_amt'] / tmp['case_cnt']
                new_colname = [f"{c}_{y}" for c in tmp.columns if c != cur_scale_col]
                tmp.columns = [cur_scale_col] + new_colname
                sub_df = pd.merge(sub_df, tmp, on=cur_scale_col, how='left')

            # 3. make the HHI for the designated period
            #    (if takes too long, the deed_prep step can output a csv and read it when future call)
            period_hhi = self.deed_prep(
                is_yearly=False, gen_data=False, period=period, scale=cur_scale
            )
            cur_thresh = self.threshold
            period_hhi = period_hhi.loc[period_hhi['case_cnt'] >= cur_thresh, [cur_scale_col, hhi_col]]
            # if wish to use 'ENI', take the reciprocal of HHI
            if cur_index == 'ENI':
                period_hhi[hhi_col] = 1 / period_hhi[hhi_col]
            sub_df = pd.merge(sub_df, period_hhi, on=cur_scale_col, how='left')

            # 4. clean the data: by case count & if there are any empty value.
            sub_df = sub_df.dropna().reset_index(drop=True)

            # 5. calculate the price_chg%
            sub_df['price_chg'] = (sub_df[f'avg_price_{e}'] / sub_df[f'avg_price_{s}']) - 1

            # And generate the file for future use
            sub_df.to_csv(file_for_plot, index=False)

        # Filter outliers, currently using the interquartile range, IQR, method
        # can consider other method like z-score, percentage, or std dev.
        fo_remark = ""
        if filter_outlier:
            fo_remark = '_fo'
            Q1 = sub_df['price_chg'].quantile(0.25)
            Q3 = sub_df['price_chg'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            # Filter out the outliers
            sub_df = sub_df[(sub_df['price_chg'] >= lower_bound) & (sub_df['price_chg'] <= upper_bound)]

        # 6. plot the percetage change with HHI as the scatter plot,
        #    can consider add legend
        plt.figure(figsize=(self.sizex, self.sizey))
        plt.scatter(sub_df[hhi_col], sub_df['price_chg'], color='blue', edgecolor='k', alpha=0.5)

        # Adding labels and title
        plt.xlabel(cur_index)
        plt.ylabel('Price Change (%)')
        plt.title(f'Scatter Plot of {cur_index} vs. Price Change')
        plt.grid(True)

        if save_fig:
            plt.savefig(f"{self.__out_path}{cur_scale}_{cur_index}_scatter_{s}_{e}{fo_remark}.svg", format="svg")
        else:
            plt.show()


        return

    def make_hhi_panel(self, filename, gen_data=False):
        '''
        The data is generated by this: deed_prep(is_yearly=True, gen_data=True)
        '''
        if 'FIPS' in filename:
            cur_scale = 'counties'
            cur_scale_col = 'FIPS'
        else:
            cur_scale = 'states'
            cur_scale_col = 'STUSPS'

        df = pd.read_csv(self.__out_path+filename)

        start_year = 1999 # since using 1987 would eliminate too many entries
        end_year = max(df['year'])
        cur_thresh = self.threshold

        # 0. first filter the data:
        #    0-1. year >= start_year
        #    0-2. case_cnt >= 20 for all entries
        #    0-3. need the presence of all years
        df = df[df['year'] >= start_year].reset_index(drop=True)
        df = df[df['case_cnt'] >= cur_thresh].reset_index(drop=True)

        # 1. do operations for all rows: take reciprocal of HHI & avg. price
        df['avg_price'] = df['sale_amt'] / df['case_cnt']
        df['HHI'] = df[f'HHI_{self.hhi_base}']
        df['ENI'] = 1 / df['HHI']

        # 2. filter the counties/states out (year already sorted in the csv file)
        #    and do the operation on filtered row: compute the yoy
        regions_to_cat = []
        for region in set(df[cur_scale_col]):
            cond = df[cur_scale_col] == region
            tmp = df.loc[cond]

            tmp['price_chg_pct'] = tmp['avg_price'].pct_change()

            if set(list(range(start_year, end_year + 1))).issubset(set(tmp['year'])):
                # have to dropna here, or the first row will be dropped and no region could meet the if's cond
                regions_to_cat.append(tmp.dropna())

        # cat vertically
        df = pd.concat(regions_to_cat, axis=0).reset_index(drop=True)

        cols_to_keep = [
            'year', 'FIPS', 'case_cnt', 'sale_amt', 'avg_price',
            'price_chg_pct', 'HHI', 'ENI'
        ]
        df = df[cols_to_keep]

        if gen_data:
            out_file = self.__out_path + f"{cur_scale}_panel_data.csv"
            df.to_csv(out_file, index=False)
        else:
            print(df)

    def file_out(self, df, filename: str) -> None:
        # TODO: filename validity check
        # TODO: kwargs for to_csv

        print(f"Generating {filename} ...")

        filename = self.__out_path + filename
        df.to_csv(filename, index=False)

        print("DONE")


def main():
    client = Client(n_workers=1, threads_per_worker=6, memory_limit='10GB')

    a = Analysis()

    # ================
    #  Basic Cleaning
    # ================
    types_spec = {
        "FIPS": 'category',
        "APN_UNFORMATTED": 'str',
        "BATCH-ID": 'str', # in the form of yyyymmdd, but not the same as sale date
        "BATCH-SEQ": 'str',
        "SELLER_NAME1": 'str',
        "SALE_DATE": 'str',
        "SALE_AMOUNT": 'str',
        "LAND_USE": 'category',
        "PROPERTY_INDICATOR": 'category',
        "RESALE/NEW_CONSTRUCTION": 'category', # M: re-sale, N: new construction
        "BUYER_NAME_1": 'str',
        "LAND_SQUARE_FOOTAGE": 'str',
        "UNIVERSAL_BUILDING_SQUARE_FEET": 'str',
        "BUILDING_SQUARE_FEET_INDICATOR_CODE": 'category', # A: adjusted; B: building; G: gross; H: heated; L: living; M: main; R: ground floor
        "BUILDING_SQUARE_FEET": 'str', # doesn't differentiate living and non-living (if lack indicator code)
        "LIVING_SQUARE_FEET": 'str',
    }

    ddf = dd.read_csv(
        STACKED_PATH+"merged_stacked.csv",
        blocksize='300MB',
        usecols=list(types_spec.keys()), # need to be subscriptable (use [0] to access elms)
        dtype=types_spec
    )

    to_num = [
        "SALE_AMOUNT", "LAND_SQUARE_FOOTAGE", "UNIVERSAL_BUILDING_SQUARE_FEET",
        "BUILDING_SQUARE_FEET", "LIVING_SQUARE_FEET"
    ]

    for col in to_num:
        # if not number, turn to NaN
        ddf[col] = dd.to_numeric(ddf[col], errors='coerce')

    # =====================================
    #  Generates the HHI data for plotting
    # =====================================
    a.data_prep(data=ddf)

    client.close()

    # ==========
    #  Plotting
    # ==========
    # 1. draw heat map
    tb_list = {
        'states': 'agg_result_STATEFP.csv',
        'counties': 'agg_result_FIPS.csv'
    }
    # for s, file in tb_list.items():
    #     a.deed_plot_heat_map(file, save_fig=True, hhi_base='sale_amt', scale=s)

    # 2. draw time series of states from 1987
    file = 'yearly_agg_result_STATEFP.csv'
    # a.deed_plot_time_series(file, hhi_base='sale_amt', save_fig=True)

    # 3. scatter plot of HHI (x-axis) and price change (y-axis)
    # change rate from 2006 to 2012 and 2012 to 2020
    file = "yearly_agg_result_FIPS.csv"
    # a.deed_plot_scatter(period=[2006, 2012], cur_index='ENI', filename=file, save_fig=False, filter_outlier=False)
    # a.deed_plot_scatter(period=[2012, 2016], cur_index='ENI', filename=file, save_fig=False, filter_outlier=False)

    # 4. make the panel data (make yearly data to include price change)
    file = "yearly_agg_result_FIPS.csv"
    # a.make_hhi_panel(file, gen_data=True)

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