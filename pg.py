import platform
import os
import pandas as pd
from rapidfuzz import process, fuzz
import re

CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
VAR_PATH = EXT_DISK + "Homebuilder/Variables/"
EXT_DISK += "Homebuilder/2016_files/"
COUNTY_FILE_PATH = EXT_DISK + "cleaned_by_county/"

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


def clean_firm_names(data):
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

def old_tests():
    types_spec = {
        "SELLER_NAME1": 'str',
        "SALE_AMOUNT": 'str',
        "PROPERTY_INDICATOR": 'category',
        "RESALE/NEW_CONSTRUCTION": 'category' # M: re-sale, N: new construction
    }

    df = pd.read_csv(
        f"{COUNTY_FILE_PATH}05069.csv",
        usecols=types_spec.keys(),
        dtype=types_spec
    )

    df = df[df['RESALE/NEW_CONSTRUCTION'] == 'N']

    df[COL_SALE_AMT] = pd.to_numeric(df[COL_SALE_AMT], errors='coerce')

    df = clean_firm_names(df)

    grouped_df = df.groupby(COL_SELLER).agg(
        SALE_AMOUNT=(COL_SALE_AMT, 'sum'),
        CASE_CNT=(COL_SELLER, 'count')
    ).reset_index()

    print(grouped_df)

    # 1. Get total case count (or simply df.shape[0] works)
    ttl_case_cnt = sum(grouped_df[COL_CASE_CNT])

    # 2. HHI = sum((x_i/X)^2), notice the base is top 50
    TOP_N = 50
    hhi = grouped_df[COL_SALE_AMT].nlargest(TOP_N) / \
        grouped_df[COL_SALE_AMT].sum()

    hhi = hhi ** 2
    hhi = sum(hhi)

    print(hhi)

def test_clip_apn_fill():
    # Sample DataFrame
    df = pd.DataFrame({
        'A': [None, 'X1', 'Y2', 'X2', 'X3', None, 'X2'],
        'B': ['B1', 'B1', 'B2', 'B2', 'B3', 'B3', 'B2']
    })

    # Fill missing values in column A based on B
    df['A'] = df['A'].fillna(df.groupby('B')['A'].transform('first'))

    print(df)


if __name__ == "__main__":
    test_clip_apn_fill()

# class Analysis():
#     def __init__(self, out_path: str) -> None:
#         self.__out_path = out_path
#         self.threshold = 20
#         self.sizex = 16
#         self.sizey = 12

#         if not os.path.isdir(out_path):
#             os.mkdir(out_path)

#     def __archive_deed_prep(self, is_yearly=True, gen_data=False, period=[], scale=""):
#         '''
#         This method would prepare a deed dataset from the stacked csv for analysis.
#         Specifically, this data is stacked data grouped by year, state, fips.
#         And the value columns are the sum of sale amount and case count.

#         Parameters
#         ----------
#         is_yearly:
#         gen_data:
#         period: list.
#             if not porvided would use all the years available.
#         scale: str.
#             can be "states", "counties", or "" (will include both)

#         Notice:
#         1. we do not calculate the HHI here, since we will calculate HHI for
#            years seperated and aggregated.
#         2. the unique seller count is a little hard to present in this dataset
#            因為在同state不同county中也可能有同一個seller, 以目前資料設計來看, 這樣依state加總就會失真
#         '''
#         ddf = dd.read_csv('data/deed_stacked.csv')

#         cols_to_clean = [
#             "FIPS", # = counties
#             "SITUS_STATE", # eg. CA
#             "SELLER NAME1",
#             "SALE AMOUNT",
#             "RECORDING DATE", # yyyymmdd, int64
#             "SALE DATE" # yyyymmdd, int64
#         ]

#         ddf = ddf[cols_to_clean]

#         # 1. data prep
#         #   1.1 ignore rows if any of those columns is empty
#         #   1.2 make the STATEFP column, since there are some typo of SITUS_STATE
#         #   1.3 fill the 'SALE DATE' when 0 with 'RECORDING DATE'
#         #   1.4 make the 'year' column
#         #       (opt.) ignore the rows with 'year' == 0,
#         #              should be around 5831 after combining SALE and RECORDING
#         #   1.5 (opt.) when period provided, filter the data to contain only the period

#         ddf = ddf.dropna(subset=cols_to_clean)

#         ddf['STATEFP'] = ddf['FIPS'].astype(str).str.zfill(5).str[:2]

#         ddf['SALE DATE'] = ddf['SALE DATE'].mask(ddf['SALE DATE'] == 0, ddf['RECORDING DATE'])

#         ddf['year'] = ddf['SALE DATE'] // 10000
#         ddf = ddf[ddf['year'] != 0].reset_index(drop=True)
#         if is_yearly:
#             pre_group_base = ['year']
#         else:
#             pre_group_base = []

#         if period:
#             s, e = period
#             ddf = ddf[ddf['year'].between(s, e)]

#         # 2. group by year, STATEFP, FIPS
#         #   2-1. count cases
#         #   2-2. sum "SALE AMOUNT"

#         if not scale:
#             grouped_results = {
#                 "FIPS": None,
#                 "STATEFP": None
#             }
#         else:
#             scale_col = "FIPS" if scale == 'counties' else "STATEFP"
#             grouped_results = {scale_col: None}

#         for c in grouped_results.keys():
#             group_base = pre_group_base + [c]

#             to_cat = [
#                 ddf.groupby(group_base)\
#                     .agg(
#                         case_cnt=('SALE DATE', 'count'),
#                         sale_amt=('SALE AMOUNT', 'sum')
#                     )\
#                     .compute(),
#                 # since the following is a series, no need to specify column when renaming
#                 ddf.groupby(group_base)['SELLER NAME1']\
#                     .nunique()\
#                     .rename('uniq_seller_cnt')\
#                     .compute()
#             ]

#             # for computing Herfindahl-Hirschman Index
#             full_list = ddf.groupby(group_base + ['SELLER NAME1'])\
#                 .agg(
#                     case_cnt=('SALE DATE', 'count'),
#                     sale_amt=('SALE AMOUNT', 'sum')
#                 )\
#                 .reset_index()\
#                 .compute()

#             TOP_N = 50 # defined by HHI
#             # since 'full_list' is not large, using apply is reasonable
#             # top 50 case_cnt and sale_amt
#             # NOTE: grp means the groups during the group by process,
#             #       we can treat each of them as a pd.series and apply operations accordingly
#             to_cat.extend([
#                 full_list.groupby(group_base)['case_cnt']\
#                     .apply(lambda grp: grp.nlargest(TOP_N).sum())\
#                     .rename(f'top_{TOP_N}_case_cnt'),
#                 full_list.groupby(group_base)['sale_amt']\
#                     .apply(lambda grp: grp.nlargest(TOP_N).sum())\
#                     .rename(f'top_{TOP_N}_sale_amt')
#             ])
#             # HHI = sum((x_i/X)^2), notice the base is top 50
#             to_cat.extend([
#                 full_list.groupby(group_base)['case_cnt']\
#                     .apply(
#                         lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
#                     )\
#                     .rename('HHI_case_cnt'),
#                 full_list.groupby(group_base)['sale_amt']\
#                     .apply(
#                         lambda grp: sum((grp.nlargest(TOP_N) / grp.nlargest(TOP_N).sum()) ** 2)
#                     )\
#                     .rename('HHI_sale_amt')
#             ])

#             # concat horizontally
#             grouped_results[c] = pd.concat(to_cat, axis=1).reset_index()\
#                 .sort_values(by=group_base, ascending=[True] * len(group_base))

#             # give the states their abbreviation
#             if c == 'STATEFP':
#                 states = gpd.read_file("data/cb_2018_us_state_500k/")
#                 states = states[['STUSPS', 'STATEFP']]

#                 tmp = pd.merge(grouped_results[c], states, on='STATEFP', how='left')
#                 tmp = tmp[['STUSPS'] + [col for col in tmp.columns if col != 'STUSPS']]
#                 if is_yearly:
#                     tmp = tmp[['year'] + [col for col in tmp.columns if col != 'year']]

#                 grouped_results[c] = tmp
#                 # 這裡可以考慮把grouped_results 裡的 'STATEFP' 刪掉換成 'STUSPS'
#                 # (for consistancy with other functions)

#         if gen_data:
#             for c, res in grouped_results.items():
#                 fname = f"agg_result_{c}.csv"
#                 if is_yearly:
#                     fname = 'yearly_' + fname
#                 self.file_out(df=res, filename=fname)
#         else:
#             if not scale:
#                 return grouped_results # this is a dict of dfs
#             return grouped_results['FIPS'] if scale == 'counties' else grouped_results['STATEFP']
#         return

#     def __archive_deed_plot_heat_map(self, filename, scale='states', save_fig=False):
#         '''
#         REF: https://dev.to/oscarleo/how-to-create-data-maps-of-the-united-states-with-matplotlib-p9i
#         plot the non yearly result on maps
#         1. map points?
#         2. current only has abbrev. of states

#         TODO: need to change this to be able to plot both state and counties
#         '''
#         if not scale in ['states', 'counties']:
#             print("Please state a valid scale, either 'states' or 'counties'")
#             return

#         def translate_geometries(df, x, y, scale, rotate):
#             df.loc[:, "geometry"] = df.geometry.translate(yoff=y, xoff=x)
#             center = df.dissolve().centroid.iloc[0]
#             df.loc[:, "geometry"] = df.geometry.scale(xfact=scale, yfact=scale, origin=center)
#             df.loc[:, "geometry"] = df.geometry.rotate(rotate, origin=center)
#             return df

#         def adjust_maps(df):
#             df_main_land = df[~df.STATEFP.isin(["02", "15"])]
#             df_alaska = df[df.STATEFP == "02"]
#             df_hawaii = df[df.STATEFP == "15"]

#             df_alaska = translate_geometries(df_alaska, 1300000, -4900000, 0.5, 32)
#             df_hawaii = translate_geometries(df_hawaii, 5400000, -1500000, 1, 24)

#             return pd.concat([df_main_land, df_alaska, df_hawaii])

#         f = self.__out_path + filename # TODO: should change the way of writing this
#         HHI = pd.read_csv(f) # or maybe we can combine the scale indicator with the filename

#         cur_hhi = 'HHI_sale_amt'
#         cur_scale = 'STUSPS' if scale == 'states' else 'FIPS'
#         scale_new_name = 'STUSPS' if scale == 'states' else 'GEOID'

#         keep_cols = [cur_scale, cur_hhi, 'case_cnt']
#         HHI = HHI[keep_cols]
#         HHI.rename(
#             columns={
#                 cur_scale: scale_new_name,
#                 cur_hhi: 'HHI'
#             },
#             inplace=True
#         )

#         counties = gpd.read_file("data/cb_2018_us_county_500k/")
#         counties = counties[~counties.STATEFP.isin(["72", "69", "60", "66", "78"])]
#         counties["GEOID"] = counties["GEOID"].astype(int)

#         states = gpd.read_file("data/cb_2018_us_state_500k/")
#         # remove "unincorporated territories":
#         # "Puerto Rico", "American Samoa", "United States Virgin Islands"
#         # "Guam", "Commonwealth of the Northern Mariana Islands"
#         states = states[~states.STATEFP.isin(["72", "69", "60", "66", "78"])]

#         # map projection to be centered on United States.
#         counties = counties.to_crs("ESRI:102003")
#         states = states.to_crs("ESRI:102003")

#         # place Alaska and Hawaii on the bottom left side on the graph.
#         counties = adjust_maps(counties)
#         states = adjust_maps(states)

#         # add data with color to the states df.
#         low_case_cnt_color = "lightgrey"
#         data_breaks = [
#             (90, "#ff0000", "Top 10%"),   # Bright Red
#             (70, "#ff4d4d", "90-70%"),    # Light Red
#             (50, "#ff9999", "70-50%"),    # Lighter Red
#             (30, "#ffcccc", "50-30%"),    # Pale Red
#             (0,  "#ffe6e6", "Bottom 30%") # Very Light Red
#         ]

#         def create_color(county_df, data_breaks, target):
#             colors = []

#             for i, row in county_df.iterrows():
#                 for p, c, _ in data_breaks:
#                     if row[target] >= np.percentile(county_df[target], p):
#                         colors.append(c)
#                         break

#             return colors

#         cur_geo = states if scale == 'states' else counties

#         to_plot = pd.merge(cur_geo, HHI, on=scale_new_name, how='left')
#         # since there will be some counties that has no HHI data, we fill them with 0
#         to_plot[['HHI', 'case_cnt']] = to_plot[['HHI', 'case_cnt']].fillna(0)
#         to_plot.loc[:, "color"] = create_color(to_plot, data_breaks, target='HHI')

#         # if 'case_cnt' is smaller than threshold, we set its color to 'low_case_cnt_color'
#         cur_thresh = self.threshold
#         to_plot.loc[to_plot['case_cnt'] < cur_thresh, 'color'] = low_case_cnt_color

#         ax = counties.plot(edgecolor=edge_color + "55", color="None", figsize=(self.sizex, self.sizey))
#         to_plot.plot(ax=ax, edgecolor=edge_color, color=to_plot.color, linewidth=0.5)

#         plt.axis("off")

#         if save_fig:
#             plt.savefig(f"{self.__out_path+scale}.svg", format="svg")
#         else:
#             plt.show()
#         return

#     def __archive_deed_plot_time_series(self, filename, save_fig=False):
#         '''
#         This function only considers plotting for all the states,
#         since including all the counties in one plot would be chaos.

#         Also this function might also support plotting for a given state or county.
#         (the corresponding data shold be provided though)
#         '''
#         f = self.__out_path + filename
#         df = pd.read_csv(f)

#         cur_tresh = self.threshold
#         df = df[df['year'] >= 1987] # since 1987 差不多HHI趨於穩定，也是FRED資料庫S&P CoreLogic Case-Shiller U.S. National Home Price Index 的起始點
#         df = df[df['case_cnt'] >= cur_tresh] # 把一些量太少的年份/row踢掉
#         cur_hhi = 'HHI_sale_amt'

#         # Define the full range of years expected in the data
#         full_years = np.arange(df['year'].min(), df['year'].max() + 1)

#         fig, ax = plt.subplots(figsize=(self.sizex, self.sizey))

#         n_color = len(set(df['STUSPS']))
#         palette = sns.color_palette("icefire", n_colors=n_color)  # distinct colors
#         state_colors = {state: palette[i] for i, state in enumerate(df['STUSPS'].unique())}

#         # Group by state and plot each group separately
#         for state, group in df.groupby('STUSPS'):
#             # Reindex to include all years, filling missing HHI with NaN
#             group = group.set_index('year').reindex(full_years).reset_index()
#             group['state'] = state  # Refill the state column
#             group[cur_hhi] = group[cur_hhi].interpolate()  # Interpolate missing HHI values (dropped in previous step)

#             # Plot the reindexed and interpolated data
#             ax.plot(group['year'], group[cur_hhi], label=state, marker='o', color=state_colors[state])

#         # Add plot details
#         ax.set_xlabel('Year')
#         ax.set_ylabel('HHI')
#         ax.set_title('HHI Over Time by State')
#         ax.legend(title='State', loc='upper right', ncol=3, fontsize='small', frameon=True)  # Place legend outside
#         plt.grid()

#         if save_fig:
#             plt.savefig(f"{self.__out_path}state_time_series.svg", format="svg")
#         else:
#             plt.show()
#         return

#     def __archive_deed_plot_scatter(self, period, filename, cur_index="HHI", filter_outlier=False, save_fig=False):
#         '''
#         This function plots the scatter plot of HHI, with x-axis being the
#         county-level HHI and the y-axis being the house price change.

#         Currently, since we have only the aggregated case count and sale amt
#         for counties, we assume all the properties have the same size, and use
#         the average sale value as the housing price for each county.
#         Still, we'll ignore the data with limited case count to avoided biased HHI.

#         Parameters
#         ----------
#         period: [start_year, end_year], list of int
#             specify the time period, where the HHI will be calculated with the data
#             in this range, and the price change will be the pct change from start_year
#             to end_year.
#         cur_index: "HHI" or "ENI", str
#             HHI is what we will compute, and ENI is the reciprocal of HHI.
#         '''
#         # us_hpi = pd.read_csv("data/USSTHPI.csv")

#         s, e = min(period), max(period)
#         hhi_col = 'HHI_sale_amt'
#         if 'FIPS' in filename:
#             cur_scale = 'counties'
#             cur_scale_col = 'FIPS'
#         else:
#             cur_scale = 'states'
#             cur_scale_col = 'STUSPS'

#         # if the file exist then read it, otherwise run the code below
#         file_for_plot = self.__out_path + f'for_scatter_{s}_{e}.csv'
#         if os.path.exists(file_for_plot):
#             # If the file exists, read it
#             sub_df = pd.read_csv(file_for_plot)
#             print("File found. Data loaded from file.")
#         else:
#             df = pd.read_csv(self.__out_path+filename)

#             # 1. make the county/state column
#             sub_df = pd.DataFrame(sorted(set(df[cur_scale_col])), columns=[cur_scale_col])

#             # 2. do the avg price calculation, and append the year data to the sub_df
#             cols = [cur_scale_col, 'case_cnt', 'sale_amt']
#             for y in period:
#                 tmp = df.loc[df['year'] == y, cols].reset_index(drop=True)
#                 tmp['avg_price'] = tmp['sale_amt'] / tmp['case_cnt']
#                 new_colname = [f"{c}_{y}" for c in tmp.columns if c != cur_scale_col]
#                 tmp.columns = [cur_scale_col] + new_colname
#                 sub_df = pd.merge(sub_df, tmp, on=cur_scale_col, how='left')

#             # 3. make the HHI for the designated period
#             #    (if takes too long, the deed_prep step can output a csv and read it when future call)
#             period_hhi = self.deed_prep(
#                 is_yearly=False, gen_data=False, period=period, scale=cur_scale
#             )
#             cur_thresh = self.threshold
#             period_hhi = period_hhi.loc[period_hhi['case_cnt'] >= cur_thresh, [cur_scale_col, hhi_col]]
#             # if wish to use 'ENI', take the reciprocal of HHI
#             if cur_index == 'ENI':
#                 period_hhi[hhi_col] = 1 / period_hhi[hhi_col]
#             sub_df = pd.merge(sub_df, period_hhi, on=cur_scale_col, how='left')

#             # 4. clean the data: by case count & if there are any empty value.
#             sub_df = sub_df.dropna().reset_index(drop=True)

#             # 5. calculate the price_chg%
#             sub_df['price_chg'] = (sub_df[f'avg_price_{e}'] / sub_df[f'avg_price_{s}']) - 1

#             # And generate the file for future use
#             sub_df.to_csv(file_for_plot, index=False)

#         # Filter outliers, currently using the interquartile range, IQR, method
#         # can consider other method like z-score, percentage, or std dev.
#         fo_remark = ""
#         if filter_outlier:
#             fo_remark = '_fo'
#             Q1 = sub_df['price_chg'].quantile(0.25)
#             Q3 = sub_df['price_chg'].quantile(0.75)
#             IQR = Q3 - Q1
#             lower_bound = Q1 - 1.5 * IQR
#             upper_bound = Q3 + 1.5 * IQR
#             # Filter out the outliers
#             sub_df = sub_df[(sub_df['price_chg'] >= lower_bound) & (sub_df['price_chg'] <= upper_bound)]

#         # 6. plot the percetage change with HHI as the scatter plot,
#         #    can consider add legend
#         plt.figure(figsize=(self.sizex, self.sizey))
#         plt.scatter(sub_df[hhi_col], sub_df['price_chg'], color='blue', edgecolor='k', alpha=0.5)

#         # Adding labels and title
#         plt.xlabel(cur_index)
#         plt.ylabel('Price Change (%)')
#         plt.title(f'Scatter Plot of {cur_index} vs. Price Change')
#         plt.grid(True)

#         if save_fig:
#             plt.savefig(f"{self.__out_path}{cur_scale}_{cur_index}_scatter_{s}_{e}{fo_remark}.svg", format="svg")
#         else:
#             plt.show()


#         return

#     def __archive_make_hhi_panel(self, filename, gen_data=False):
#         '''
#         The data is generated by this: deed_prep(is_yearly=True, gen_data=True)
#         '''
#         if 'FIPS' in filename:
#             cur_scale = 'counties'
#             cur_scale_col = 'FIPS'
#         else:
#             cur_scale = 'states'
#             cur_scale_col = 'STUSPS'

#         df = pd.read_csv(self.__out_path+filename)

#         start_year = 1999 # since using 1987 would eliminate too many entries
#         end_year = max(df['year'])
#         cur_thresh = self.threshold

#         # 0. first filter the data:
#         #    0-1. year >= start_year
#         #    0-2. case_cnt >= 20 for all entries
#         #    0-3. need the presence of all years
#         df = df[df['year'] >= start_year].reset_index(drop=True)
#         df = df[df['case_cnt'] >= cur_thresh].reset_index(drop=True)

#         # 1. do operations for all rows: take reciprocal of HHI & avg. price
#         df['avg_price'] = df['sale_amt'] / df['case_cnt']
#         df['HHI'] = df['HHI_sale_amt']
#         df['ENI'] = 1 / df['HHI']

#         # 2. filter the counties/states out (year already sorted in the csv file)
#         #    and do the operation on filtered row: compute the yoy
#         regions_to_cat = []
#         for region in set(df[cur_scale_col]):
#             cond = df[cur_scale_col] == region
#             tmp = df.loc[cond]

#             tmp['price_chg_pct'] = tmp['avg_price'].pct_change()

#             if set(list(range(start_year, end_year + 1))).issubset(set(tmp['year'])):
#                 # have to dropna here, or the first row will be dropped and no region could meet the if's cond
#                 regions_to_cat.append(tmp.dropna())

#         # cat vertically
#         df = pd.concat(regions_to_cat, axis=0).reset_index(drop=True)

#         cols_to_keep = [
#             'year', 'FIPS', 'case_cnt', 'sale_amt', 'avg_price',
#             'price_chg_pct', 'HHI', 'ENI'
#         ]
#         df = df[cols_to_keep]

#         if gen_data:
#             out_file = self.__out_path + f"{cur_scale}_panel_data.csv"
#             df.to_csv(out_file, index=False)
#         else:
#             print(df)

#     def __archive_file_out(self, df, filename: str) -> None:
#         # TODO: filename validity check
#         # TODO: kwargs for to_csv

#         print(f"Generating {filename} ...")

#         filename = self.__out_path + filename
#         df.to_csv(filename, index=False)

#         print("DONE")

#     # ==========
#     #  Plotting
#     # ==========
#     # 1. draw heat map
#     tb_list = {
#         'states': 'agg_result_STATEFP.csv',
#         'counties': 'agg_result_FIPS.csv'
#     }
#     # for s, file in tb_list.items():
#     #     a.deed_plot_heat_map(file, save_fig=True, scale=s)

#     # 2. draw time series of states from 1987
#     file = 'yearly_agg_result_STATEFP.csv'
#     # a.deed_plot_time_series(file, save_fig=True)

#     # 3. scatter plot of HHI (x-axis) and price change (y-axis)
#     # change rate from 2006 to 2012 and 2012 to 2020
#     file = "yearly_agg_result_FIPS.csv"
#     # a.deed_plot_scatter(period=[2006, 2012], cur_index='ENI', filename=file, save_fig=False, filter_outlier=False)
#     # a.deed_plot_scatter(period=[2012, 2016], cur_index='ENI', filename=file, save_fig=False, filter_outlier=False)

#     # 4. make the panel data (make yearly data to include price change)
#     file = "yearly_agg_result_FIPS.csv"
#     # a.make_hhi_panel(file, gen_data=True)