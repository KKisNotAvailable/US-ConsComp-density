import pandas as pd
import platform
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


def stats_county(out_file='county_summary_stats.csv'):
    '''
    Group by county, across all years, get the:
    1. unique seller count
    2. HHI
    3. case_cnt
    4. sale_amt
    '''
    data = pd.read_csv(
        STACKED_PATH+'corelogic_seller_panel.csv',
        dtype={"FIPS": 'category', "YEAR": 'category'}
    )

    print(sum(data['CASE_CNT']))
    print(data['SELLER_NAME1'].nunique())

    result = data.groupby('FIPS').agg(
        unique_seller_cnt=('SELLER_NAME1', 'count'),
        sale_amt=('SALE_AMOUNT', 'sum'),
        case_cnt=('CASE_CNT', 'sum'),
        mean_unit_price=('MEAN_UNIT_PRICE', 'mean')
    ).reset_index()

    hhi_data = data = pd.read_csv(
        STACKED_PATH+'for_heat_map.csv',
        usecols=['FIPS', 'HHI'],
        dtype={"FIPS": 'category'}
    )
    hhi_data['ENI'] = 1 / hhi_data['HHI']

    result = result.merge(hhi_data, on='FIPS', how='left')

    # result.describe().to_csv(IMG_PATH+out_file, index=True)

    # when the county does not have any new construction sold between
    # 2000 and 2016, then its FIPS won't show in result.
    # (and in hhi_data, it will show case_cnt == 0)
    # print(sorted(set(hhi_data['FIPS']) - set(result['FIPS'])))


def stats_vars(out_file='variable_summary_stats.csv'):
    # we directly get summary for all for now (might want to first get mean of counties and
    # then get overall summary?)
    to_cat = []

    type_spec = {"FIPS": 'category'}
    hs_data = pd.read_csv(f"{VAR_PATH}house_stock_panel.csv", dtype=type_spec)
    to_cat.append(hs_data['House_stock'].describe())

    mhhi_data = pd.read_csv(f"{VAR_PATH}med_hh_income_panel.csv", dtype=type_spec)
    to_cat.append(mhhi_data['Median_HH_income'].describe())

    nd_data = pd.read_csv(f"{VAR_PATH}natural_disaster_panel.csv", dtype=type_spec)
    nd_data = nd_data[nd_data['FIPS'].str[-2:] != '00']
    to_cat.append(nd_data['Count'].describe())

    pop_data = pd.read_csv(f"{VAR_PATH}pop_panel.csv", dtype=type_spec)
    print(max(pop_data['Population']))
    to_cat.append(pop_data['Population'].describe())

    unemp_rate_data = pd.read_csv(f"{VAR_PATH}unemployment_rate_panel.csv", dtype=type_spec)
    unemp_rate_data['Unemployment_rate'] = unemp_rate_data['Unemployment_rate'].replace('-', '0').astype(float)
    to_cat.append(unemp_rate_data['Unemployment_rate'].describe())

    wrluri_data = pd.read_csv(f"{VAR_PATH}WRLURI_2006.csv", dtype=type_spec)
    to_cat.append(wrluri_data['WRLURI'].describe())


    type_spec = {"StateFIPS": 'category'}
    vacan_data = pd.read_csv(f"{VAR_PATH}vacancy_panel.csv", dtype=type_spec)
    to_cat.append(vacan_data['Vacancy_rate'].describe())

    result = pd.concat(to_cat, axis=1)
    # result.to_csv(IMG_PATH+out_file, index=True)


def main():
    stats_county()
    return
    # stats_vars()

    data = pd.read_csv(
        STACKED_PATH+'homebuilder_panel.csv',
        usecols=['COUNTY_CASE_CNT', 'RESALE_CASE_CNT']
    )
    data = data.dropna()

    print(sum(data['RESALE_CASE_CNT']) + sum(data['COUNTY_CASE_CNT']))


if __name__ == '__main__':
    main()