import dask.dataframe as dd
import pandas as pd

class Analysis():
    def __init__(self) -> None:
        pass

    def deed_analysis(self):
        '''
        COLUMNS:
        ['FIPS', 'PCL_ID_IRIS_FRMTD', 'BLOCK LEVEL LATITUDE',
       'BLOCK LEVEL LONGITUDE', 'SITUS_CITY', 'SITUS_STATE', 'SITUS_ZIP_CODE',
       'SELLER NAME1', 'SALE AMOUNT', 'SALE DATE', 'RECORDING DATE',
       'PROPERTY_INDICATOR', 'RESALE/NEW_CONSTRUCTION']
        '''
        ddf = dd.read_csv('data/deed_stacked.csv')

        ttl_rows = ddf['FIPS'].shape[0].compute() # but why len(ddf['FIPS']) won't work?

        # check if PCL_ID_IRIS_FRMTD is distinct (it is not, but should it be?)
        # unique_count = ddf['PCL_ID_IRIS_FRMTD'].nunique().compute()
        # print(f"PCL_ID_IRIS_FRMTD is{' not' if ttl_rows != unique_count else ''} unique key.")
        
        # check the nan distribution of the following columns
        cols_to_clean = [
            "SITUS_CITY", 
            "SITUS_STATE", 
            "SALE AMOUNT", 
            "SALE DATE"
        ]
        good_counts = ddf[cols_to_clean].count().compute()
        
        print("Non NA pct for ...")
        for i, c in enumerate(cols_to_clean):
            print(f"  {c:<11}: {good_counts.iloc[i]*100/ttl_rows:>7.2f}%")

        # 1. ignore rows if any of those columns is empty
        ddf = ddf.dropna(subset=cols_to_clean)

        # 2. group by SITUS_CITY / SITUS_STATE 
        #   2-1. count cases, distinct "SELLER NAME1" 
        #   2-2. sum "SALE AMOUNT"
        cols_to_gp = ["SITUS_CITY", "SITUS_STATE"]

        for c in cols_to_gp:
            to_cat = [
                ddf.groupby(c)\
                    .agg(
                        case_cnt=('PCL_ID_IRIS_FRMTD', 'count'),
                        sale_amt=('SALE AMOUNT', 'sum')
                    )\
                    .compute(),
                # since the following is a series, no need to specify column when renaming
                ddf.groupby(c)['SELLER NAME1']\
                    .nunique()\
                    .rename('uniq_seller_cnt')\
                    .compute()
            ]
            
            result = pd.concat(to_cat, axis=1) # horizontally
            
            print(f"Grouping result by {c}\n{result}")

        # plot the non yearly result on maps

        # do similar action above but based on SALE YEAR
        # maybe the plotting can be animated? 
        # (even is the plotting for yearly possible?)


def main():

    a = Analysis()

    # =======================
    # Test analysis with dask
    # =======================
    a.deed_analysis()

if __name__ == "__main__":
    main()