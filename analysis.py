import dask.dataframe as dd

class Analysis():
    def __init__(self) -> None:
        pass

    def simple_analysis(self):
        df = dd.read_csv('data/deed_stacked.csv')
        print(df.columns)
        
        # check the nan distribution of the following columns
        cols_to_clean = [
            "SITUS_CITY", 
            "SITUS_STATE", 
            "SALE AMOUNT", 
            "SALE DATE"
        ]
        good_counts = df[cols_to_clean].count().compute()
        print(good_counts)
        ttl_rows = len(df['FIPS'])
        print(ttl_rows)

        # for i, c in enumerate(cols_to_clean):
        #     print(f"Non NA pct for {c}: {good_counts[i]*100/ttl_rows:.2f}%")

        # ignore rows if SITUS_CITY, SITUS_STATE, SALE AMOUNT, SALE DATE
        # is empty
        
        # df = df.dropna(subset=cols_to_clean)

        # check if PCL_ID_IRIS_FRMTD is distinct


        # group by SITUS_CITY / SITUS_STATE 
        # 1. count cases, distinct SELLER NAME1 
        # 2. sum "SALE AMOUNT"

        # do similar action above but based on SALE YEAR


def main():

    a = Analysis()

    # =======================
    # Test analysis with dask
    # =======================
    a.simple_analysis()

if __name__ == "__main__":
    main()