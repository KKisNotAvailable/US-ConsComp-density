import pandas as pd
import numpy as np

class Preprocess():
    def __init__(self, filename: None) -> None:
        self._filename = filename

    def read_n_clean(self, cols_to_keep: list = []):
        '''
        The actions done here will be based on the data type of self.__filename
        str: does on single file
        list: does on the list of files
        None: all of the files in the directory
        '''

        df = pd.read_csv(self._filename, delimiter="|")

        if cols_to_keep:
            df = df[cols_to_keep]


        return df

    def deed_analysis(self):
        '''
        The actions done here will be based on the data type of self.__filename
        str: does on single file
        list: does on the list of files
        None: all of the files in the directory
        '''

        df = pd.read_csv(self._filename, delimiter="|")

        cols_to_keep = [
            "OWNER_1_FIRST_NAME&MI", # company name
            "SELLER NAME1",
            "SALE AMOUNT", # but after skimming throught, there are several NULL
            "CORPORATE_INDICATOR",
            "OWNER_RELATIONSHIP_RIGHTS_CODE",
            "RESALE/NEW_CONSTRUCTION" # M: re-sale, N: new construction
        ]
        df = df[cols_to_keep]

        cond1 = df['CORPORATE_INDICATOR'] == "Y"

        # check the dist. of 0 sale amount
        df['corp_idx'] = cond1
        df['amt_is_nan'] = pd.isna(df['SALE AMOUNT'])
        tmp = df.groupby(['corp_idx', 'amt_is_nan']).size().reset_index(name='counts')
        print("#####")
        print(tmp)
        print("=====")

        cond1 = df['CORPORATE_INDICATOR'] == "Y"

        df = df[cond1]

        # for given cond, see the content of owner name
        # tmp = df['OWNER_1_FIRST_NAME&MI']
        # print(tmp)
        # # resale and new constr. dist.
        # tmp = df.groupby(['RESALE/NEW_CONSTRUCTION']).size().reset_index(name='counts')
        # print(tmp)
        # # check what owner rights code might actually means
        # tmp = df.groupby(['OWNER_RELATIONSHIP_RIGHTS_CODE']).size().reset_index(name='counts')
        # print(tmp)
        # # check what are the seller names
        # tmp = df.groupby(['SELLER NAME1']).size().reset_index(name='counts')
        # print(tmp)

        # print(df[df['SELLER NAME1'] != 'OWNER RECORD'][['SELLER NAME1', 'OWNER_1_FIRST_NAME&MI']])
        # print(df[df['SELLER NAME1'] == 'OWNER RECORD'][['SELLER NAME1', 'OWNER_1_FIRST_NAME&MI']])
        return

    def check_company_list(self):
        '''
        This method simply serves as a exploratory analysis of 
        the company list provided by previous research.
        '''
        filepath = "../Merge_Compustat&Corelogic/"
        filename = filepath+'corelogic_clean.dta'
        
        comp_list = pd.read_stata(filename)

        print(comp_list.columns)


def main():

    filepath = "../Corelogic/bulk_deed_fips_split/"

    filename = filepath+'fips-01001-UniversityofPA_Bulk_Deed.txt'

    p = Preprocess(filename)
    #p.check_company_list()
    # p.deed_analysis()


if __name__ == "__main__":
    main()