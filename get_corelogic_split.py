import shutil
import os
import pandas as pd
import gc
import csv
from collections import Counter

SERVER_PATH = '/Volumes/corelogic/'
DATA_FILE_PATH = SERVER_PATH + 'scripts/full_2023_fips_split/'

DEST_FOLDER_DEED = 'output/'  # just for not reporting error
# DEST_FOLDER_DEED = '/Volumes/KINGSTON/CoreLogic/deed_split_2023/'
DEST_FOLDER_TAX = 'output/'  # just for not reporting error
# DEST_FOLDER_TAX = '/Volumes/KINGSTON/CoreLogic/tax_split_2023/'
DEST_FOLDER_HIST_TAX_TEMP = 'output/historical_tax_split/'  # download to here and then move to _FINAL
DEST_FOLDER_HIST_TAX_FINAL = '/Volumes/KINGSTON/CoreLogic/hist_tax_split_2023/'

TAX_STR = 'university_property_basic'
DEED_STR = '_duke_university_ownertransfer'
HIST_TAX_STR = 'university_hist_property_basic'


def get_files_not_in_dest(get_deed=False, get_tax=False, get_hist_tax=False):
    '''
    This function will get the files from the source folder if the file name
    does not exist in the destination folder.

    Parameters
    ----------
    get_deed: bool.
        to indicate whether is downloading deed.
    get_tax: bool.
        to indicate whether is downloading tax.
    '''
    deed_files_transferred = os.listdir(DEST_FOLDER_DEED)
    tax_files_transferred = os.listdir(DEST_FOLDER_TAX)
    hist_tax_files_transferred = os.listdir(DEST_FOLDER_HIST_TAX_FINAL)

    for fname in os.listdir(DATA_FILE_PATH):
        fpath = os.path.join(DATA_FILE_PATH, fname)

        if os.path.isfile(fpath):
            if get_deed and (DEED_STR in fname) and (fname not in deed_files_transferred):
                print(f"Deed {fname[:10]} found, starting download...", end=' ')
                # Copy the file to the destination folder
                shutil.copy(fpath, DEST_FOLDER_DEED)
                print('Done!')
            elif get_tax and (TAX_STR in fname) and (fname not in tax_files_transferred):
                print(f"Tax {fname[:10]} found, starting download...", end=' ')
                shutil.copy(fpath, DEST_FOLDER_TAX)
                print('Done!')
            elif get_hist_tax and (HIST_TAX_STR in fname) and (fname not in hist_tax_files_transferred):
                print(f"Historical Tax {fname[:10]} found, starting download...", end=' ')
                shutil.copy(fpath, DEST_FOLDER_HIST_TAX_TEMP)
                print('Done!')

    return


def check_file_size(check_deed: bool, check_tax: bool, check_hist_tax: bool):
    '''
    Compare the downloaded files in the destination folder with the source
    folder for the file size consistency. If a certain file's size is not
    consistent, re-download the file.

    Parameters
    ----------
    check_deed: bool.
        to indicate whether is checking deed.
    check_tax: bool.
        to indicate whether is checking tax.
    '''
    ignore_list = [".DS_Store"]
    if check_deed:
        for fname in os.listdir(DEST_FOLDER_DEED):
            if fname in ignore_list:
                continue
            f1 = DEST_FOLDER_DEED + fname
            f2 = DATA_FILE_PATH + fname
            size1 = os.path.getsize(f1)
            size2 = os.path.getsize(f2)

            if size1 != size2:
                print('Deed', fname[:10], 'need re-download...')

        print("All deed files have matching size!")

    if check_tax:
        for fname in os.listdir(DEST_FOLDER_TAX):
            if fname[0] == ".":
                continue
            f1 = DEST_FOLDER_TAX + fname
            f2 = DATA_FILE_PATH + fname
            size1 = os.path.getsize(f1)
            size2 = os.path.getsize(f2)

            if size1 != size2:
                print('Tax', fname[:10], 'need re-download...')

    if check_hist_tax:
        for fname in os.listdir(DEST_FOLDER_HIST_TAX_FINAL):
            if fname[0] == ".":
                continue
            f1 = DEST_FOLDER_HIST_TAX_FINAL + fname
            f2 = DATA_FILE_PATH + fname
            size1 = os.path.getsize(f1)
            size2 = os.path.getsize(f2)

            if size1 != size2:
                # since there are multiple files for a FIPS, we need the full
                # name to locate the exact file.
                print('Hist Tax', fname, 'need re-download...')

    return


def check_file_io(check_deed: bool, check_tax: bool, check_hist_tax: bool):
    '''
    Check if the downloaded files could be correctly read by python.
    The assumption is: if the files were not intact, there should be multiple
    missing columns in the last row and would not be able to be read.

    Parameters
    ----------
    check_deed: bool.
        to indicate whether is checking deed.
    check_tax: bool.
        to indicate whether is checking tax.
    '''
    if check_deed:
        for fname in os.listdir(DEST_FOLDER_DEED):
            if fname[:4] != 'FIPS':
                continue
            f1 = DEST_FOLDER_DEED + fname

            try:
                tmp = pd.read_csv(f1, sep='|', quoting=csv.QUOTE_NONE, dtype='str')
            except:
                print(f"Error reading deed {fname[:10]}")
            finally:
                if 'tmp' in locals():
                    # Ensure memory is cleared
                    del tmp
                    gc.collect()

        print("Deed file check done.")

    if check_tax:
        for fname in os.listdir(DEST_FOLDER_TAX):
            if fname[:4] != 'FIPS':
                continue
            f1 = DEST_FOLDER_TAX + fname

            try:
                tmp = pd.read_csv(f1, sep='|', quoting=csv.QUOTE_NONE, dtype='str')
            except:
                print(f"Error reading tax {fname[:10]}")
            finally:
                if 'tmp' in locals():
                    # Ensure memory is cleared
                    del tmp
                    gc.collect()

        print("Tax file check done.")

    if check_hist_tax:
        for fname in os.listdir(DEST_FOLDER_HIST_TAX_FINAL):
            if fname[:4] != 'FIPS':
                continue
            f1 = DEST_FOLDER_HIST_TAX_FINAL + fname

            try:
                tmp = pd.read_csv(f1, sep='|', quoting=csv.QUOTE_NONE, dtype='str')
            except:
                print(f"Error reading hist tax {fname}")
            finally:
                if 'tmp' in locals():
                    # Ensure memory is cleared
                    del tmp
                    gc.collect()

        print("Hist Tax file check done.")

    return


def check_specific_fips(fips: str, is_deed=True):
    '''
    This function is to check why individual files could not read.

    Parameters
    ----------
    fips: str.
        the fips file for either deed or tax.
    is_deed: bool.
        to specify if the file is a deed or tax file.
    '''
    type_spec = {
        "CLIP": 'str',
        "FIPS CODE": 'category',
        "LAND USE CODE - STATIC": 'category',
        "MOBILE HOME INDICATOR": 'category',
        "PROPERTY INDICATOR CODE - STATIC": 'category',
        "PRIMARY CATEGORY CODE": 'category',
        "DEED CATEGORY TYPE CODE": 'category',
        "SALE AMOUNT": 'float',
        "SALE DERIVED DATE": 'str',
        "SALE DERIVED RECORDING DATE": 'str',  # used to fill the empty in derived date
        "INTERFAMILY RELATED INDICATOR": 'uint8',
        "RESALE INDICATOR": 'uint8',
        "NEW CONSTRUCTION INDICATOR": 'uint8',
        "RESIDENTIAL INDICATOR": 'category',
        "SHORT SALE INDICATOR": 'uint8',
        "FORECLOSURE REO INDICATOR": 'uint8',
        "FORECLOSURE REO SALE INDICATOR": 'uint8',
        "BUYER 1 FULL NAME": 'str',
        "SELLER 1 FULL NAME": 'str'
    }

    if is_deed:
        fpath = f"{DEST_FOLDER_DEED}FIPS_{fips}_duke_university_ownertransfer_v3_dpc_01465911_20230803_072211_data.txt"
    else:
        fpath = f"{DEST_FOLDER_TAX}FIPS_{fips}_duke_university_property_basic2_dpc_01465909_20230803_072103_data.txt"

    df = pd.read_csv(
        fpath, sep='|',
        quoting=csv.QUOTE_NONE,
        usecols=type_spec.keys(), dtype=type_spec
    )

    print(df.shape)


def check_missing_fips(check_deed: bool, check_tax: bool):
    '''
    This function will check all of the files from corelogic, and see if there
    are any missing fips.
    '''
    all_fips = pd.read_csv('../NewCoreLogic_Codes/Data/fips2county.tsv.txt', sep='\t', dtype='str')

    if check_deed:
        print("Checking Deed...")
        deed_fips_dict = {f: 0 for f in all_fips['CountyFIPS']}

        for fname in os.listdir(DEST_FOLDER_DEED):
            if fname[:4] != 'FIPS':
                continue
            cur_fips = fname[5:10]
            deed_fips_dict[cur_fips] = deed_fips_dict.get(cur_fips, -2) + 1

        # 1: in list and in folder => 2801
        # 0: in list but not in folder => 7
        # -1: not in list but in folder => 342
        ctrs = Counter(deed_fips_dict.values())
        print(f"There are {ctrs[1]} FIPS files in the folder.")
        print(f"There are {ctrs[-1]} FIPS files in the folder but not in the list.")
        print(f"There are {ctrs[0]} FIPS not in the folder.")

        no_file, not_in_list = [], []
        for k, v in deed_fips_dict.items():
            if v == -1:
                not_in_list.append(k)
            elif v == 0:
                no_file.append(k)
        print("Has file but not in list:", not_in_list)
        # print("No file:", no_file)


    if check_tax:
        print("Checking Tax...")
        tax_fips_dict = {f: 0 for f in all_fips['CountyFIPS']}

        for fname in os.listdir(DEST_FOLDER_TAX):
            if fname[:4] != 'FIPS':
                continue
            cur_fips = fname[5:10]
            tax_fips_dict[cur_fips] = tax_fips_dict.get(cur_fips, -2) + 1
        ctrs = Counter(tax_fips_dict.values())
        print(f"There are {ctrs[1]} FIPS files in the folder.")
        print(f"There are {ctrs[-1]} FIPS files in the folder but not in the list.")
        print(f"There are {ctrs[0]} FIPS not in the folder.")

        no_file, not_in_list = [], []
        for k, v in tax_fips_dict.items():
            if v == -1:
                not_in_list.append(k)
            elif v == 0:
                no_file.append(k)
        print("Has file but not in list:", not_in_list)


def main():
    # get_files_not_in_dest(get_deed=False, get_tax=False, get_hist_tax=True)

    # check_file_size(check_deed=False, check_tax=False, check_hist_tax=True)

    check_file_io(check_deed=False, check_tax=False, check_hist_tax=True)

    # deed_to_check = [  # they could not be read without quoting=csv.QUOTE_NONE
    #     '41053', '06037', '42091', '06001', '06085', '48149', '37127', '48349',
    #     '06053', '49011', '06059', '36055', '37159', '17031', '39155'
    # ]
    # for d in deed_to_check:
    #     print(f"Checking FIPS {d}")
    #     check_specific_fips(fips=d, is_deed=True)  # originally 103 columns

    # check_missing_fips(check_deed=False, check_tax=True)

    print("All Checking Done.")

if __name__ == "__main__":
    main()