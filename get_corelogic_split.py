import shutil
import os

SERVER_PATH = '/Volumes/corelogic/'
DATA_FILE_PATH = SERVER_PATH + 'scripts/full_2023_fips_split/'

DEST_FOLDER_DEED = 'deed_split_2023/'
DEST_FOLDER_TAX = 'tax_split_2023/'

TAX_STR = 'university_property_basic'
DEED_STR = '_duke_university_ownertransfer'

def main():
    for fname in os.listdir(DATA_FILE_PATH):
        fpath = os.path.join(DATA_FILE_PATH, fname)

        if os.path.isfile(fpath):
            if DEED_STR in fname:
                # Copy the file to the destination folder
                shutil.copy(fpath, DEST_FOLDER_DEED)
            elif TAX_STR in fname:
                shutil.copy(fpath, DEST_FOLDER_TAX)


if __name__ == "__main__":
    main()