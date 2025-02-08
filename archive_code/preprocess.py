'''
This program is to deal with the huge deed and tax files.
But no need now, we directly downloaded the split files.
'''

import pandas as pd
import csv # for csv.QUOTE_NONE, which ignores ' when reading csv
import platform
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import gc
import pyarrow.csv as pv
import pyarrow as pa

CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
BULK_DATA_PATH = EXT_DISK + 'CoreLogic/'

class bulk_process():
    def __init__(self, raw_fname: str, raw_path: str, raw_split_path: str, fips_file_path: str):
        self.raw_fname = raw_fname
        # but this cannot deal with property basic history
        ftype = 'deed' if 'ownertransfer' in raw_fname else 'tax'

        self.num_of_rows = 500000 if ftype == 'deed' else 250000  # ~250MB for both
        # self.num_of_rows = 200000

        self.raw_path = raw_path
        self.raw_split_path = raw_split_path.replace('REPL', ftype)
        self.fips_file_path = fips_file_path.replace('REPL', ftype)
        self.ftype = ftype

    def split_bulk_by_row(self):
        '''
        This function splits the original bulk data, by rows, into smaller files
        for later process.
        '''
        file_full_path = f'{self.raw_path}{self.raw_fname}'

        def save_chunk(chunk, path):
            chunk.to_csv(path, index=False)

        pbar = tqdm()

        with ThreadPoolExecutor(max_workers=4) as executor:
            for i, chunk in enumerate(pd.read_csv(
                file_full_path, delimiter="|", dtype='str',
                chunksize=self.num_of_rows, # can only work with engine = c or python
                on_bad_lines='skip', # skip the bad rows for now, guess there won't be many (cannot check tho)
                engine='c',
                quoting=csv.QUOTE_NONE
            )):
                if i == 189:  # Replace with condition to detect the problem
                    chunk.head(10).to_csv(f"{self.raw_split_path}partial_error_chunk.csv", index=False)

                cur_fpath = f"{self.raw_split_path}{self.ftype}_raw_row_split_{i+1}.csv"
                executor.submit(save_chunk, chunk, cur_fpath)
                del chunk  # to free memory
                gc.collect()  # force garbage collection
                pbar.update(1)

            pbar.close()

    def __split_bulk_by_row(self):
        '''
        This function splits the original bulk data, by rows, into smaller files
        for later process.
        '''
        file_full_path = f'{self.raw_path}{self.raw_fname}'

        def handle_invalid_row(row_number, row_data, error):
            print(f"Skipping malformed row {row_number}: {row_data}")
            return None  # Skip the row

        block_size = 200 * 1024 * 1024  # 200 MB
        read_options = pv.ReadOptions(block_size=block_size)  # Read 200 MB chunks
        parse_options = pv.ParseOptions(
            delimiter="|", quote_char=None,
            ignore_empty_lines=True
            # invalid_row_handler=handle_invalid_row
        )

        # Temporarily read the file to get column names and set column_types
        sample_table = pv.read_csv(
            file_full_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=pv.ConvertOptions(strings_can_be_null=True)  # Infer schema
        )
        column_names = sample_table.schema.names
        column_types = {col: pa.string() for col in column_names}  # All columns as strings

        # Convert options
        convert_options = pv.ConvertOptions(
            strings_can_be_null=True,  # Handle null values in string columns
            column_types=column_types  # Set all columns to string type
        )

        reader = pv.open_csv(
            file_full_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        pbar = tqdm()

        for i, batch in enumerate(reader):
            cur_fpath = f"{self.raw_split_path}{self.ftype}_raw_row_split_{i+1}.csv"
            df = batch.to_pandas()
            df.to_csv(cur_fpath, index=False)

            pbar.update(1)

        pbar.close()

    def generate_files_by_fips(self):
        '''
        '''
        # 1. prepare list of fips

        # 2. use several columns to determine exact fips, such as state and county's names

def main():
    # for deed, bulk size is 213GB and sub size is about 230MB, so there will be about 1000 files
    cur_file = 'duke_university_ownertransfer_v3_dpc_01465911_20230803_072211_data.txt'

    # cur_file = 'duke_university_property_basic2_dpc_01465909_20230803_072103_data.txt'

    bp = bulk_process(
        raw_fname=cur_file,
        raw_path=BULK_DATA_PATH,
        raw_split_path=f"{BULK_DATA_PATH}raw_split_REPL/",
        fips_file_path=f"{BULK_DATA_PATH}REPL_by_fips/"
    )

    bp.split_bulk_by_row()


if __name__ == "__main__":
    main()
