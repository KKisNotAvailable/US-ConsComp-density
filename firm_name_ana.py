import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import re
import platform
from rapidfuzz import process, fuzz
from datetime import datetime
import csv # for csv.QUOTE_NONE, which ignores ' when reading csv
import warnings
warnings.filterwarnings("ignore")

CUR_SYS  = platform.system()

EXT_DISK = "F:/" if CUR_SYS == 'Windows' else '/Volumes/KINGSTON/'
EXT_DISK += "Homebuilder/"

OLD_FILES = EXT_DISK + "2016_files/"
DATA_PATH = f"{OLD_FILES}processed_data/"
COMSTAT_PATH = f"{OLD_FILES}Compustat/"

VAR_PATH = EXT_DISK + "Variables/"

TOP_COMP_LIST_PATH = "../NewCoreLogic_Codes/Data/Top200BuilderList2003_2022.txt"

CHUNK_SIZE = 1000000
CORELOGIC = 'corelogic'
COMPUSTAT = 'compustat'

ALIGN_NAME_COL = 'comp_name'

tqdm.pandas() # for tracking progress

class FirmAnalysis():
    def __init__(self) -> None:
        pass

    def __unique_names_to_file(self, source: str):
        '''
        Get unique names for CoreLogic and Compustat.
        CoreLogic used the cleaned and stacked 2016 data, and will filter only
        the new constructions to get the seller names as the company name.
        Compustat use the ~2016 yearly data (so would include firms other than builders).
        '''
        # 1. Check the existance of the unique firm name list
        out_file = f'{DATA_PATH}firm_names_{source}.csv'

        if not os.path.exists(f"{out_file}"):
            print(f"Creating the unique name list for {source}...")

            # 2. Read the data in and align the column names
            if source.lower() == CORELOGIC:
                # filter only the new constructions
                data_type_spec = {
                    "SELLER_NAME1": 'str',
                    "RESALE/NEW_CONSTRUCTION": 'category'
                }

                sub_lists = []

                for chunk in pd.read_csv(
                    f"{DATA_PATH}merged_stacked.csv", engine='c',
                    chunksize=CHUNK_SIZE, # can only work with engine = c or python
                    usecols=data_type_spec.keys(), dtype=data_type_spec
                ):
                    chunk = chunk[chunk['RESALE/NEW_CONSTRUCTION'] == 'N']
                    chunk = chunk.drop(columns=['RESALE/NEW_CONSTRUCTION'])
                    sub_lists.append(chunk)

                name_list = pd.concat(sub_lists, ignore_index=True)
                name_list.rename(
                    columns={'SELLER_NAME1': ALIGN_NAME_COL}, inplace=True)

            elif source.lower() == COMPUSTAT:
                data_type_spec = {
                    "conm": 'str',
                    "cusip": 'str',
                    "gvkey": 'str'
                }
                name_list = pd.read_stata(
                    f"{COMSTAT_PATH}compustat_fundamentals_y.dta",
                    columns=data_type_spec.keys()
                )
                name_list.rename(
                    columns={'conm': ALIGN_NAME_COL}, inplace=True)

            else:
                print("Wrong source name..., accept only corelogic and compustat.")
                return

            # 3. Take unique value of the sellers, sort by names, save to file
            name_list[ALIGN_NAME_COL] = name_list[ALIGN_NAME_COL].str.strip()
            name_list = name_list.drop_duplicates(subset=ALIGN_NAME_COL, keep='first') \
                if source == COMPUSTAT \
                    else name_list.groupby(ALIGN_NAME_COL).size().reset_index(name='count')
            name_list = name_list.sort_values(by=ALIGN_NAME_COL)

            name_list.to_csv(out_file, index=False)
            print(f"{source.capitalize()} name list created.\n")
        else:
            print(f"{source.capitalize()} file already exist!")

        return

    def get_cleaned_names(self, source: str):
        '''
        This function reads in the saved unique name list and remove the suffix

        Parameters
        ----------
        source: str.
            Accept only 'compustat' and 'corelogic'
        '''
        new_col = ALIGN_NAME_COL + "_new"

        # 1. Make sure the file is there and prepare top builder's list
        self.__unique_names_to_file(source)

        top_builders = pd.read_csv(TOP_COMP_LIST_PATH,
                                   header=None, names=[ALIGN_NAME_COL])

        # 2. Clean the names and try to match with top builders
        data = pd.read_csv(f'{DATA_PATH}firm_names_{source}.csv')

        # ======== START THE WORK =========

        # If leading is 0 and second is alphabet, replace 0 with O
        data[ALIGN_NAME_COL] = data[ALIGN_NAME_COL].map(
            lambda x: re.sub(r'^0([A-Za-z])', r'O\1', x)
        )

        # NOTE: want to keep the same row count
        # Rows starts without an alphabet or a number is set to None in new name column
        data[new_col] = data[ALIGN_NAME_COL].where(data[ALIGN_NAME_COL].str.match(r'^[A-Za-z0-9]'), "")

        # Some of the words have spaces, explicitly clean it
        data[new_col] = data[new_col].str.replace(r'\bHOME BUILDER\b', 'HOMEBUILDER', regex=True)
        data[new_col] = data[new_col].str.replace(r'\bHOME BUILDERS\b', 'HOMEBUILDERS', regex=True)

        # TODO: do layered fuzzy, like first on 'HOME', then can combine 'HOMEBUILDER'
        #       and then do 'HOMES'

        # 2-1. "-" to spaces
        data[new_col] = data[new_col].str.replace('-', ' ', regex=False)

        print("'-'s removed.")

        # 2-2. apply fuzzy match on specific words for fixing 'er' or 's' in the end of words
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

        score_threshold = 90 # have to be larger than this

        def fuzzy_match(word):
            # print(f">>> {word} !")
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

        data[new_col] = data[new_col].map(fuzzy_correct)
        print("Single word, fuzzy matching done.")

        # [Extra step]: delete everything behind ' HOMES'
        data[new_col] = data[new_col].str.replace(r'( HOMES) (.*)', r'\1', regex=True)

        # 2-3. replace with abbreviation: syntax eg. "LANE" => "LN"
        def use_abbrv(name: str):
            '''This corrects words in single company name'''
            abbrved_words = [abbrvs.get(w, w) for w in name.split()]
            return ' '.join(abbrved_words)

        data[new_col] = data[new_col].map(use_abbrv)

        # We may also apply this to the top builders
        # top_builders[new_col] = top_builders[ALIGN_NAME_COL].map(use_abbrv)

        print("Abbreviation set for both our list and top list.")

        # [Extra step]: turn HMS back to HOMES
        data[new_col] = data[new_col].str.replace(' HMS', ' HOMES', regex=False)

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

        data[new_col] = data[new_col].map(clean_suffixes)
        print("Suffixes removed.")

        # 2-5. find the best match from the top company names
        words_to_remove = [
            'CONST', 'PROP', 'CONSTRUCTION', 'PROPERTIES', 'VENTURES',
            'CUSTOM HOMES', 'BUILDERS'
        ] # they are not always suffix

        def clean_company_name(name: str):
            # Create a regex pattern to match any word in the list, accounting for spaces or punctuation
            pattern = r'\b(?:' + '|'.join(re.escape(word) for word in words_to_remove) + r')\b'

            # Substitute matching words with an empty string
            cleaned_name = re.sub(pattern, '', name)

            # Remove any extra whitespace left from removal
            return ' '.join(cleaned_name.split())

        def get_best_match_naive(name):
            '''Naively fuzzy matching names with top builders'''
            if not name: return ""

            # TODO: the first word should be the same, or fuzzily high score?

            # Get the best match and score from top company names
            match, score, *_ = process.extractOne(name, top_builders[ALIGN_NAME_COL])
            if score >= score_threshold:
                return match
            return ""

        def get_best_match(name):
            '''Fuzzy match substring of names based on name length of top builders.'''
            if name == "": return ""

            best_matches = []
            for ref in top_builders['for_match']:
                if name[0] != ref[0]:
                    continue
                # the first word less than 3 char is almost always, except THE,
                # abbreviation such as CC, but will be fuzzily matched with CCV or others
                # therefore I restrict the length of first word. (definitely not exhaustive)
                first_words_name, first_words_ref = name.split()[0], ref.split()[0]
                if (len(first_words_ref) <= 3) and \
                   (len(first_words_name) != len(first_words_ref)):
                    continue

                sub_name = name[:len(ref)+3] # this added number is manually picked, no valid reason

                # Calculate the fuzzy score between the reference and the substring
                score = fuzz.ratio(ref, sub_name)

                # Store matches that meet the score threshold
                if score >= 90:
                    best_matches.append((ref, score))

            # Return the best match based on score
            best_ref, best_score = max(best_matches, key=lambda x: x[1]) if best_matches else ("", 0)

            return best_ref

        # Apply fuzzy match on names starts with alphabet
        # TODO: might want to check the result from both old and new version.
        data['for_top_match'] = data[new_col].where(data[ALIGN_NAME_COL].str.match(r'^[A-Za-z]'), "")

        data['for_top_match'] = data['for_top_match'].map(clean_company_name)
        top_builders['for_match'] = top_builders[ALIGN_NAME_COL].map(clean_company_name)

        data['from_top_list'] = data['for_top_match'].progress_map(get_best_match)
        print("Matching with top builders done.")

        data.to_csv(f'{DATA_PATH}firm_names_{source}_matched.csv', index=False)
        print(f"{source} merged file created.")

        # TODO: List out all the records that can match with fuzzy but cannot by directly ==
        # => want to check if fuzzy is necessary
        # 目前都是關注homes，有沒有可能其他被丟掉的如CONSTRUCTION放回去也可以不用fuzzy


        # TODO: Unsolved
        # 0. process the raw names for grouping...
        # 1. MOSAIC USA => if we are going to check if a builder is operating cross counties or states
        # 2. is XXX LLC and XXX CO the same company?

    def merge_corelogic_compustat(self, core_file, comp_file, outname: str = ""):
        if not outname:
            outname = "get_comp_keys.csv"

        outname = DATA_PATH + outname

        merged = pd.merge(core_file, comp_file, on=ALIGN_NAME_COL, how='left')

        have_record = merged.dropna(subset=['gvkey']).reset_index(drop=True)
        from_top = merged.dropna(subset=['from_top_list']).reset_index(drop=True)

        print(f"Original > cases: {sum(merged['count'])}, company: {merged.shape[0]}, cleaned unique: {merged[ALIGN_NAME_COL + "_new"].nunique()}")
        print(f"From top 200 > {sum(from_top['count'])}, company: {from_top['from_top_list'].nunique()}")
        print(f"Merged > cases: {sum(have_record['count'])}, company: {have_record.shape[0]}")

        # merged.to_csv(outname, index=False)

def fuzzy_correct_example(df):
    correct_words = {
        'PACIFC': 'PACIFIC',
        'HOUSONG': 'HOUSING',
        'HSNG': 'HOUSING',
        # Add other words as needed
    }
    def correct_word(word):
        # Check if the word is in the dictionary
        if word in correct_words:
            return correct_words[word]
        # Use fuzzy matching for other cases
        match, score = process.extractOne(word, correct_words.keys())
        if score >= 80:  # Adjust threshold as needed
            return correct_words[match]
        return word  # Return the word unchanged if no match

    def correct_name(name):
        words = name.split()
        corrected_words = [correct_word(word) for word in words]
        return ' '.join(corrected_words)

    # Apply to the DataFrame
    df['Corrected Name'] = df['Company Name'].apply(correct_name)


def main():
    p = FirmAnalysis()

    # p.get_cleaned_names(COMPUSTAT)
    # p.get_cleaned_names(CORELOGIC)

    # ================================================
    #  Merge seller names in CoreLogic with Compustat
    # ================================================
    core_file = pd.read_csv(f"{DATA_PATH}firm_names_corelogic_matched.csv")
    comp_file = pd.read_excel(f"{OLD_FILES}Merge_Compustat&Corelogic/merge_corelogic_compustat.xlsx")
    comp_file = comp_file[['seller_name_original', 'gvkey']]\
        .rename(columns={'seller_name_original': ALIGN_NAME_COL})

    p.merge_corelogic_compustat(
        core_file=core_file, comp_file=comp_file
    )

    # Testing fuzz
    # print(fuzz.ratio("CENTENNIAL", "CENTEX")) # 71
    # print(fuzz.ratio("CBG", "CRABBE")) # 44

    # res = process.extractOne("MDC HOMES OF GREENVILLE LLC", ["MDC HOMES", "ABC DEVELOPMENT", "XYZ REALTY"])
    # print(res) # 90



if __name__ == "__main__":
    main()