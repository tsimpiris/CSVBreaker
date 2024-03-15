import os
import sys
import glob
import shutil
from sys import argv
from pathlib import Path
from itertools import islice

import polars as pl
import polars.selectors as cs


def main():
    input_folder, cols_per_csv, csv_files = validate_args(argv)
    
    output_folder = os.path.join(input_folder, 'outputs')
    # remove output folder + its contents, if exist
    if os.path.isdir(output_folder): shutil.rmtree(output_folder)
    
    try: # create output folder
        os.makedirs(output_folder)
    except Exception:
        print(f'oh dear, I cannot create an output folder, who knows why: {output_folder}')
        sys.exit(1)

    lazyframes_from_csv = {}
    for csv_file in csv_files:
        lazyframes_from_csv[os.path.basename(csv_file)] = pl.scan_csv(csv_file, dtypes={'FIPS': str, 'ID': str})
        
    for filename, lf in lazyframes_from_csv.items():
        print(f'Processing {filename}')

        df = lf.collect() # convert lazyframe to dataframe
        df = df.cast({cs.integer(): pl.Float64}) # cast all integer columns to f64

        print(f'Total columns: {len(df.columns)}\nTotal rows: {len(df)}')
        
        batch_counter = 0

        cols_excluding_first = df.columns[1:]

        for batch in batched(cols_excluding_first, cols_per_csv-1):
            batch_counter += 1
            batch.insert(0, df.columns[0]) # insert 1 in each batch to get the first column which is usually the ID column

            df_to_export = df.select([pl.col(i) for i in batch])

            output_filepath = os.path.join(output_folder, f'{filename[:-4]}_{batch_counter}.csv')
            
            df_to_export.write_csv(output_filepath)
            print(f'{os.path.basename(output_filepath)} has been exported.')
            del df_to_export
            del batch
        del df


# https://discuss.python.org/t/add-batching-function-to-itertools-module/19357/17
def batched(iterable, n):
    it = iter(iterable)
    while (batch := list(islice(it, n))):
        yield batch


def validate_args(argv):
    argv.pop(0) # remove script's name
    
    if not len(argv) == 2:
        print(f'Requires user defined parameters: 2\nGiven parameters: {len(argv)}')
        print(argv)
        sys.exit(1)

    input_folder = Path(argv[0])
    
    if not os.path.isdir(input_folder):
        print('The given input folder does not exist')
        sys.exit(1)
    else:
        csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
        if len(csv_files) > 0:
            print(f'Input folder has been validated\nCSV File(s): {len(csv_files)}\nInput Path: {input_folder}')
        else:
            print('Input folder exists but there are no CSV files.')
            sys.exit(0)
    
    try:
        cols_per_csv = int(argv[1])
        if not cols_per_csv > 1:
            print('The second parameter should be an integer greater than 1.')
            sys.exit(1)
        else:
            print(f'The second parameter has been validated: {cols_per_csv}')
    except ValueError as e:
        print('The second parameter should be an integer.')
        sys.exit(1)

    return input_folder, cols_per_csv, csv_files


if __name__ == "__main__":
    main()