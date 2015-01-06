TajoProfile2CSV
===============

It converts custom tajo profiling result json file to CSV file

## Usage

python3 [-d] run.py <json files...>

-d is for debugging. It makes db file in local disk instead of memory.

#### Output File Name

It is determined automatically.    
If input file name has an extension, it will be replaced with '.csv'.    
If it doesn't, '.csv' will be just added.
