# Pandas Data Quality Assurance (PDQA) Tools

The `pdqa` library puts together some often repeated data QA tasks in one place.
It helps you create data QA routines, run them repeatedly with minimal manual typing, 
and log QA results to a variety of destinations such as log files or Slack feeds (as a bot).

## How to Install
Right now this repository is not listed in any `pip` or `conda` channel.  
To use it, clone or download this repository to your python script working directory, or the `site-packages` folder of your Python installation.  
For example:
```bash
$ git clone https://github.com/ligz08/pdqa.git
```

## How to Use
For example, if you want to check if column `ID` of a dataframe are all 10-digit numbers, you can create a `ColumnFormatInspector` object, and use it to `inspect()` a dataframe.
```python
from pdqa.singledf import ColumnFormatInspector
id_format_inspector = ColumnFormatInspector(col='ID', regex='\d{10}')
for df in list_of_dataframes:
    id_format_inspector.inspect(df)
```
More advancedly, you can pass a `Logger` (python built-in class) and/or `FailSampler` (implemented in this library) object to an inspector so that when any abnormality happens, the logger can log it and the fail sampler can sample your data for records that failed an inspection.


## How it Works
At the core of `pdqa` are functions that check certain aspects of a Pandas dataframe, such as [`check_col_format()`](singledf.py#L9). These functions usually take in a dataframe as input, perform their respective checks, and output check results to destinations of your choice, which can be simply messages printed on screen, or as fancy as automatic messeges sent to a Slack channel.

Since many checks are done repeatedly on many dataframes, several `DataFrameInspector` classes are implemented in this library, such as a `ColumnFormatInspector`. Instances of these classes are simply containers of a set of parameters you want to pass to a check function.