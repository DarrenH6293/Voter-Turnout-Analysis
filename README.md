# Data Extraction

The structure and content of the election data files vary significantly between states, and even within the same state, formatting differences exist across different years due to factors such as legal changes. In this dataset, some states had vote totals at both the county and statewide levels, while others didn't have specific vote counts entirely. Additionally, candidate and party names are not standardized across states, particularly for minor-party candidates. The extraction script selectively excludes files that lacked essential information, such as vote counts.

# Data Cleaning

The data cleaning script created a uniform dataset for analysis. Some states provided a single file containing all counties, while others split precinct data into multiple files. To standardize the data, the precinct-level files were merged. Then converted the processed data into a Parquet format for more efficient analysis.

# Exploratory Data Analysis

DuckDB was used to explore and validate the dataset.

## Schema Validation

- Ensure that all necessary fields were present.
- Expected columns were properly formatted.

## Row Count Validation

- Helped check that all extracted data was correctly written to the Parquet file.
- No major discrepancies existed in the number of records.
- Removed duplicate records.

## Outlier Detection

- See potential data anomalies by getting the highest vote counts.
- Helped identify any unreasonable vote counts that could show incorrect aggregation or errors in extraction.

## Missing Value Analysis

- Missing values could significantly impact analysis.
- Gathered the number of null or empty values across the columns.
- Check whether exclusion or data correction was necessary.

## Aggregation Check

- Calculate the minimum, maximum, and average vote counts to highlight potential data entry errors.
- Find party distribution for inconsistencies in party name.

## Dataset Sources

- Kaggle Link: https://www.kaggle.com/datasets/paultimothymooney/open-elections-data-usa/data
- Original Source: https://github.com/openelections
