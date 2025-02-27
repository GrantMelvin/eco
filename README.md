# SAS ECO Data Quality Framework

## Overview

The ECO Data Quality Framework is a comprehensive SAS-based solution for automating data quality assessment, reporting, and correction workflows. This framework leverages SAS Viya's API to interact with the Information Catalog, perform statistical analysis on datasets, and generate standardized data quality reports.

## Key Features

- **Automated Data Import**: Support for various file formats (CSV, Excel, SAS7BDAT)
- **Data Quality Bot Integration**: Uses Information Catalog bots for automated data profiling
- **Statistical Analysis**: Extracts and analyzes data quality metrics from the Information Catalog
- **Automated Reporting**: Generates PDF reports highlighting data quality issues
- **Data Correction**: Provides functionality for data imputation and cleanup

## Directory Structure

```
└── grantmelvin-eco/
    ├── dq_testing.sas       # API testing
    ├── eco_dq_v1.sas        # Core framework implementation
    ├── workflow_test.sas    # End-to-end workflow testing script
    ├── reports/             # Directory for generated reports
        ├── TEST_E2E_1_report.pdf
        ├── TEST_E2E_2_report.pdf
        ├── TEST_E2E_3_report.pdf
        └── TEST_E2E_4_report.pdf
    └── test_files/          # Test datasets
        ├── Financial_Sample.xlsx
        ├── all-approved_oncology_drugs.xlsx
        ├── metadata_test.csv
        └── test.sas7bdat
```

## Core Macros

### Import Data (`%import_data`)

Imports data from various file formats into SAS CAS libraries:

```sas
%import_data(
    file,           /* Path to the file you want to import */
    caslib,         /* Target caslib to upload to */
    table=table     /* Desired table name for the imported file */
);
```

### Run Bots (`%run_bots`)

Creates and executes SAS Information Catalog bots to analyze dataset properties:

```sas
%run_bots(
    BASE_URI,       /* The base path of the SAS Viya site */
    table,          /* The table you want to evaluate */
    caslib,         /* The caslib that the table is located in */
    provider,       /* The provider of the desired table */
    server          /* The server of the provided table */
);
```

### Get Statistics (`%get_statistics`)

Retrieves data quality metrics from the Information Catalog:

```sas
%get_statistics(
    BASE_URI,       /* The base path of the SAS Viya site */
    table,          /* The table you want to evaluate */
    caslib          /* The caslib that the table is located in */
);
```

### Generate Report (`%generate_report`)

Creates a standardized data quality report highlighting issues:

```sas
%generate_report(
    BASE_URI,       /* The base path of the SAS Viya site */
    table,          /* The table you want to evaluate */
    caslib,         /* The caslib that the table is located in */
    provider,       /* The provider of the desired table */
    server,         /* The server of the provided table */
    doc_path        /* The directory where you want the report to be stored */
);
```

### First Correction (`%first_correction`)

Performs initial data quality corrections through imputation:

```sas
%first_correction(
    BASE_URI,       /* The base path of the SAS Viya site */
    table,          /* The table you want to evaluate */
    caslib,         /* The caslib that the table is located in */
    provider,       /* The provider of the selected table */
    server,         /* The server of the selsected table */
    impute_on,      /* The variable that you want to perform imputation on */
    impute_method   /* The method of imputation (mean, ...) */
);
```

### End-to-End Workflow (`%run_e2e`)

Executes the complete data quality workflow from data import to reporting:

```sas
%run_e2e(
    file=file,                  /* The file path that you want to upload and analyze */
    provider=provider,          /* The provider of the desired table */
    server=server,              /* The server of the provided table */
    caslib=caslib,              /* The caslib that you want the table to be located in */
    table=table,                /* The name of the table that you want to create */
    doc_path=path               /* The directory where you want the report to be stored */
);
```

## Usage Example

The following example demonstrates a complete end-to-end workflow for analyzing a SAS dataset:

```sas
/* Get the current directory path */
%let fullpath = &_SASPROGRAMFILE;
%let basepath = %substr(&fullpath, 1, %index(&fullpath, workflow_test.sas) - 2);

/* Include the Data Quality Module */
%include "&basepath/eco_dq_v1.sas";

/* Define test file path */
%let test_file = &basepath/test_files/test.sas7bdat;

/* Run end-to-end workflow */
%run_e2e(
    file=&test_file,
    provider=cas,
    server=cas-shared-default,
    caslib=CASUSER(grmelv),
    table=test_e2e_1,
    doc_path=&basepath/reports
);
```

## Data Quality Metrics

The framework evaluates several key data quality metrics:

1. **Completeness**: Identifies variables with high percentages of missing values
2. **Outliers**: Detects variables containing statistical outliers
3. **Type Mismatches**: Identifies variables with data type mismatches

## Report Generation

The framework generates PDF reports highlighting data quality issues with the following sections:

- Variables with completeness issues (< 50% complete)
- Variables with outlier issues (> 10% outliers)
- Variables with type mismatch issues (> 25% mismatch)

## API Integration

The framework leverages SAS Viya REST APIs for:

- Catalog interaction
- Bot creation and management
- Data table management
- Row and column access
- Information Catalog statistics retrieval

## Requirements

- SAS Viya environment
- Access to CAS libraries
- Appropriate permissions for creating and running bots, accessing instances, creating caslibs
- Write access to the reports directory

## Notes

- The `dq_testing.sas` file contains development and testing code for API interaction
- The framework uses environment-specific paths and may require adjustments for your SAS environment
- Bot execution can take significant time for large datasets
