/* Get the current directory path */
%let fullpath = &_SASPROGRAMFILE;
%let basepath = %substr(&fullpath, 1, %index(&fullpath, workflow_test.sas) - 2);
%put &basepath;

/* Data Quality Module */
%include "&basepath/eco_dq_v1.sas";

/* Test files for analysis */
%let test_file_1 = &basepath/test_files/test.sas7bdat;
%let test_file_2 = &basepath/test_files/metadata_test.csv;
%let test_file_3 = &basepath/test_files/Financial_Sample.xlsx;
%let test_file_4 = &basepath/test_files/all-approved_oncology_drugs.xlsx;

/* Uploads and promotes a file to CAS */
/* Parameters:
	file   = file path you want to perform analysis on (sas7bdat, csv, xls, xlsx supported)
	caslib = target caslib to upload to (best with casuser)
	table  = desired table name for file
*/
%import_data(
    &test_file_1,
    CASUSER(grmelv),
    table=test_e2e_1
);

/* Creates and Runs bot for analysis in the SAS Catalog */
/* Parameters:
	BASE_URI = the base path of the SAS Viya site
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
	provider = the provider of the desired table
	server   = the server of the provided table
*/
%run_bots(
	&BASE_URI,
	test_e2e_1,  
	CASUSER(grmelv),
	cas,
	cas-shared-default
);

/* Retrieves the information catalog statistics for the given table */
/* Parameters:
	BASE_URI = the base path of the SAS Viya site
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
*/
%get_statistics(
	&BASE_URI,
	test_e2e_1, 
	CASUSER(grmelv)
);

/* Creates a report to get data quality statistics on the given table */
/* Parameters:
	BASE_URI = the base path of the SAS Viya site
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
	provider = the provider of the desired table
	server   = the server of the provided table
	doc_path = the directory where you want the report to be stored
*/
%generate_report(
	&BASE_URI,
	test_e2e_1,
	CASUSER(grmelv),
	cas,
	cas-shared-default,
	&basepath/reports
);

/* Produces a table that imputes on the desired variable and drops rows that have a certain % of values missing*/
/* Parameters:
	BASE_URI 	  = the base path of the SAS Viya site
	table 	  	  = the table you want to evaluate
	caslib 	  	  = the caslib that the table is located in
	provider  	  = the provider of the desired table
	server    	  = the server of the provided table
	impute_on     = the variable that you want to perform imputation on
	impute_method = the method of imputation that you'd like to use
*/
%first_correction(
	&BASE_URI,
	test_e2e_1, 
	CASUSER(grmelv),
	cas,
	cas-shared-default,
	impute_on=KIL_END_DATE,
	impute_method=mean
);

/* Runs the entire script end to end; apart from a first table correction */
/* Parameters:
	file	 = the file that you want to upload to caslib and perform analysis on
	provider = the provider of the desired table
	server   = the server of the provided table
	caslib 	 = the caslib that you want the table to be located in
	table    = the name of the table that you want to create in caslib/information catalog
	doc_path = the directory where you want the report to be stored
*/
%run_e2e(
	file=&test_file_1,
	provider=cas,
	server=cas-shared-default,
    caslib=CASUSER(grmelv),
    table=test_e2e_1,
	doc_path=&basepath/reports
);
