/* Get the current directory path */
%let fullpath = &_SASPROGRAMFILE;
%let basepath = %substr(&fullpath, 1, %index(&fullpath, workflow_test.sas) - 2);
%put &basepath;
%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

/* Data Quality Module */
%include "&basepath/eco_dq_v1.sas";

/* Test files for analysis */
%let test_file_1 = &basepath/test_files/test.sas7bdat;
%let test_file_2 = &basepath/test_files/metadata_test.csv;
%let test_file_3 = &basepath/test_files/Financial_Sample.xlsx;
%let test_file_4 = &basepath/test_files/all-approved_oncology_drugs.xlsx;
%let test_file_5 = &basepath/test_files/qol_life_data.xlsx;

/* Test parameters for analysis */
%let test_table = test_e2e_1;
%let caslib = CASUSER(grmelv);
%let provider = cas;
%let server = cas-shared-default;
%let doc_path = &basepath/reports;

/* Uploads and promotes a file to CAS */
/* Parameters:
	file   = file path you want to perform analysis on (sas7bdat, csv, xls, xlsx supported)
	caslib = target caslib to upload to (best with casuser)
	table  = desired table name for file
*/
%import_data(
    &test_file_1,
    &caslib,
    table=&test_table
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
	&test_table,  
	&caslib,
	&provider,
	&server
);

/* Retrieves the information catalog statistics for the given table */
/* Parameters:
	BASE_URI = the base path of the SAS Viya site
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
*/
%get_statistics(
	&BASE_URI,
	&test_table, 
	&caslib
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
	&test_table,
	&caslib,
	&provider,
	&server,
	&doc_path
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
	provider=&provider,
	server=&server,
    caslib=&caslib,
    table=&test_table,
	doc_path=&doc_path
);

/* Produces a table that imputes on the desired variable and drops rows that have a certain % of values missing*/
/* Parameters:
	BASE_URI 	  	   = the base path of the SAS Viya site
	table 	  	  	   = the table you want to evaluate
	caslib 	  	  	   = the caslib that the table is located in
	provider  	  	   = the provider of the desired table
	server    	  	   = the server of the provided table
	deletion_threshold = if a row exceeds this % of missing values it will be deleted
	impute_on     	   = the array of variable names that you want to perform imputation on
	impute_method 	   = the method of imputation that you'd like to use
*/
%first_correction(
	&BASE_URI,
	&test_table, 
	&caslib,
	&provider,
	&server,
	deletion_threshold=5,
	impute_on=PRE_23 PRE_20,
	impute_method=mean
);
