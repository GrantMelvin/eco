/* Get the current directory path & URL*/
%let fullpath = &_SASPROGRAMFILE;
%let basepath = %substr(&fullpath, 1, %index(&fullpath, e2e_test.sas) - 2);
%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

/* Data Quality Module */
%include "&basepath/../eco_dq_v1.sas";

/* Test files for analysis */
%let test_file_1 = &basepath/Test_Files/abt_demo.sas7bdat;
%let test_file_2 = &basepath/Test_Files/vars_meta_data_in_DEMO.xlsx;
%let test_file_3 = &basepath/Test_Files/Financial_Sample.xlsx;
%let test_file_4 = &basepath/Test_Files/all-approved_oncology_drugs.xlsx;
%let test_file_5 = &basepath/Test_Files/Diabetes_Missing_Data.csv;

/* Test parameters for analysis */
%let test_table = test_e2e_5;			/* The name of the table in Information Catalog */
%let caslib 	= CASUSER(grmelv);		/* The name of the caslib you have access to    */
%let provider 	= cas;					/* The name of the provider you have access to  */
%let server 	= cas-shared-default;	/* The name of the server you have access to  	*/
%let doc_path 	= &basepath/Reports;	/* The directory to save the PDF reports to  	*/

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
	file=&test_file_5,
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
	impute_method 	   = the method of imputation that you'd like to use (Supported: Mean, Median)
*/
%first_correction(
	&BASE_URI,
	&test_table, 
	&caslib,
	&provider,
	&server,
	deletion_threshold=50,
	impute_on=Serum_Insulin,
	impute_method=mean
);
