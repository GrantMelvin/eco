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
%let test_table = test_e2e_1;			/* The name of the table in Information Catalog */
%let caslib 	= CASUSER(grmelv);		/* The name of the caslib you have access to    */
%let provider 	= cas;					/* The name of the provider you have access to  */
%let server 	= cas-shared-default;	/* The name of the server you have access to  	*/
%let doc_path 	= &basepath/Reports;	/* The directory to save the PDF reports to  	*/

/* Runs the entire script end to end; apart from a first table correction */
/* Parameters:
	file	 			   = The file that you want to upload to caslib and perform analysis on
	provider 			   = The provider of the desired table
	server   			   = The server of the provided table
	caslib 	 			   = The caslib that you want the table to be located in
	table                  = The name of the table that you want to create in caslib/information catalog
	doc_path 			   = The directory where you want the report to be stored
	completeness_threshold = The feature has to contain at least this % of values or be flagged
	outlier_threshold 	   = The feature cannot contain more than this % of outliers or be flagged
	mismatch_threshold 	   = The feature cannot contain more than this % of mismatched types or be flagged
*/
%run_e2e(
	file=&test_file_1,
	provider=&provider,
	server=&server,
    caslib=&caslib,
    table=&test_table,
	doc_path=&doc_path,
	completeness_threshold=30,
	outlier_threshold=10,
	mismatch_threshold=25
);


