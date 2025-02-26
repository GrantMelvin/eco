/* Script path */
%include "/export/sas-viya/homes/grmelv/casuser/dq_module/eco_dq_v1.sas";

%let test_file_1 = /export/sas-viya/homes/grmelv/casuser/dq_module/test_files/test.sas7bdat;
%let test_file_2 = /export/sas-viya/homes/grmelv/casuser/dq_module/test_files/metadata_test.csv;
%let test_file_3 = /export/sas-viya/homes/grmelv/casuser/dq_module/test_files/Financial_Sample.xlsx;

%run_e2e(
	file=&test_file_1,
    caslib=CASUSER(grmelv),
    table=test_e2e_1,
	bot=test-bot-e2e_1,
	doc_path=/export/sas-viya/homes/grmelv/casuser/dq_module/reports
);
/* Uploads files to CAS */
/* Parameters:
	file   = the file you want to perform analysis on
	caslib = the caslib that you want to upload 
	table  = the table name of the file you are analyzing
*/
%import_data(
    &test_file_3,
    CASUSER(grmelv),
    table=test2
);

/* Creates and Runs bot for analysis in the SAS Catalog */
/* Parameters:
	table 	 = the table you want to evaluate
	bot_name = the name of the bot you want to create
	caslib 	 = the caslib that the table is located in
*/
%run_bots(
	test4, 
	test-bot-test4, 
	CASUSER(grmelv)
);

/* Retrieves the information catalog statistics for the given table */
/* Parameters:
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
*/
%get_statistics(
	testing_table_new_new, 
	CASUSER(grmelv)
);

/* Creates a report to get data quality statistics on the given table */
/* Parameters:
	sastable = the lib.table name to perform analysis on
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
	path	 = the directory where you want the report to be stored
*/
%generate_report(
	test_e2e_1,
	CASUSER(grmelv),
	/export/sas-viya/homes/grmelv/casuser/dq_module/reports
);

/* Produces a table that drops the rows if a specified variable is not present(as if it is a target)*/
/* Parameters:
	name	 = the name of the variable you want to treat as a target
	table 	 = the table you want to evaluate
	caslib 	 = the caslib that the table is located in
*/
/* %first_correction( */
/* 	zone, */
/* 	testing_table,  */
/* 	CASUSER(grmelv) */
/* ); */

/* Runs the entire script E2E*/
/* Parameters:
	file	 = the file that you want to upload to caslib and perform analysis on
	caslib 	 = the caslib that you have access to
	table    = the name of the table that you want to create in caslib/information catalog
	bot      = the name of the bot that you will create to perform the analysis
*/
%run_e2e(
	file=&test_file_1,
    caslib=CASUSER(grmelv),
    table=test_e2e_1,
	bot=test-bot-e2e_1,
	sastable=work.final_entities,
	path=/export/sas-viya/homes/grmelv/casuser
);
