/* Get the current directory path & URL*/
%let fullpath = &_SASPROGRAMFILE;
%let basepath = %substr(&fullpath, 1, %index(&fullpath, e2e_test.sas) - 2);
%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

/* Data Quality Module */
%include "&basepath/../eco_dq_v1.sas";

/* Test parameters for analysis */
%let test_table = test_e2e_1;			/* The name of the table in Information Catalog */
%let caslib 	= CASUSER(grmelv);		/* The name of the caslib you have access to    */
%let provider 	= cas;					/* The name of the provider you have access to  */
%let server 	= cas-shared-default;	/* The name of the server you have access to  	*/

/* Produces a table in the Work lib that imputes on the desired variable and drops rows that have a certain % of values missing*/
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
	deletion_threshold=5,
	impute_on=SHA_39,
	impute_method=mean
);