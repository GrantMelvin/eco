libname casuser cas caslib=casuser;

/* View the first 100 rows of the abt_demo.sas7bdat file */
/* %let lib=mydata; */
/* %let table=abt_demo; */
/* libname &lib "/export/sas-viya/homes/grmelv/casuser"; */
/* data &lib.&table; */
/*     set "/export/sas-viya/homes/grmelv/casuser/abt_demo.sas7bdat"; */
/* run; */
/* proc print data=&lib.abt_demo(obs=100); run; */
/*---------------------*/

/* Get Catalog with API */
	%let BASE_URI=%sysfunc(getoption(servicesbaseurl));	
	%let desired_table = 'abt_demo';
	%let query=%sysfunc(urlencode(&desired_table));
	
	%put The base uri is &BASE_URI;
	%put The desired table is &desired_table;
	
	filename resp     temp;
	filename resp_hdr temp;
	
	%let server_name=%sysfunc(urlencode(cas-shared-default));

/* Gets the catalog ID we want */
/* proc http  */
/*     url="&BASE_URI/catalog/search?q=&query" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */

/* Saves the ID that we are interested in to desired_id */
/* data _null_; */
/*     set table_info; */
/*     call symputx('desired_id', id); */
/* run; */
/* %put The ID for &desired_table is &desired_id.; */

/* Gets the data source ID, provider ID*/
/* Can get the server and caslib IDs */
/* Can get whether the table is loaded or not */
/* proc http  */
/*     url="&BASE_URI/dataTables/dataSources/cas~fs~cas-shared-default~fs~CASUSER(grmelv)/tables/ABT_DEMO" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */

/* Load a table */
/* Allows us to look at a table*/
/* proc http  */
/*     url="&BASE_URI/casManagement/servers/cas-shared-default/caslibs/CASUSER(grmelv)/tables/ABT_DEMO/state?value=loaded" */
/*     method='PUT' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers "Content-Type"="application/json"; */
/* run; quit; */

/* Get info about that table */
/* Not that useful, but shows some stuff we can do with that table*/
/* proc http  */
/*     url="&BASE_URI/casManagement/servers/cas-shared-default/caslibs/CASUSER(grmelv)/tables/ABT_DEMO" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */

/* Get columns from that table*/
/* proc http  */
/*     url="&BASE_URI/casManagement/servers/cas-shared-default/caslibs/CASUSER(grmelv)/tables/ABT_DEMO/columns?start=0&limit=1000" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */

/* Get rows of table */
/* proc http */
/*     url="&BASE_URI/rowSets/tables/cas~fs~cas-shared-default~fs~CASUSER(grmelv)~fs~ABT_DEMO/rows/TESTING_TABLE_NEW" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */

%let server_name=%sysfunc(urlencode(cas-shared-default));
%let caslibs_name=%sysfunc(urlencode(CASUSER(grmelv)));
%let table_name=%sysfunc(urlencode(ABT_DEMO.sashdat));

/* Get summary statistics of that table*/
/* proc http  */
/*     url="&BASE_URI/casManagement/servers/&server_name/caslibs/&caslibs_name/tables/&table_name/summaryStatistics?start=0&limit=1000" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */


/* proc http  */
/*     url="&BASE_URI/catalog/statistics" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/* 	out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */


/* Produces the name of the instance in the link */
/* proc http  */
/*     url="&BASE_URI/catalog/search?q=testing_table_new" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers 'Content-Type' = 'application/json'; */
/* 	headers 'Accept' = 'application/json, application/vnd.sas.metadata.search.collection+json, application/vnd.sas.error+json'; */
/* run; quit; */


/* view produced: 13308fd1-7636-495a-9d8f-b5316235f3b3 */
/* proc http  */
/*     url="&BASE_URI/catalog/views/" */
/*     method='POST' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/* 	in='{ */
/*   "name": "test", */
/*   "label": "test", */
/*   "description": "testing", */
/*   "query": "match (t {id:\"1e2f32a7-0d82-48de-b289-ea196707e345\"})-[r:dataSetDataFields]->(col) match (col)-[s:semanticClassifications]-><c> match (col)<-[rt:termAsset]-<ta> match (col)-[r:topNCollectionsForDataField|bottomNCollectionsForDataField|fieldPatternCollectionsForDataField]->(e) return col,s,c,rt,ta,r,e" */
/* }' */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers 'Accept' = 'application/json, application/vnd.sas.metadata.definition.view+json, application/vnd.sas.error+json'; */
/* 	headers 'Content-Type' = 'application/json'; */
/* run; quit; */
/*  */
/* proc http  */
/*     url="&BASE_URI/catalog/instances/" */
/*     method='POST' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/* 	in='{ */
/*   "version": 1, */
/*   "query": "match (t {id:\"da5e7088-9abc-4645-839a-b9f5d434bec4\"})-[r:dataSetDataFields]->(col) match (col)-[s:semanticClassifications]-><c> match (col)<-[rt:termAsset]-<ta> match (col)-[r:topNCollectionsForDataField|bottomNCollectionsForDataField|fieldPatternCollectionsForDataField]->(e) return col,s,c,rt,ta,r,e" */
/* }' */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers 'Content-Type' = 'application/vnd.sas.metadata.instance.query+json'; */
/* 	headers 'Accept' = 'application/json, application/vnd.sas.metadata.instance.archive+json, application/vnd.sas.error+json'; */
/* run; quit; */

/* Get list of bots */
/* proc http */
/*     url="&BASE_URI/catalog/bots" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp; */
/* 	headers 'Content-Type' = 'application/json'; */
/*     debug level=3; */
/* run; */


/* Delete a bot */
/* proc http */
/*     url="&BASE_URI/catalog/bots/5c6fbca8-3958-492c-b072-0d50f70b5b82" */
/*     method='DELETE' */
/*     oauth_bearer=sas_services */
/*     out=resp; */
/* 	headers 'Accept' = 'application/vnd.sas.error+json'; */
/* run; */


/* Create a bot */
/* proc http  */
/*     url="&BASE_URI/catalog/bots/" */
/*     method='POST' */
/* 	in='{ */
/* 	  "provider": "TABLE-BOT", */
/* 	  "name": "Grant-Crawly", */
/* 	  "description": "Crawl my data source", */
/* 	  "parameters": { */
/* 	    "datasourceURI": "/dataSources/providers/cas/sources/cas-shared-default~fs~Public" */
/* 	  } */
/* 	}' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers 'Content-Type' = 'application/json'; */
/*     debug level=3; */
/* run; */

/* Create an adhoc analysis job for a bot */
/* proc http  */
/*     url="&BASE_URI/catalog/bots/adhocAnalysisJobs" */
/*     method='POST' */
/* 	in='{ */
/* 	  "provider": "TABLE-BOT", */
/* 	  "name": "Grant-Crawl adhoc analysis test", */
/* 	  "description": "Analyze some data source", */
/* 	  "resources": [ */
/* 		  { */
/* 		    "uri": "/dataTables/dataSources/cas~fs~cas-shared-default~fs~CASUSER(grmelv)/tables/ABT_DEMO.sashdat", */
/* 		    "type": "CASTable" */
/* 		  } */
/* 		], */
/* 	  "jobParameters": {} */
/* 	}' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers 'Content-Type' = 'application/json'; */
/*     debug level=3; */
/* run; */

/* Get an adhoc analysis job*/
/* proc http */
/*     url="&BASE_URI/catalog/bots/adhocAnalysisJobs?start=0&limit=100" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; */

/* Run a bot */
/* Runs Grant-Crawl on my library to analyze the abt_demo */
/* proc http */
/*     url="&BASE_URI/catalog/bots/40288891-db5c-4229-bb18-20712983e846/state?value=running" */
/*     method='PUT' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; */


/* Get a data source */
/* proc http  */
/*     url="&BASE_URI/dataSources/providers/cas/sources/cas-shared-default~fs~CASUSER(grmelv)" */
/*     method='GET' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* run; quit; */

/* Load a table */
/* proc http  */
/*     url="&BASE_URI/casManagement/servers/cas-shared-default/caslibs/Public/tables/ABT_DEMO.sashdat/state?value=loaded" */
/*     method='PUT' */
/*     oauth_bearer=sas_services */
/*     out=resp */
/*     headerout=resp_hdr */
/*     headerout_overwrite; */
/* 	headers "Content-Type"="application/json"; */
/* run; quit; */

libname resp json fileref=resp;

/* TODO: Do some changes to the given variable within the table */
/* %macro first_correction(name, table, caslib); */
/*  */
/*     %let table=%upcase(&table); */
/*     %let BASE_URI=%sysfunc(getoption(servicesbaseurl));     */
/*     %let encoded_table=%sysfunc(urlencode(&table)); */
/*     %let encoded_caslib=%sysfunc(urlencode(&caslib)); */
/* 	 */
/* 	filename resp temp; */
/* 	filename resp_hdr temp; */
/* 	filename payload temp; */
/* 	 */
/* 	for filtered get */
/* 	data _null_; */
/* 	    file payload; */
/* 	    put 'PLA_MONTH < 100';  */
/* 	run; */
/*  */
/* 		for filtered get */
/* 	proc http */
/* 	    url="&BASE_URI/casRowSets/servers/cas-shared-default/caslibs/&encoded_caslib/tables/&encoded_table/rows" */
/* 	    method='POST'  */
/* 	    in=payload  */
/* 	    oauth_bearer=sas_services */
/* 	    out=resp */
/* 	    headerout=resp_hdr */
/* 	    verbose;  */
/* 	    headers */
/* 	        'Accept' = 'application/json, application/vnd.sas.collection+json' */
/* 	        'Content-Type' = 'text/plain';  */
/* 	run; */
/*  */
/* 	proc http */
/* 	    url="&BASE_URI/casRowSets/servers/cas-shared-default/caslibs/&encoded_caslib/tables/&encoded_table/rows?start=0&limit=100" */
/* 	    method='POST'  */
/* 	    in=payload  */
/* 	    oauth_bearer=sas_services */
/* 	    out=resp */
/* 	    headerout=resp_hdr */
/* 	    verbose;  */
/* 	    headers */
/* 	        'Accept' = 'application/json, application/vnd.sas.collection+json' */
/* 	        'Content-Type' = 'text/plain';  */
/* 	run; */
/*  */
/* 	for delete */
/* 	data _null_; */
/* 	    file payload; */
/* 	    put '{'; */
/* 	    put '  "version": 1,'; 	     */
/* 	    put '  "where": PLA_MONTH < 10';  */
/* 	    put '}'; */
/* 	run; */
/*  */
/* 	data _null_; */
/* 	    infile payload; */
/* 	    input; */
/* 	    put _infile_; */
/* 	run; */
/*  */
/* 	proc http */
/* 	    url="&BASE_URI/casRowSets/servers/cas-shared-default/caslibs/&encoded_caslib/tables/&encoded_table/rows" */
/* 	    method='POST' */
/* 	    in=payload */
/* 	    oauth_bearer=sas_services */
/* 	    out=resp */
/* 	    headerout=resp_hdr */
/* 	    verbose; */
/*         headers 'Content-Type' = 'application/vnd.sas.cas.row.delete.request+json'; */
/*  		headers 'Accept' = 'application/vnd.sas.cas.row.delete.response+json, application/json, application/vnd.sas.collection+json, application/vnd.sas.error+json'; */
/* 	run; */
/*  */
/* 	data _null_; */
/* 	    infile resp; */
/* 	    input; */
/* 	    put _infile_; */
/* 	run; */
/*  */
/* 	libname resp json fileref=resp; */
/*  */
/* %mend first_correction; */