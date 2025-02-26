/* uploads a file to the caslib with the table name described */
%macro import_data(file, caslib, table=table);

	/* Silence warnings */
    %if %sysfunc(sessfound(mysession)) %then %do;
        cas mysession terminate;
    %end;
    cas mysession;

	/* File import for each type of file */
    %macro import_single_file(filepath, tablename);
		%let filename = %scan(&filepath, -1, "/");
        %let filetype = %scan(&filepath, -1, '.');
	    /* drop the table if it already exists */
		proc casutil;
			droptable casdata="&tablename" incaslib="&caslib" quiet;
		quit;
        proc casutil incaslib="&caslib";
			%let tablename = %upcase(&tablename);

            %if %upcase(&filetype) = CSV %then %do;
                load casdata="ECO/test_files/&filename"       
                importoptions=(filetype="csv" 
                               getnames="true" 
                               encoding="latin1")
                outcaslib="&caslib" 
                casout="&tablename"   
                replace;
            %end;
            %else %if %upcase(&filetype) = XLSX or %upcase(&filetype) = XLS %then %do;  
				load casdata="ECO/test_files/&filename"   
                importoptions=(filetype="excel" 
                               getnames="true")
                outcaslib="&caslib" 
                casout="&tablename"   
                replace;
            %end;
            %else %if %upcase(&filetype) = SAS7BDAT %then %do;
                load data="&filepath" 
                casout="&tablename"
                replace;
            %end;
			/* After all your load steps but before promote */
			list files;
			list tables incaslib="&caslib";
            promote casdata="&tablename" incaslib="&caslib" outcaslib="&caslib";
        quit;
    %mend;
    /* If we wanted to do more than one table*/
    %import_single_file(&file, &table);
%mend import_data;

/* Use Information catalog to generate baseline analysis */
%macro run_bots(table, bot, caslib);
	%let table=%upcase(&table);
	%let BASE_URI=%sysfunc(getoption(servicesbaseurl));	
    %let encoded_table=%sysfunc(urlencode(&table));
    %let encoded_bot=%sysfunc(urlencode(&bot));
	%let encoded_caslib=%sysfunc(urlencode(&caslib));

	filename resp     temp;
	filename resp_hdr temp;

	/* Load table into something from our public caslib */
	proc http 
	    url="&BASE_URI/casManagement/servers/cas-shared-default/caslibs/&encoded_caslib/tables/&encoded_table/state?value=loaded"
	    method='PUT'
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
		headers "Content-Type"="application/json";
	run; quit;

	/* Creates a bot for the specified table */
	proc http 
	    url="&BASE_URI/catalog/bots/"
	    method='POST'
		in=
		"{
          ""provider"": ""TABLE-BOT"",
          ""name"": ""&encoded_bot"",
          ""description"": ""Crawl my data source"",
          ""parameters"": {
            ""datasourceURI"": ""/dataSources/providers/cas/sources/cas-shared-default~fs~&encoded_caslib""
          }
        }"
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
		headers 'Content-Type' = 'application/json';
	run;

	/* Get list of bots for its ID*/
	proc http
	    url="&BASE_URI/catalog/bots"
	    method='GET'
	    oauth_bearer=sas_services
	    out=resp;
		headers 'Content-Type' = 'application/json';
	run;

	libname resp json fileref=resp;
	
	/* Gets the bot_id */
	data bot_id;
	    set resp.items;
	    where name = "&bot";
	    keep id;
	run;
	
	/* Puts the bot id into the macro */
	proc sql noprint;
	    select id
	    into :bot_id
	    from bot_id;
	quit;
	%put Bot ID: &bot_id;
	%let encoded_bot_id=%sysfunc(urlencode(&bot_id));

	/* Runs the discovery bot we just created */
	proc http
	    url="&BASE_URI/catalog/bots/&bot_id/state?value=running"
	    method='PUT'
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
	run;
	
	/* tells us if the bot is still running */
	/* have to do this even though we just ran it in case of errors */
	proc http
	    url="&BASE_URI/catalog/bots/state"
	    method='POST'
		in=
		"{
		  ""version"": 1,
		  ""template"": ""/catalog/bots/{id}"",
		  ""resources"": [
		    ""&encoded_bot_id""
		  ]
		}"
	    oauth_bearer=sas_services
	    out=resp;
		headers 'Content-Type' = 'application/json';
	run;

	libname resp json fileref=resp;

	/* Stores the status of the bot */
	data running_status;
	    set resp.alldata;
	    keep value;
	run;
	
	proc sql noprint;
	    select value into :running_status 
	    from running_status;
	quit;

	%put Initial Running Status: &running_status;

	/* Check the status of the bot */
	%let max_checks = 50;
	%let status = running;
	%let check_count = 0;
	
	/* Poll the bot every 10 seconds to see if it is done running */
	%do %while(&status eq running and &check_count lt &max_checks);
		%let check_count = %eval(&check_count + 1);

		proc http
		    url="&BASE_URI/catalog/bots/state"
		    method='POST'
			in=
			"{
			  ""version"": 1,
			  ""template"": ""/catalog/bots/{id}"",
			  ""resources"": [
			    ""&encoded_bot_id""
			  ]
			}"
		    oauth_bearer=sas_services
		    out=resp;
			headers 'Content-Type' = 'application/json';
		run;

		libname resp json fileref=resp;

		/* Stores the running status */
		data running_status;
		    set resp.alldata;
		    keep value;
		run;
		proc sql noprint;
		    select value into :status
		    from running_status;
		quit;

		%if &status eq running %then %do;
			data _null_;
				call sleep(10, 1);
			run;
		%end;
	%end;

	/* Check if bot completed */
	%if &status ne running %then %do;
		%put Bot completed successfully with status: &status;
	%end;
	%else %do;
		%put Bot did not complete within the timeout period.;
	%end;

	/* Run the adhoc analysis job */
	proc http 
	    url="&BASE_URI/catalog/bots/adhocAnalysisJobs"
	    method='POST'
		in='{
		  "provider": "TABLE-BOT",
		  "name": "&encoded_bot",
		  "description": "Analyze some data source",
		  "resources": [
			  {
			    "uri": "/dataTables/dataSources/cas~fs~cas-shared-default~fs~&encoded_caslib/tables/&encoded_table",
			    "type": "CASTable"
			  }
			],
		  "jobParameters": {}
		}'
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
		headers 'Content-Type' = 'application/json';
	run;

%mend run_bots;

/* Get statistics from the DQ part of the information catalog */
%macro get_statistics(table, caslib);
    /* Uppercase the table name for consistency */
    %let table=%upcase(&table);
    %let BASE_URI=%sysfunc(getoption(servicesbaseurl));    
    %let encoded_table=%sysfunc(urlencode(&table));
    %let encoded_caslib=%sysfunc(urlencode(&caslib));

	%put &instance is the instance;

    /* Create temporary files for response handling */
    filename resp temp;
    filename resp_hdr temp;

	/* Extracts the instance ID into a variable */
	proc http 
	    url="&BASE_URI/catalog/search?q=&encoded_table"
	    method='GET'
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
		headers 'Content-Type' = 'application/json';
		headers 'Accept' = 'application/json, application/vnd.sas.metadata.search.collection+json, application/vnd.sas.error+json';
	run; quit;

	filename jsonfile temp;

	/* Copy resp to JSON */
	data _null_;
	    infile resp;
	    file jsonfile;
	    input;
	    put _infile_;
	run;
	
	/* Use JSON libname engine */
	libname myjson json fileref=jsonfile;
	
	/* Extract the id where name matches table */
	data _null_;
	    set myjson.items;
	    where name = "&table"; /* Ensure case insensitivity */
	    call symputx('insance_id', id); /* Store the id in a macro variable */
	run;
	
	%put Instance ID: &insance_id;

	%let raw_query = '{
    "version": 1,
    "query": "match (t {id: \"9a505a5e-22c0-44ac-87d4-4d81ae7155f2\" })-[r:dataSetDataFields]->(col) 
                 match (col)-[s:semanticClassifications]->(c) 
                 match (col)<-[rt:termAsset]-(ta) 
                 match (col)-[r:topNCollectionsForDataField|bottomNCollectionsForDataField|fieldPatternCollectionsForDataField]->(e) 
                 return col,s,c,rt,ta,r,e"
}';

	/* TODO: Dynamically place the instance_id into the string */
	
    /* Execute HTTP request */
    proc http 
        url="&BASE_URI/catalog/instances/"
        method='POST'
        oauth_bearer=sas_services
        out=resp
        in=&raw_query
        headerout=resp_hdr
        headerout_overwrite;
        headers 'Content-Type' = 'application/vnd.sas.metadata.instance.query+json';
        headers 'Accept' = 'application/json, application/vnd.sas.metadata.instance.archive+json, application/vnd.sas.error+json';
    run;

    /* Process JSON response into SAS table */
    libname resp json fileref=resp;

    /* The stats for each feature */
    data work.attributes;
        set resp.entities_attributes;
		by ordinal_entities;
    run;

	/* The different features names*/
	data work.entities;
		set resp.entities;
		by ordinal_entities;
		keep name ordinal_entities;
	run;

	/* Merge on ordinal_entities */
	data work.final_entities;
	    merge work.entities (in=a)
	          work.attributes (in=b);
	    by ordinal_entities;
	    if a and b; /* Keep only matching records */
	run;

	proc print data=work.final_entities;
	run;

%mend get_statistics;

/* Generate a table report on characteristics of the analysis from bots */
%macro generate_report(table, caslib, doc_path);
    /* Uppercase the table name for consistency */
    %let table=%upcase(&table);
    %let BASE_URI=%sysfunc(getoption(servicesbaseurl));    
    %let encoded_table=%sysfunc(urlencode(&table));
    %let encoded_caslib=%sysfunc(urlencode(&caslib));
    /* Create temporary files for response handling */
    filename resp temp;
    filename resp_hdr temp;
	/* Get the total row count for this table */
	proc http
	    url="&BASE_URI/rowSets/tables/cas~fs~cas-shared-default~fs~&encoded_caslib~fs~&encoded_table/rows"
	    method='GET'
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
	run; quit;
	libname resp json fileref=resp;
	
	/* Extract root.count using JSON libname */
  	data _null_;
    	set resp.root; /* Access the root object */
	    total_count = count; /* Assign the count to a variable */
	    call symputx('total_count', total_count); /* Store in macro variable */
  	run;
	%put Total Count: &total_count; /* Log the value */
	
	/* Create a new dataset containing only features with completenessPercent < 50 */
	data CompletenessReport;
	  set work.final_entities;
	  where completenessPercent < 50 and completenessPercent is not missing;
   	  message = catx('',
            "Contains",
            trim(put(completenessPercent, best8.2)),
            "% of values relative to the dataset.");
	  type = 'Completeness';
	run;
	
	/* Check if hasOutliers exists in the dataset */
	proc contents data=work.final_entities out=contents noprint; run;
	
	%let has_outlier_var = 0;
	data _null_;
	  set contents;
	  if upcase(name) = 'HASOUTLIERS' then call symputx('has_outlier_var', 1);
	run;
	
	/* Create a new dataset containing only features with outlierPercent > 10 */
	%if &has_outlier_var = 1 %then %do;
	  data OutlierReport;
	    set work.final_entities;
	    outlierPercent = (nOutliers / &total_count) * 100;
    	message = catx('', 
	            "Contains",
	            trim(put(outlierPercent, best8.2)),
	            "% of values as outliers.");
	    where hasOutliers = 1;
	    rename nOutliers=outlierCount;
	    type = 'Outlier';
	  run;
	%end;
	%else %do;
	  /* Create empty outlier report if hasOutliers doesn't exist */
	  data OutlierReport;
	    length name $256 type $50 message $200;
	    ordinal_entities = .;
	    outlierCount = .;
	    outlierPercent = .;
	    call missing(name, type, message);
	    stop;
	  run;
	%end;
	
	/* Create a new dataset containing only features with mismatchedPercent > 25 */
	data mismatchReport;
	    set work.final_entities;
	    mismatchedPercent = (mismatchedCount / &total_count) * 100;
		message = catx('',
		              "Contains",
		              trim(put(mismatchedPercent, best8.2)),
		              "% of values as incorrect type.");
		type = 'Mismatched';
	run;
	
	%if &has_outlier_var = 1 %then %do;
	  proc print data=OutlierReport noobs;
	      title "Outliers > 10%";
	      var ordinal_entities name outlierCount outlierPercent message;
		  where outlierPercent > 10;
	  run;
	%end;
	
  	proc print data=CompletenessReport noobs;
     	title "Completeness < 50%";
		var ordinal_entities name missingCount completenessPercent message;
  	run;
	
	proc print data=mismatchReport noobs;
     	title "Mismatched > 25%";
		var ordinal_entities name mismatchedCount mismatchedPercent message;
		where mismatchedPercent > 25;
  	run;
	
	/* Combine the three reports using SQL */
	proc sql;
	  create table CombinedReport as
	    %if &has_outlier_var = 1 %then %do;
	    /* Outlier rows */
	    select name, 'Outlier' as error_type, message
	    from OutlierReport
	    where outlierPercent > 10
	    union all
	    %end;
	    /* Completeness rows */
	    select name, 'Completeness' as error_type, message
	    from CompletenessReport
	    union all
	    /* Mismatched rows */
	    select name, 'Mismatched' as error_type, message
	    from mismatchReport
	    where mismatchedPercent > 25
	  ;
	quit;
	
	/* Render as a PDF */
	ods pdf file="&doc_path/&table._report.pdf" style=Journal;
	  proc print data=CombinedReport noobs;
	    title "Data Quality Report on &table";
	    var name error_type message;
	  run;
	ods pdf close;
	
%mend generate_report;

/* Impute in a way defined by the user */
%macro first_correction(table, caslib, impute_on=impute_on, impute_method=impute_method);

    %let table=%upcase(&table);
    %let BASE_URI=%sysfunc(getoption(servicesbaseurl));    
    %let encoded_table=%sysfunc(urlencode(&table));
    %let encoded_caslib=%sysfunc(urlencode(&caslib));
	
	filename resp temp;
	filename resp_hdr temp;
	filename payload temp;

	%put Impute on: &impute_on;
	%put Method: &impute_method;
		
	/* Get columns from that table*/
	proc http 
	    url="&BASE_URI/casManagement/servers/cas-shared-default/caslibs/&encoded_caslib/tables/&encoded_table/columns?start=0&limit=1000"
	    method='GET'
	    oauth_bearer=sas_services
	    out=resp
	    headerout=resp_hdr
	    headerout_overwrite;
	run; quit;
	

	libname resp json fileref=resp;

%mend first_correction;


/* E2E test */
%macro run_e2e(file=file, caslib=caslib, table=table, bot=bot, doc_path=path);
    %put NOTE: Parameter values:;
    %put NOTE: file=&file;
    %put NOTE: caslib=&caslib;
    %put NOTE: table=&table;
    %put NOTE: bot=&bot;
	%put NOTE: doc_path=&doc_path;

    %import_data(
	    &file,
	    &caslib,
	    table=&table
	);

	%run_bots(
		&table, 
		&bot, 
		&caslib
	);

	%get_statistics(
		&table, 
		&caslib
	);

	%generate_report(
		&table,
		&caslib,
		&doc_path
	);

%mend run_e2e;