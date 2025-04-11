# Spond USe Case

## Repo Content
This repo contains several folders:
- Notebooks; divided in DDL (creation statements) and DML (ETL processes); considered the number of objects at the moment no further subfolders for medallion layer have been added, of course in a real case scenario the hierarchy could be expanded as desided.
- Modules: some sample modules with functions. Usually my approach is to write functions as much parametrized as possible and divide them by functionality (read, write, transform, connect to external tools..). At the moment some parts of the logic are still in the notebooks due to time constraints, I usually try to put everything in modules for better test coverage with Pytest.
- Tests: a sample unit test for one of functions contained in modules; this test are written using Pytest framework; the reason is that, using azure devops/github actions it is possible in a relatively easy way to develop tests for pyspark in databricks, execute them on a external agent with the help of databricks connect package (see devops point)
- Utilities: it contains a base notebook to help developing and executing unit test on databricks.
- Devops: I have mainly Azure Devops expertise, so for the sake of speed I wrote a basic devops pipeline to execute and show unit tests results. The same result can be achieved (with bit more of time for research) using GitHub actions. If you want to see it actually runnning  I can show it working on a sample repo during a call; the pipeline uses currently Devops PAT authentication, but creating a technical user it is possible to convert it to OAUTH2 authentication (don it in projects, not on personal repo). In addition to the unit testing the pipeline also performs linting and black format check.
- Workflows: very basic workflow example to put dependencies in place for key checks.


## Approach
In the past I worked a lot with customers with wanted everything in one big table, I am not a big fan of this kind of solution, so what I did is maintaining the 4 tables divided in the silver layer, performing some quality check on the columns provided and adding a few more.
I gave two samples of storing data approach, SCD1 and SCD2: using SCD2 on events and membership table it is possible to track changes made to the event (date moving, location changes) and traking user joining and leaving teams multiple time.

For the creation of the next layer in the medallion architecture (gold layer), I would probably proceed according to the points below:
- Create a calendar table (stillin silver layer) with weeks, months, year, quarter etc. columns and datekey relationship to join with the silver table on integer keys (created for most of the columns, way faster than greater/lesser join, to be discussed for SDC2 start/end dates according to the actual requirement)
- To provide insights related to the recent past period (1 month, 30 rolling days) I would create a set of detailed views/materialized views (according to the performance requirement) using high granularity key combination when needed.
- To provide historical trends I would create a set of gold tables with a lower granularity, removing the single memberships layer so that this table are not impacted by the deletion of user should they leave the teams and require data deletion 

## Possible improvements and limitation due to time costraint

- Creation of sample gold layer
- DBX for data quality applied in a systematic way
- Dedicated geographical library installed to avoid API call (struggled 30 minutes with mesa package, apparently is bugged at the moment, desisted as it had alreafy taken too much time ). In the current code I just harcode Oslo and Norway as county/country, there is the option to change a parameter in the fucntion calling in order to use a geo API to extract full info from latitude and longitude. If run using the API call with the full dataframe it will fail due to timeout issues, that is not the approach I would use in production.
- Full unit testing (just given a sample to show the framework)
- Adding integration testing after the full refresh has run
- Improving workflow definition with libraries, parameters, notifications
- Lakehouse federation for PostGre SQL ingestion/sync
- DAB files for actual deployment between environments
- User of service principal in the devops pipeline
- Test DLT (would really like this one, it is super good for performance, data quality and moneywise)


## How to run
I worked in a not UC enabled workspace: I tried to give both option in the table reference initialization in the DMLs, but there could be some typos. I tested each notebook execution and the workflow as well in my enviroment but there could be some differences.

Ideally you should:
- create the tables (both silver and bronze layer) in the catalog you prefer under the correct schema, upload the data to the bronze table (using Lakehouse federation stil step would not be needed at all, so I just created  bronze tables with the data already inside for sake of speed) 
- set catalogs and schema name in the DML files (this could be done with widget, but again I only had 3 hours)
- create a workflow based on .yaml file
- run the workflow 1 time to full ingest data from bronze to silver
- run it again to show that if there is no data change no data is written
- change data in bronze tables: UPDATE, DELETE (for scd2), INSERT
- tun again workflow to show that only changes are processed

