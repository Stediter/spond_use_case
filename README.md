# spond_use_case

This repo contains several folders:
- Notebooks (.py file format for better readability in azure devops/github): divided in DDL (creation statements) and DML (ETL processes); considered the number of objects at the moment no further subfolders for medallion layer have been added, of course in a real case scenario the hierarchy could be expanded as desided.
- Modules: some sample modules with functions. Usually my approach is to write functions as much parametrized as possible and divide them by functionality (read, write, transform, connect to external tools..). At the moment some parts of the logic are still in the notebooks due to time constraints, I usually try to put everything in modules for better test coverage with Pytest.
- Tests: a few sample tests for the functions contained in modules (so unit tests); this test are written using Pytest framework; the reason is that, using azure devops/github actions it is possible in a relatively easy way to develop tests for pyspark in databricks, execute them on a external agent with the help of databricks connect package
- Utilities: it contains a base notebook to help developing and executing unit test on databricks.
- Devops: I have mainly Azure Devops expertise, so for the sake of speed I wrote a basic devops pipeline to execute and show unit tests results. The same result can be achieved (with bit more of time) using GitHub actions. If you want to see it actually runnning  I can show it working on a sample repo during a call; the pipeline uses currently Devops PAT authentication, but creating a technical user it is possible to convert it to OAUTH2 authentication (don it in projects, not on personal repo). In addition to the unit testing the pipeline also performs linting and black format check


