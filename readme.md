# ClearScore Technical Assigment

repository to ingest ClearScore customer data, perform stat calculations,
and transformations.

## Requirements

* java 1.8.x
* scala 2.11.x
* sbt 1.4.4
* spark 2.3.1 (installation instructions, windows: https://phoenixnap.com/kb/install-spark-on-windows-10 linux: https://phoenixnap.com/kb/install-spark-on-ubuntu)
* 7zip or equivalent to extract compressed raw data

## Initial Set-up
extract the compressed raw data to a sensible location on the machine

to compile the fat jars in order to run the spark jobs run `sbt buildJars`. this compiles the code, runs unit tests and assembles a fat jar to be used in a spark-submit

in `scripts/config/clearscore.conf` update `dataRoot` to point to directory containing bulk-reports of the extracted raw data.
eg. if your file structure looks like `/data/clearscore/bulk-reports` then update the config file to contain
`/data/clearscore/`

## Compute Statistics

navigate to the scripts directory in the repository. To run all the jobs end to end and output all stats run:

windows:
`.\runSparkSubmit.cmd RunAllStats`

linux: 
`./runSparkSubmit.sh RunAllStats`

this runs the following batch spark jobs in the following order:

* Data ingestion and preparation
    * ImportAccounts - ingest account data, removing corrupt records
    * ImportReports - ingest report data, removing corrupt records and selecting only necessary rows
    * GetLatestReport - filter only the most recent report for each user
* Stat Calculations:
    * AverageCreditScore - mean Score accross all reports (not just latest)
    * EmploymentStatus - number of distinct users grouped by their Employment Status
    * ScoreBucketing - Number of distinct users (from latest reports) in score ranges (the range is configurable, default is 50)
    * UserSummaries - Summary information for each user (based off their latest report)
    
this will output the stats to `$scoreRoot/stats/`

each job can be run individually using `./runSparkSubmit.sh JobName`. eg `./runSparkSubmit.sh EmploymentStatus`.

Note: the Data ingestion steps must be run before the stat calculations, the stats may be executed in any order.

Note: Jobs are set to run with very little memory on a local system, extra configuration will be needed (in the runSparkSubmit batch/shell scripts)
to run on a proper distributed system.

## Improvements

* Integration tests - run a Spark Testing Suite which runs the above jobs on a small known test dataset, through sbt, to check the outputs match expected values and that jobs run correctly.
* Improved Automation - orchestrate jobs using Airflow instead of .cmd or .sh
* load user summaries to database to help others interact with the summary data.
* Import reports may not function so well on some file systems (i.e. HDFS) check this, and edit code accordingly, to do different things for different file systems.
* separate non-spark jobs to merge and prettify output stats (currently they´re in a standard not-so-friendly partitioned format)