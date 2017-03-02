# IRS Form 909 Aggregation with Apache Spark

Simple Apache Spark jobs for aggregating the IRS Form 909 dataset
available [here](https://aws.amazon.com/es/public-datasets/irs-990/).
Jobs include average and median calculation in a nationwide and per-state
basis.

## Dependencies
- Scala 2.11
- sbt 0.13.8
- Apache Spark 2.1 for Hadoop 2.4

More recent Hadoop versions are not supported because of a bug
related to S3 support.
See more about it [here](https://issues.apache.org/jira/browse/SPARK-7442).

## Instructions to run
1. Start the Spark master
`$SPARK_HOME/sbin/start-master.sh`
1. Start a Spark slave
`$SPARK_HOME/sbin/start-slave.sh MASTER_URL`
1. Build the fat jar `sbt clean assembly`
1. Deploy the jar `$SPARK_HOME/bin/spark-submit  --class "IrsRevenueApp" --master "MASTER_URL" --executor-memory MAX_MEMORY target/scala-2.11/irs-revenue-assembly-1.0.jar`

