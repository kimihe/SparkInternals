README
-------------------------

# Enter into porject `SparkWordCount`
* `cd /Path/To/SparkWordCount`

# Build the poject with sbt
* `sbt clean package`

# Submit task to spark
* `/Path/To/Spark-Home/bin/spark-submit --class "WordCount"  target/scala-2.11/sparkwordcount_2.11-1.0.jar`

# Out Put
A directory called `helloOutput` will be generated in current path.