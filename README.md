# SparkInternals
Research on Apache Spark.

# Prerequisites
The Maven-based build is the build of reference for Apache Spark. Building Spark using Maven requires Maven 3.3.9 or newer and Java 8+. Note that support for Java 7 was removed as of Spark 2.2.0.

# How to compile
* Replace the original modules in Spark-2.2.0 with our modified source codes.
* Configure Maven to use more memory than usual by setting MAVEN_OPTS: 

```
"export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"
```
* Build project using maven:

```
./build/mvn -DskipTests clean package
```

# Other Documents
* About building spark: [Building Spark](https://spark.apache.org/docs/latest/building-spark.html).