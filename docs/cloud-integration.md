---
layout: global

displayTitle: Integration with Cloud Infrastructures
title: Integration with Cloud Infrastructures
description: Introduction to cloud storage support in Apache Spark SPARK_VERSION_SHORT
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

* This will become a table of contents (this text will be scraped).
{:toc}

## Introduction

Apache Spark can use cloud object stores as a source or destination of data. It does so
through filesystem connectors implemented in apache Hadoop. Provided the relevant libraries
are on the classpath, a file can be referenced simply via its URL

```scala
sparkContext.textFile("s3a://landsat-pds/scene_list.gz").count()
```

Similarly, an RDD can be saved to an object store via `saveAsTextFile()`


```scala
val numbers = sparkContext.parallelize(1 to 1000)
// save to Amazon S3 (or compatible implementation)
numbers.saveAsTextFile("s3a://testbucket/counts")
// save to an OpenStack Swift implementation
numbers.saveAsTextFile("swift://testbucket.rackspace/counts")
// Save to Azure Object store
numbers.saveAsTextFile("wasb://testbucket@example.blob.core.windows.net/counts")
```

While Object stores can be used as the source and destination of data, they cannot be
used as a direct replacement for a cluster-wide filesystem, such as HDFS.
This is important to know, as the fact they are easy to work with can be misleading.

## Cloud object stores are not filesystems

Object stores are not filesystems: they are not a hierarchical tree of directories and files.

The Hadoop filesystem APIs offer a filesystem API to the object stores, but underneath
they are still object stores, [and the difference is significant](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

Many behaviors expected of a filesystem are emulated in the object store APIs, but only
imperfectly.

### Directory operations may not be atomic nor fast

Directory rename and delete may be performed as a series of operations on the client. Specifically,
`delete(path, recursive=true)` may be implemented as "list the objects, delete them singly or in batches".
`rename(source, dest)` may be implemented as "copy all the objects" followed by the delete operation.

1. They may fail part way through, leaving the status of the filesystem "undefined".
1. The time to delete may be `O(files)`
1. The time to rename may be `O(data)`. If the rename is done on the client, the time to rename
each file will depend upon the bandwidth between client and the filesystem. The further away the client
is, the longer the rename will take.
1. Recursive directory listing can be very slow. This can slow down some parts of job submission
and execution.

Because of these behaviours, committing of work by renaming directories is neither efficient nor
reliable. There is a special output committer for Parquet,
the `org.apache.spark.sql.execution.datasources.parquet.DirectParquetOutputCommitter`
which bypasses the rename phase.

*Critical* speculative execution does not work against object
stores which do not support atomic directory renames. Your output may get
corrupted.

*Warning* even non-speculative execution is at risk of leaving the output of a job in an inconsistent
state if a "Direct" output committer is used and executors fail.

### Data may not be written until the output stream's `close()` operation.

Data to be written to the object store is usually buffered to a local file or stored in memory,
until one of: there is enough data to create a partition in a multi-partitioned upload (where enabled),
or when the output stream's `close()` operation is done.

- If the process writing the data fails, no data at all may have been saved to the object store.
- Data may be visible in the object store until the entire output stream is complete
- There may not be an entry in the object store for the file (even a 0 byte one) until
that stage.

### An object store may display eventual consistency

Object stores are often *Eventually Consistent*. This can surface, in particular:-

- When listing "a directory"; newly created files may not yet be visible, deleted ones still present.
- After updating an object: opening and reading the object may still return the previous data.
- After deleting an obect: opening it may succeed, returning the data.
- While reading an object, if it is updated or deleted during the process.

For many years, Amazon US East S3 lacked create consistency: attempting to open a newly created object
could return a 404 response, which Hadoop maps to a `FileNotFoundException`. This was fixed in August 2015
—see [S3 Consistency Model](http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel)
for the full details.

### Read operations may be significantly slower than normal filesystem operations.

Object stores usually implement their APIs as HTTP operations; clients make HTTP(S) requests
and block for responses. Each of these calls can be expensive. For maximum performance

1. Try to list filesystem paths in bulk.
1. Know that `FileSystem.getFileStatus()` is expensive: cache the results rather than repeat
the call (or wrapper methods such as `FileSystem.exists(), isDirectory() or isFile()`).
1. Try to forward seek through a file, rather than backwards.
1. Avoid renaming files: This is slow and, if it fails, may fail leave the destination in a mess.
1. Use the local filesystem as the destination of output which you intend to reload in follow-on work.
Retain the object store as the final destination of persistent output, not as a replacement for
HDFS.

## Object stores and their library dependencies

The different object stores supported by Spark depend on specific Java libraries.

### Amazon S3 with s3a://

The "S3A" filesystem is a connector with Amazon S3, initially implemented in Hadoop 2.6, and
considered ready for production use in Hadoop 2.7.

The implementation is `hadoop-aws`, which is included in the `spark-assembly` JAR when spark
is built against Hadoop 2.7 or later.

Dependencies: `amazon-aws-sdk` JAR (Hadoop 2.6 and 2.7); `amazon-s3-sdk` and `amazon-core-sdk`
in Hadoop 2.8. *Warning*: The Amazon JARs have proven very brittle —the version of the Amazon
libraries must match that which the Hadoop binaries were built against.

### Amazon S3 with s3n://

The "S3N" filesystem connector is a long-standing connector shipping with all versions of Hadoop 2.
It uses the `jets3t` library to talk to HDFS; this must be on the classpath.

Note that S3N is effectively unmaintained by the Hadoop team and, on Hadoop 2.7+ is deprecated in
favor of S3A. Only critical security issues are being fixed on S3N.

### Microsoft Azure with wasb://

The `wasb` filesystem connector is implemented in `hadoop-aws` and built into the `spark-assembly`
JAR. It needs the `azure-storage` JAR on the classpath.

### Openstack Swift

The `swift` filesystem connector is implemented in `hadoop-openstack`.

## Testing Cloud integration

The `spark-cloud` module contains tests which can run against the object stores. These verify
functionality integration and performance.

### Example Configuration for testing cloud data


This is a configuration enabling the S3A and Azure test, referencing the secret credentials
kept in another file.

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///home/hadoop/.ssh/auth-keys.xml"/>

  <property>
    <name>aws.tests.enabled</name>
    <value>true</value>
    <description>Flag to enable AWS tests</description>
  </property>

  <property>
    <name>s3a.test.uri</name>
    <value>s3a://testplan1</value>
    <description>S3A path to a bucket which the test runs are free to write, read and delete
    data.</description>
  </property>

  <property>
    <name>azure.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>azure.test.uri</name>
    <value>wasb://MYCONTAINER@TESTACCOUNT.blob.core.windows.net</value>
  </property>

</configuration>
```

The configuration uses XInclude to pull in the secret credentials for the account
from the user's `/home/hadoop/.ssh/auth-keys.xml` file:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>USERKEY</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>SECRET_AWS_KEY</value>
  </property>
  <property>
    <name>fs.azure.account.key.TESTACCOUNT.blob.core.windows.net</name>
    <value>SECRET_AZURE_KEY</value>
  </property>
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, with reduced risk.


## Large dataset input tests

Some tests read from large datasets; some simple IO of a multi GB source file,
followed by actual parsing operations of CSV files.

### Amazon S3 test datasets

When testing against Amazon S3, their [public datasets](https://aws.amazon.com/public-data-sets/)
are used. Specifically

* Large object input.
* CSV parsing: `http://landsat-pds.s3.amazonaws.com/scene_list.gz`, which can be referenced
as an S3A file as `s3a://landsat-pds/scene_list.gz`


## Running a single test case

Each cloud test takes time; it is convenient to be able to work on a single test case at a time
when implementing or debugging a test.

Every test has a *key* name which SHOULD BE unique within the specific test class; it MAY BE
even across the entire module —though this does not hold for subclassed tests.

As an example, here is a test create and save a test data to an object store using the
Hadoop filesystem API via the RDD function `saveAsNewAPIHadoopFile()`

```scala

  ctest("NewHadoopAPI", "New Hadoop API",
    "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val example1 = new Path(TestDir, "example1")
    saveAsTextFile(numbers, example1, sc.hadoopConfiguration)
  }
```

The test is defined with a key, `NewHadoopAPI`, a name for the XML/HTML reports,
`New Hadoop API`, and a description for logging (and perhaps future XML reports).


This method can be exclusively executed by passing it to maven in the property `test.method.keys`

```

# running all (possibly subclassed) instantations of this method in scalatest suites.
mvn test -Phadoop-2.7 -Dcloud.test.configuration.file=cloud.xml -Dtest.method.keys=NewHadoopAPI

# running the test purely in the S3A suites
mvn test -Phadoop-2.7 -DwildcardSuites=org.apache.spark.cloud.s3.S3aIOSuite -Dcloud.test.configuration.file=cloud.xml

# running two named tests across all filesystems
mvn test -Phadoop-2.7 -Dcloud.test.configuration.file=cloud.xml -Dtest.method.keys=NewHadoopAPI,CSVgz

# test run against Hadoop "branch-2"
mvt -Phadoop-2.7  -Dcloud.test.configuration.file=../cloud.xml -Dhadoop.version=2.9.0-SNAPSHOT
```

The combination of scalatest naming via the `wildcardSuites` property with the test-case specific
key allows developers to easily focus on the failure or performance issues of a single test case
within the module

## Best practices for adding a new test

1. Use the `ctest()` declaration of a test case conditional on the suite being enabled.
1. Give it a uniqe key using upper-and-lower-case letters and numerals only.
1. Give it a name useful in test reports/bug reports
1. Give it a meaningful description.
1. Test against multiple infrastructure instances.


## Best practices for adding a new test suite

1. Extend `CloudSuite`
1. Have an `after {}` clause which cleans up all object stores —this keeps costs down.
1. Support parallel operation.
1. Do not assume that any test has exclusive access to any part of an object store other
than the specific test directory. This is critical to support parallel test execution.
1. Share setup costs across test cases, especially for slow directory/file setup operations.


## Test costs

S3 incurs charges for storage and for IO out of the datacenter where the data is stored.

The tests try to keep costs down by not working with large amounts of data, and by deleting
all data on teardown. If a test run is aborted, data may be retained on the test filesystem.
While the charges should only be a small amount, period purges of the bucket will keep costs down.

Rerunning the tests to completion again should achieve this.

The large dataset tests read in public data, so storage and bandwidth costs
are incurred by Amazon themselves.
=======

All major cloud providers offer persistent data storage in *object stores*.
These are not classic "POSIX" file systems.
In order to store hundreds of petabytes of data without any single points of failure,
object stores replace the classic file system directory tree
with a simpler model of `object-name => data`. To enable remote access, operations
on objects are usually offered as (slow) HTTP REST operations.

Spark can read and write data in object stores through filesystem connectors implemented
in Hadoop or provided by the infrastructure suppliers themselves.
These connectors make the object stores look *almost* like file systems, with directories and files
and the classic operations on them such as list, delete and rename.


### Important: Cloud Object Stores are Not Real Filesystems

While the stores appear to be filesystems, underneath
they are still object stores, [and the difference is significant](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

They cannot be used as a direct replacement for a cluster filesystem such as HDFS
*except where this is explicitly stated*.

Key differences are:

* Changes to stored objects may not be immediately visible, both in directory listings and actual data access.
* The means by which directories are emulated may make working with them slow.
* Rename operations may be very slow and, on failure, leave the store in an unknown state.
* Seeking within a file may require new HTTP calls, hurting performance. 

How does this affect Spark? 

1. Reading and writing data can be significantly slower than working with a normal filesystem.
1. Some directory structures may be very inefficient to scan during query split calculation.
1. The output of work may not be immediately visible to a follow-on query.
1. The rename-based algorithm by which Spark normally commits work when saving an RDD, DataFrame or Dataset
 is potentially both slow and unreliable.

For these reasons, it is not always safe to use an object store as a direct destination of queries, or as
an intermediate store in a chain of queries. Consult the documentation of the object store and its
connector to determine which uses are considered safe.

In particular: *without some form of consistency layer, Amazon S3 cannot
be safely used as the direct destination of work with the normal rename-based committer.*

### Installation

With the relevant libraries on the classpath and Spark configured with valid credentials,
objects can be read or written by using their URLs as the path to data.
For example `sparkContext.textFile("s3a://landsat-pds/scene_list.gz")` will create
an RDD of the file `scene_list.gz` stored in S3, using the s3a connector.

To add the relevant libraries to an application's classpath, include the `hadoop-cloud` 
module and its dependencies.

In Maven, add the following to the `pom.xml` file, assuming `spark.version`
is set to the chosen version of Spark:

{% highlight xml %}
<dependencyManagement>
  ...
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>hadoop-cloud_2.11</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
  </dependency>
  ...
</dependencyManagement>
{% endhighlight %}

Commercial products based on Apache Spark generally directly set up the classpath
for talking to cloud infrastructures, in which case this module may not be needed.

### Authenticating

Spark jobs must authenticate with the object stores to access data within them.

1. When Spark is running in a cloud infrastructure, the credentials are usually automatically set up.
1. `spark-submit` reads the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
and `AWS_SESSION_TOKEN` environment variables and sets the associated authentication options
for the `s3n` and `s3a` connectors to Amazon S3.
1. In a Hadoop cluster, settings may be set in the `core-site.xml` file.
1. Authentication details may be manually added to the Spark configuration in `spark-defaults.conf`
1. Alternatively, they can be programmatically set in the `SparkConf` instance used to configure 
the application's `SparkContext`.

*Important: never check authentication secrets into source code repositories,
especially public ones*

Consult [the Hadoop documentation](https://hadoop.apache.org/docs/current/) for the relevant
configuration and security options.

## Configuring

Each cloud connector has its own set of configuration parameters, again, 
consult the relevant documentation.

### Recommended settings for writing to object stores

For object stores whose consistency model means that rename-based commits are safe
use the `FileOutputCommitter` v2 algorithm for performance:

```
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version 2
```

This does less renaming at the end of a job than the "version 1" algorithm.
As it still uses `rename()` to commit files, it is unsafe to use
when the object store does not have consistent metadata/listings.

The committer can also be set to ignore failures when cleaning up temporary
files; this reduces the risk that a transient network problem is escalated into a 
job failure:

```
spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored true
```

As storing temporary files can run up charges; delete
directories called `"_temporary"` on a regular basis to avoid this.

### Parquet I/O Settings

For optimal performance when working with Parquet data use the following settings:

```
spark.hadoop.parquet.enable.summary-metadata false
spark.sql.parquet.mergeSchema false
spark.sql.parquet.filterPushdown true
spark.sql.hive.metastorePartitionPruning true
```

These minimise the amount of data read during queries.

### ORC I/O Settings

For best performance when working with ORC data, use these settings:

```
spark.sql.orc.filterPushdown true
spark.sql.orc.splits.include.file.footer true
spark.sql.orc.cache.stripe.details.size 10000
spark.sql.hive.metastorePartitionPruning true
```

Again, these minimise the amount of data read during queries.

## Spark Streaming and Object Storage

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` to monitor a path in the store through a call to
`StreamingContext.textFileStream()`.

1. The time to scan for new files is proportional to the number of files
under the path, not the number of *new* files, so it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a workflow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.

1. In case of the default checkpoint file manager called `FileContextBasedCheckpointFileManager`
streams should only be checkpointed to a store implementing a fast and
atomic `rename()` operation. Otherwise the checkpointing may be slow and potentially unreliable.
On AWS S3 with Hadoop 3.3.1 or later using the S3A connector the abortable stream based checkpoint
file manager can be used (by setting the `spark.sql.streaming.checkpointFileManagerClass`
configuration to `org.apache.spark.internal.io.cloud.AbortableStreamBasedCheckpointFileManager`)
which eliminates the slow rename. In this case users must be extra careful to avoid the reuse of
the checkpoint location among multiple queries running parallelly as that could lead to corruption
of the checkpointing data.

## Committing work into cloud storage safely and fast.

As covered earlier, commit-by-rename is dangerous on any object store which
exhibits eventual consistency (example: S3), and often slower than classic
filesystem renames.

Some object store connectors provide custom committers to commit tasks and
jobs without using rename. 

### Hadoop S3A committers

In versions of Spark built with Hadoop 3.1 or later,
the hadoop-aws JAR contains committers safe to use for S3 storage
accessed via the s3a connector.

Instead of writing data to a temporary directory on the store for renaming,
these committers write the files to the final destination, but do not issue
the final POST command to make a large "multi-part" upload visible. Those
operations are postponed until the job commit itself. As a result, task and
job commit are much faster, and task failures do not affect the result. 

To switch to the S3A committers, use a version of Spark was built with Hadoop
3.1 or later, and switch the committers through the following options.

```
spark.hadoop.fs.s3a.committer.name directory
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
```

It has been tested with the most common formats supported by Spark.

```python
mydataframe.write.format("parquet").save("s3a://bucket/destination")
```

More details on these committers can be found in 
[the latest Hadoop documentation](https://hadoop.apache.org/docs/current/)
with S3A committer detail covered in
[Committing work to S3 with the S3A Committers](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html).

Note: depending upon the committer used, in-progress statistics may be
under-reported with Hadoop versions before 3.3.1.

### Amazon EMR: the EMRFS S3-optimized committer

Amazon EMR has its own S3-aware committers for parquet data.
For instructions on use, see
[the EMRFS S3-optimized committer](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-committer.html)

For implementation and performanc details, see
["Improve Apache Spark write performance on Apache Parquet formats with the EMRFS S3-optimized committer"](https://aws.amazon.com/blogs/big-data/improve-apache-spark-write-performance-on-apache-parquet-formats-with-the-emrfs-s3-optimized-committer/


### Azure and Google cloud storage: MapReduce Intermediate Manifest Committer.

Versions of the hadoop-mapreduce-core JAR shipped after September 2022 (3.3.5 and later)
contain a committer optimized for performance and resilience on
Azure ADLS Generation 2 and Google Cloud Storage.

This committer, the "manifest committer" uses a manifest file to propagate
directory listing information from the task committers to the job committer.
These manifests can be written atomically, without relying on atomic directory rename,
something GCS lacks.

The job commmitter reads these manifests and will rename files from the task output
directories directly into the destination directory, in parallel, with optional
rate limiting to avoid throttling IO.
This deliviers performance and scalability on the object stores.

It is not critical for job correctness to use this with Azure storage; the
classic FileOutputCommitter is safe there -however this new committer scales
better for large jobs with deep and wide directory trees.

Because Google GCS does not support atomic directory renaming,
the manifest committer should be used where available.

This committer does support  "dynamic partition overwrite" (see below). 

For details on availability and use of this committer, consult
the hadoop documentation for the Hadoop release used.

It is not available on Hadoop 3.3.4 or earlier.

### IBM Cloud Object Storage: Stocator

IBM provide the Stocator output committer for IBM Cloud Object Storage and OpenStack Swift.

Source, documentation and releasea can be found at
[https://github.com/CODAIT/stocator](Stocator - Storage Connector for Apache Spark).


## Cloud Committers and `INSERT OVERWRITE TABLE`

Spark has a feature called "dynamic partition overwrite"; a table can be updated and only those
partitions into which new data is added will have their contents replaced.

This is used in SQL statements of the form `INSERT OVERWRITE TABLE`,
and when Datasets are written in mode "overwrite"

{% highlight scala %}
eventDataset.write
  .mode("overwrite")
  .partitionBy("year", "month")
  .format("parquet")
  .save(tablePath)
{% endhighlight %}

This feature uses file renaming and has specific requirements of
both the committer and the filesystem:

1. The committer's working directory must be in the destination filesystem.
2. The target filesystem must support file rename efficiently.

These conditions are _not_ met by the S3A committers and AWS S3 storage.

Committers for other cloud stores _may_ support this feature, and
declare to spark that they are compatible. If dynamic partition overwrite
is required when writing data through a hadoop committer, Spark
will always permit this when the original `FileOutputCommitter`
is used. For other committers, after their instantiation, Spark
will probe for their declaration of compatibility, and
permit the operation if state that they are compatible.

If the committer is not compatible, the operation will fail with
the error message
`PathOutputCommitter does not support dynamicPartitionOverwrite`

Unless there is a compatible committer for the target filesystem,
the sole solution is to use a cloud-friendly format for data
storage.

## Further Reading

Here is the documentation on the standard connectors both from Apache and the cloud providers.

* [OpenStack Swift](https://hadoop.apache.org/docs/current/hadoop-openstack/index.html). Hadoop 2.6+
* [Azure Blob Storage](https://hadoop.apache.org/docs/current/hadoop-azure/index.html).
* [Azure Blob Filesystem (ABFS) and Azure Datalake Gen 2](https://hadoop.apache.org/docs/current/hadoop-azure/abfs.html).
* [Azure Data Lake Gen 1](https://hadoop.apache.org/docs/current/hadoop-azure-datalake/index.html).
* [Amazon S3 Strong Consistency](https://aws.amazon.com/s3/consistency/)
* [Hadoop-AWS module (Hadoop 3.x)](https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/index.html).
* [Amazon EMR File System (EMRFS)](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-fs.html). From Amazon.
* [Using the EMRFS S3-optimized Committer](https://docs.amazonaws.cn/en_us/emr/latest/ReleaseGuide/emr-spark-s3-optimized-committer.html)
* [Google Cloud Storage Connector for Spark and Hadoop](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage). From Google.
* [The Azure Blob Filesystem driver (ABFS)](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver)
* IBM Cloud Object Storage connector for Apache Spark: [Stocator](https://github.com/CODAIT/stocator),
  [IBM Object Storage](https://www.ibm.com/cloud/object-storage). From IBM.
* [Using JindoFS SDK to access Alibaba Cloud OSS](https://github.com/aliyun/alibabacloud-jindofs).

The Cloud Committer problem and hive-compatible solutions
* [Committing work to S3 with the S3A Committers](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html)
* [Improve Apache Spark write performance on Apache Parquet formats with the EMRFS S3-optimized committer](https://aws.amazon.com/blogs/big-data/improve-apache-spark-write-performance-on-apache-parquet-formats-with-the-emrfs-s3-optimized-committer/)
* [The Manifest Committer for Azure and Google Cloud Storage](https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/site/markdown/manifest_committer.md)
* [A Zero-rename committer](https://github.com/steveloughran/zero-rename-committer/releases/).
* [Stocator: A High Performance Object Store Connector for Spark](http://arxiv.org/abs/1709.01812)

