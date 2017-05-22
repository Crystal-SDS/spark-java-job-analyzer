### Code Analyzer for Spark Jobs (Java)

This project is an example of the type of code analyzers that can be integrated in [Crystal](https://github.com/Crystal-SDS) for optimizing Big Data access to object storage via software-defined storage mechanisms. In particular, the objective of this project is to analyze the code of an input Spark job (Java) and to identify those operations performed on RDDs (and many of its subclasses) that can be delegated for execution to the storage side (thanks to a new [Storlet](https://github.com/Crystal-SDS/filter-samples/tree/master/Storlet_lambda_pushdown)). That is, operations like filter(), map() or reduce() are migrated and executed at the storage side for obtaining a faster application execution time.

### Lifecycle

The lifecycle of Crystal's code analyzers is as follows:
1. The administrator deploys a code analyzer executable via the Crystal API.
2. The administrator deploys the filter/Storlet at the storage side to execute code on data object flows.
3. The administrator defines a policy specifying that the Spark jobs of a certain tenant will be associated to the code analyzer.
4. A user submits his Spark job (e.g., WordCount.java) via the Crystal API.
5. The Crystal API inputs the job to the code analyzer.
6. Depending on the output of the code analyzer, the Crystal API will submit to the Spark cluster the original job or a new version of in which parts of the computations are pushed-down to the storage filter/Storlet.

### A Simple Example

Let us show which is the actual point of a code analyzer. To this end, we make use of a word count Spark job:

```java
//Original job submitted by the user
JavaRDD<String> textFile = sc.textFile("swift2d://...");
JavaPairRDD<String, Integer> counts = textFile
    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
    .mapToPair(word -> new Tuple2<>(word, 1))
    .reduceByKey((a, b) -> a + b);
counts.saveAsTextFile("hdfs://...");
```

