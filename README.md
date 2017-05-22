### Code Analyzer for Spark Jobs (Java)

This project is an example of the type of code analyzers that can be integrated in [Crystal](https://github.com/Crystal-SDS) for optimizing Big Data access to object storage via software-defined storage mechanisms. In particular, the objective of this project is to analyze the code of an input Spark job (Java) and to identify those operations performed on RDDs (and many of its subclasses) that can be delegated for execution to the storage side (thanks to a new [Storlet](https://github.com/Crystal-SDS/filter-samples/tree/master/Storlet_lambda_pushdown)). That is, operations like filter(), map() or reduce() are migrated and executed at the storage side for obtaining a faster application execution time.

### Lifecycle

The lifecycle of Crystal's code analyzers is as follows:
1. The administrator deploys a code analyzer executable via the [Crystal API](https://github.com/Crystal-SDS/controller).
2. The administrator deploys the [filter/Storlet](https://github.com/Crystal-SDS/filter-samples/tree/master/Storlet_lambda_pushdown) at the storage side to execute code on data object flows.
3. The administrator defines a policy specifying that the Spark jobs of a certain tenant will be associated to the code analyzer.
4. A user submits his Spark job (e.g., WordCount.java) via the Crystal API.
5. The Crystal API inputs the job to the code analyzer.
6. Depending on the output of the code analyzer, the Crystal API will submit to the Spark cluster the original job or a new version of in which parts of the computations are pushed-down to the storage filter/Storlet.

### A Simple Example

Let us show which is the actual point of a code analyzer. To this end, we make use of a word count Spark job:

```java
//Original job submitted by the user
JavaRDD<String> textFile = sc.textFile("swift2d://data1.lvm/hamlet.txt");
JavaPairRDD<String, Integer> counts = textFile
    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
    .mapToPair(word -> new Tuple2<>(word, 1))
    .reduceByKey((a, b) -> a + b);
counts.saveAsTextFile("hdfs://...");
```
If we execute this job as usual in a compute cluster, Spark will first require to *ingest the whole dataset from Swift* to split lines into words, create word/count pairs and then execute a final sum of word occurrences grouping workds by key.

The idea behind a code analyzer in Crystal is that some of these processing steps can be efficienlty done in a *stream fashion* at the storage side, just when data is being served to SPark from Swift. To show an example of this, this project can automatically transform the original job into the following one:

```java
//Modified job code output by the code analyzer
JavaRDD<String> textFile = sc.textFile("swift2d://data1.lvm/hamlet.txt"); 
JavaPairRDD<String, Integer> counts = textFile
    .mapToPair(word -> new Tuple2<String, Integer>(word.split("=")[0], Integer.valueOf(word.split("=")[1])))
    .reduceByKey((a, b) -> a + b); 
counts.saveAsTextFile("swift2d://data1.lvm/hamlet_result.txt");
```

Conversely to the original job code, the modified version only gets a dataset which is in fact composed by <word,count> pairs and then it performs a final reduceByKey operation on that data. The reason for this  modification in the code is that the code analyzer told the storage side to execute the following parts of the job:

```java
//Lambda functions delegated to Swift and executed in stream as data is served
flatMap(s -> Arrays.stream(s.split(" ")))
map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
map(word -> new SimpleEntry<String, Integer>(word, 1))
collect(java.util.stream.Collectors.groupingBy(SimpleEntry<String, Integer>::getKey, java.util.stream.Collectors.counting()))
```





