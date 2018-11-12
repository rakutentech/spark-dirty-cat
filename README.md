# Dirty Cat: Dealing with dirty categorical (strings).

DirtyCat(Scala) is a package that leverage Spark ML to perform large scale Machine Learning, and provides an alternative to encode string variables.
This package is largely based on the python original code, https://github.com/dirty-cat  


## Documentation
* https://github.com/dirty-cat
* Patricio Cerda, Gaël Varoquaux, Balázs Kégl. Similarity encoding for learning with dirty categorical variables. Machine Learning journal, Springer. 2018.


### Getting started: How to use it 

The DirtyCat  project is built for both Scala 2.11.x against Spark v2.3.0.

This package is provided as it is, hence, you will have to install it by
yourself. Here are some indications to start using it.


### Build it by yourself: Installation

This project can be built with [SBT](https://www.scala-sbt.org/) 1.1.x.


Change build.sbt to satisfy your scala/spark installations.
Then, run on the command line 
```{.bash}
sbt clean

sbt compile

sbt package
```

This will generate a .jar file in: target/scala_VERSION/PACKAGE.jar, where
PACKAGE = com.rakuten.dirty_cat_VERSION-0.1-SNAPSHOT.jar


If you are using Jupyter notebooks (scala), you can 
add this file to your toree-spark-options in your Jupyter kernel. 

* Find your available kernesls running: 
```
jupyter kernelspec list 
```
* Go to your Scala kernel and add:
```{.python}
"env": {
    "DEFAULT_INTERPRETER": "Scala",
    "__TOREE_SPARK_OPTS__": "--conf spark.driver.memory=2g --conf spark.executor.cores=4 --conf spark.executor.memory=1g --jars PATH/target/scala_VERSION/PACKAGE.jar
    }
```


To submit your spark application, run 
```{.bash}
spark-submit --master local[3]  --jars target/scala-2.11/spam_scorer_2.11-1.0.jar YOUR_APPLICATION
```


### Ceate local package
```{.bash}
make publish 
```

### Usage with Spark ML

#### Declaration
```{.scala}
import com.rakuten.dirty_cat.feature.SimilarityEncoder

val encoder = (new SimilarityEncoder()
  .setInputCol("devices")
  .setOutputCol("devicesEncoded")
  .setSimilarityType("nGram")
  .setVocabSize(1000))
```

#### Using it in a pipeline
```{.scala}
import org.apache.spark.ml.Pipeline

val pipeline = (new Pipeline().setStages(Array(encoder, YOUR_ESTIMATOR)))
val pipelineModel = pipeline.fit(dataframe)
```

#### Serialization
```{.scala}
pipelineModel.write.overwrite().save("pipeline.parquet") 
```



## History

Andrés Hoyos-Idrobo started this implementation of DirtyCat as a way to improve his Spark/Scala skills.

Contributions from:

* Andrés Hoyos-Idrobo


Corporate (Code) Contributors:
* Rakuten Institut of Technology
