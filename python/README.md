# Python wrapper

This is a python wrapper of com.rakuten.dirty_cat.


## Install 

Please make sure to have built target/scala_VERSION/PACKAGE.jar as this is
required to use this wrapper.


* Local machine:  

```{.python}
python setup.py install --user
```

* Distributed: Build distribution
```{.python}
python setup.py sdist
```


## Testing

```{.bash}
spark-submit --jars target/scala-2.11/com-rakuten-dirty_cat_2.11-0.1-SNAPSHOT.jar  python/test/test_similarity_encoder.py
```



### Usage

#### Declaration
```{.python}
from dirty_cat_spark.feature.encoder.SimilarityEncoder

encoder = (SimilarityEncoder()
  .setInputCol("devices")
  .setOutputCol("devicesEncoded")
  .setSimilarityType("nGram")
  .setVocabSize(1000))
```

#### Using it in a pipeline
```{.python}
from pyspark.ml import Pipeline

pipeline = Pipeline(stages[encoder, YOUR_ESTIMATOR])
pipelineModel = pipeline.fit(dataframe)
```

#### Serialization
```{.python}
pipelineModel.writei().overwrite().save("pipeline.parquet") 
```


## Reference 
Patricio Cerda, Gaël Varoquaux, Balázs Kégl. Similarity encoding for learning with dirty categorical variables. 2018. Accepted for publication in: Machine Learning journal, Springer.



