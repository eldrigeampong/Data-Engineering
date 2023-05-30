
![Logo](https://theleftjoin.com/what-is-pyspark-and-why-should-i-use-it/what-is-pyspark-and-why-should-i-use-it.png)


# Apache Spark Projects In Python

In this repository, raw data is collected, managed, and converted into usable information using Apache Spark.


## Author

- [@eldrigeampong](https://www.github.com/eldrigeampong)


## Usage/Examples

```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Project').getOrCreate()
print(f'The PySpark {spark.version} version is running....')
```

