from pyspark import SparkContext
import time

sc = SparkContext("local", "HelloWorld")
data = ["Hello", "World"]
rdd = sc.parallelize(data)
result = rdd.map(lambda x: x.upper()).collect()

time.sleep(30)

for word in result:
  for word in result:
    for word in result:
      for word in result:
        for word in result:
          print(word)
sc.stop()
