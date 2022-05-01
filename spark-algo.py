from pyspark.sql import SparkSession, column
import time

start_time = time.time()
spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
data_frame = spark.read.csv(
    "hdfs://namenode:9000/output_mapper/output2/part-00000",
    header=True,
    inferSchema=True,
)

data_frame.printSchema()

def column_rename(data_frame):
     for col in data_frame.columns:
         col_new = col.replace('.','')
         data_frame = data_frame.withColumnRenamed(col, col_new)
     return data_frame

import re
from pyspark.sql.functions import col

data_frame_2 = column_rename(data_frame)
data_frame_2.columns
data_frame_3 = data_frame_2.select('customId', col('timestamp').alias('id'), col("startRating").cast('int').alias("reviewsrating"))
data_frame_3.groupBy('customId').count().orderBy('count', ascending=False)

data_frame_3.groupBy('reviewsrating').count().orderBy('count', ascending=False)

data_frame_4 = data_frame_3.filter("reviewsrating != 44 AND reviewsrating != 16")
data_frame_4.show(5)

from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, IndexToString

indexing_string = StringIndexer(inputCol="id", outputCol="id_int")
indexing_model = indexing_string.fit(data_frame_4)
data_frame_5 = indexing_model.transform(data_frame_4)

data_frame_5.groupBy('id_int').count().orderBy('count', ascending=False)

indexing_string = StringIndexer(inputCol="customId", outputCol="userid")
indexing_string
indexing_model = indexing_string.fit(data_frame_5)
data_frame_6 = indexing_model.transform(data_frame_5)

data_frame_6.show(5)

train_dataset, testing_dataset = data_frame_6.randomSplit([0.75,0.25])
from pyspark.ml.recommendation import ALS
rs_transform = ALS(maxIter=10, regParam=0.01, userCol='userid', itemCol='id_int', ratingCol='reviewsrating', nonnegative=True, coldStartStrategy="drop")
rs_transform = rs_transform.fit(train_dataset)
prediction = rs_transform.transform(testing_dataset)

from pyspark.ml.evaluation import RegressionEvaluator
evl = RegressionEvaluator(metricName='rmse', predictionCol='prediction', labelCol='reviewsrating')
rmse_value = evl.evaluate(prediction)

main_dataset = data_frame_6.select('id_int').distinct()
selected_dataset = data_frame_6.filter(data_frame_6['userid'] == 76).select('id_int').distinct()
selected_dataset = selected_dataset.withColumnRenamed("id_int", "id_int_used")
joined_dataset = main_dataset.join(selected_dataset, main_dataset.id_int == selected_dataset.id_int_used, how='left')

new_dataset = joined_dataset.where(col('id_int_used').isNull()).select(col('id_int')).distinct()
new_dataset = new_dataset.withColumn("userid",lit(int(76)))

recommondation = rs_transform.transform(new_dataset).orderBy('prediction', ascending=False)
recommondation.createTempView('recommondation')

recommondation_5 = spark.sql('SELECT id_int FROM recommondation limit 5')

selected_dataset_id = recommondation_5.join(data_frame_6, recommondation_5.id_int == data_frame_6.id_int, how='left')
# productTitle
selected_dataset_id.select('id').join(data_frame_2, selected_dataset_id.id == data_frame_2.timestamp, how='left').select('productTitle').distinct().show(5)

end_time = time.time() - start_time
final_time = time.strftime("%H:%M:%S", time.gmtime(end_time))
print("Total execution time: ", final_time)
