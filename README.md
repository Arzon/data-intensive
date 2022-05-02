# data-intensive

All of our codes are in code folder of name node. to see the list of code please follow the follwing code

```bash
$ cd /code
$ ls -l
```

All of our raw dataset are in the dataset folder in the name node

```bash
$ cd /dataset
$ ls -l
```

Hadoop location of our dataset and out of mapper and reducer

```bash
$ hadoop fs -ls /dataset
$ hadoop fs -ls /output
$ hadoop fs -ls /output_mapper
$ hadoop fs -ls /output_reducer
$ hadoop fs -ls /output_second
$ ls -l
```

To run the mappers and reducer follow the follwing command 

```bash
python3  final_preprocess_code_one.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///dataset/furniture.csv --output-dir hdfs:///output/output1 --no-output
```

```bash
hadoop  jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -input hdfs:///output/output1 -output hdfs:///output_mapper/output1 -mapper final_preprocess_code_second.py -file final_preprocess_code_second.py
```

```bash
hadoop  jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -input hdfs:///output_mapper/output1 -output hdfs:///output_mapper/output_reducer -mapper final_preprocess_code_third.py -file final_preprocess_code_third.py
```

```bash
hadoop  jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -input hdfs:///output_reducer/output1 -output hdfs:///output_second/output1 -mapper final_process_code_fourth.py.py -file final_process_code_fourth.py.py
```

To run the algorithm runs in local (namenode)
```bash
$ cd code 
$ python3 read_hadoop.py
```

To run the spark algorim 
```bash
$ cd code 
$ spark-submit --master yarn --executor-cores 4 --num-executors 4 --executor-memory 6g --driver-memory 1g --conf spark.kryoserializer.buffer.max=512m spark-algo.py
```

