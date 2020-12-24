# Big data Project 
---
### Install
``` bash 
git clone https://github.com/huyhnvh/big_data_project.git
```

Download spark at: https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz 

Save to: big_data_project/dockerfiles/pyspark-notebook/spark-2.4.1-bin-hadoop2.7.tgz

```bash
sh run.sh
```

---
### Start 
```bash
cd big_data_project
docker-compose up -d
```
1. Copy file from local to namenode (in terminal of docker)
```bash
docker cp ./src/data/articles.parquet namenode:/tmp/
docker cp ./src/data/Dstream/ namenode:/tmp/
```
2. Execute namenode to get in terminal of namenode (in terminal of docker)
```bash   
docker exec -it namenode /bin/bash
```
3. Create folder /data in hdfs (in terminal of namenode)
```bash   
hdfs dfs -mkdir -p /data/
hdfs dfs -mkdir -p /results/
```
4. Put file from namenode to datanode (in terminal of namenode)
```bash   
hdfs dfs -put /tmp/articles.parquet /data/
hdfs dfs -put /tmp/Dstream/ /data/
```
---
### Training Classifer models

Open and run article_classifer.ipynb

---
### Running Streaming experiment
1. Open terminal in pyspark-notebook and run
```code
python api.py
```
2. Run article_classifer.ipynb
3. Get file from hdfs to local system (Run to get results of streaming experiment)
```bash
hdfs dfs -get /results/category_table.csv /tmp/results/
docker cp namenode:/tmp/results/ ./src/results/
```