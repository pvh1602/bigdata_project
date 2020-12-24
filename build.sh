docker build dockerfiles/pyspark-notebook -t spark-base:latest
docker build dockerfiles/spark-master -t spark-master:latest
docker build dockerfiles/spark-worker -t spark-worker:latest
docker build dockerfiles/dashboard -t dashboard:latest
