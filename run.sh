
./build.sh
docker network create hoang
docker-compose -f docker-compose.yml up -d
docker rm -f $(docker ps -a -q)	# Delete all containers that are not currently running
