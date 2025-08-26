docker build -t johnlenon003/publisher-image:v1 ./publisher
docker build -t johnlenon003/consumer1-image:v1 ./consumer1
docker build -t johnlenon003/consumer2-image:v1 ./consumer2

docker network create mynet

docker run -d --name zookeeper --network=mynet ^
  -p 2181:2181 ^
  -e ALLOW_ANONYMOUS_LOGIN=yes ^
  bitnami/zookeeper:3.8

docker run -d --name kafka --network=mynet ^
  -p 9092:9092 ^
  -e KAFKA_BROKER_ID=1 ^
  -e KAFKA_LISTENERS=PLAINTEXT://:9092 ^
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 ^
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 ^
  -e ALLOW_PLAINTEXT_LISTENER=yes ^
  bitnami/kafka:3.7

docker run -d --name mongo --network=mynet ^
  -p 27017:27017 ^
  mongo:7

docker run -d --name publisher --network=mynet ^
  -e KAFKA_BOOTSTRAP=kafka:9092 ^
  -e MONGO_URI=mongodb://mongo:27017 ^
  publisher-image:v4

docker run -d --name consumer1 --network=mynet ^
  -e KAFKA_BOOTSTRAP=kafka:9092 ^
  -e MONGO_URI=mongodb://mongo:27017 ^
  consumer_interesting:v4

docker run -d --name consumer2 --network=mynet ^
  -e KAFKA_BOOTSTRAP=kafka:9092 ^
  -e MONGO_URI=mongodb://mongo:27017 ^
  consumer_non_interesting:v4

docker ps
pause
