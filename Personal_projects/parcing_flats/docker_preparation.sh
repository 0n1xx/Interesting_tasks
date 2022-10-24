# Создание volume, которые позволят сохранять данные в контейнерах
docker volume create postgres_vol_1
docker volume create clickhouse_vol

docker network create apt_net

#Создаем postgres
docker run -d \
 --name postgres_1 \
 -e POSTGRES_PASSWORD=postgres_admin_onixx  \
 -e POSTGRES_USER=onixx \
 -e POSTGRES_DB=db_for_education \
 -v postgres_vol_1:/var/lib/postgresql/data \
 --net=apt_net \
 -p 5432:5432 \
 postgres:14

#Создаем superset
docker run -d --net=apt_net -p 80:8088 --name superset apache/superset
#Устанавливаем admin с паролем:
docker exec -it superset superset fab create-admin \
              --username onixx \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password data_vis_616_onixx

docker exec -it superset superset db upgrade
docker exec -it superset superset init

#Поднятие Clickhouse
docker run -d\
 --name clickhouse_for_education\
 --net=apt_net\
 -v clickhouse_vol:/var/lib/clickhouse\
 -p 9000:9000\
  -p 8123:8123\
 yandex/clickhouse-server

#Установка либы для superset, чтобы он увидел click
docker exec superset pip install clickhouse-sqlalchemy

