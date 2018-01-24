# Testing Mariadb

Using Mariadb 10.3 but other versions should work as well.

## Setup

```
docker pull mariadb:10.3
```

## Run

```
docker run -d --rm --name maria-db -p 3306:3306 -e MYSQL_ROOT_PASSWORD=secretpw mariadb:10.3
```

## Connect

```
docker run --rm --name maria-db-client -it mariadb:10.3 sh -c 'exec mysql -h"<host-ip-address>" -P"3306" -uroot -p"secretpw"'
```

## Load sample data (sakila)

Run maria-db container and then

```
wget "http://downloads.mysql.com/docs/sakila-db.tar.gz" -O /tmp/sakila.tar.gz && \
  tar -xvzf /tmp/sakila.tar.gz -C /tmp/ && \
  docker cp "/tmp/sakila-db/sakila-data.sql" maria-db:/tmp/ && \
  docker cp "/tmp/sakila-db/sakila-schema.sql" maria-db:/tmp/ && \
  docker exec -it maria-db sh -c 'mysql -uroot -p"secretpw" < /tmp/sakila-schema.sql' && \
  docker exec -it maria-db sh -c 'mysql -uroot -p"secretpw" < /tmp/sakila-data.sql'
```

## Persistance

(to be done)
