# GIS

## Start Postgres

```shell
docker stop gis_db || true && \
docker rm gis_db || true && \
docker run -d --name gis_db --restart no -p 5584:5432 \
     -e POSTGRES_DB=gis \
     -e POSTGRES_USER=gis \
     -e POSTGRES_PASSWORD=gis \
     postgis/postgis
```
## Build image
```shell
docker build -t gis .
```
## Start module
```shell
docker run --name gis -p 84:84 -e HOST=0.0.0.0 -e DB_HOST=host.docker.internal --add-host host.docker.internal:host-gateway -e PYTHONUNBUFFERED=1 gis
```
## Переменные окружения

* `HOST` - хост веб-приложения
* `PORT` - порт веб-приложения (по умлочанию - `84`, адаптировано под докер)
* `DB_HOST` - хост СУБД (по умолчанию - `127.0.0.1`)
* `DB_PORT` - порт СУБД (по умолчанию - `5584`)
* `DB_NAME` - название БД в СУБД (по умолчанию - `gis`)
* `DB_USER` - имя пользователя в СУБД (по умолчанию - `gis`)
* `DB_PWD` - пароль в СУБД (по умолчанию - `gis`)


