FROM python:3.9-slim

COPY . /app
WORKDIR /app

ENV TZ=Europe/Moscow
ENV PYTHONUNBUFFERED=1

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN apt-get update
#RUN apt-get remove -y binutils
#RUN apt-get install -y python3-dev

RUN apt-get install -y gdal-bin libgdal-dev g++

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip3 install --no-cache-dir -r /app/requirements.txt

ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
