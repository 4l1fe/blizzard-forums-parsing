FROM ubuntu:14.04

ENV PROJ_DIR /var/opt/forum_parsing

COPY . $PROJ_DIR

WORKDIR  $PROJ_DIR

RUN apt-get update \
    && apt-get install -y python3-dev \
        python3-pip \
        libcurl4-openssl-dev \
        curl \
        libxml2-dev \
        libxslt-dev \
    && pip3 install -r requirements.txt
