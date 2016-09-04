FROM ubuntu:14.04

ENV PROJ_DIR /var/opt/forum_parsing

COPY . $PROJ_DIR

WORKDIR  $PROJ_DIR

RUN apt-get update \
    && apt-get install -y python3-dev \
        python3-pip \
        libcurl4-gnutls-dev \
        libxml2-dev \
        libxslt-dev \
    && export PYCURL_SSL_LIBRARY=gnutls \
    && pip3 install -r requirements.txt \
    && locale-gen en_US en_US.UTF-8 \
    && update-locale \
    && dpkg-reconfigure locales \
    && export LC_ALL="en_US.UTF-8"
