FROM ubuntu:18.04
RUN apt-get update &&\
    apt-get install -y build-essential python3-dev python3-setuptools python3-pip python3-wheel \
                       libxml2-dev libxslt-dev redis libpq5 libpq-dev libleveldb1v5 libleveldb-dev &&\
    python3 -m pip install \
         psycopg2==2.8.5 datapackage-pipelines-github==0.0.4 \
         datapackage-pipelines-sourcespec-registry==0.0.9 datapackage-pipelines-aws==0.0.23 &&\
    mkdir -p /var/redis && chmod 775 /var/redis && chown redis.redis /var/redis &&\
    ln -s `which python3` /usr/bin/python && mkdir -p /var/run/dpp
COPY dpp/ /dpp/
ENV DPP_NUM_WORKERS=4
ENV DPP_REDIS_HOST=127.0.0.1
ENV DPP_CELERY_BROKER=redis://localhost:6379/6
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
EXPOSE 5000
WORKDIR /pipelines/

RUN apt-get update && apt-get install -y libspatialindex-c4v5 git orca
COPY requirements-full.txt /pipelines/
COPY requirements.txt /pipelines/
RUN python3 -m pip install -r requirements-full.txt
COPY . /pipelines
ENTRYPOINT ["/pipelines/entrypoint.sh"]
