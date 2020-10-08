FROM ubuntu:18.04@sha256:3235326357dfb65f1781dbc4df3b834546d8bf914e82cce58e6e6b676e23ce8f
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

RUN apt-get update && apt-get install -y libspatialindex-c4v5 git xvfb libgtk2.0-0 libgconf-2-4 chromium-browser
RUN apt-get install -y curl
RUN curl -L https://github.com/plotly/orca/releases/download/v1.3.1/orca-1.3.1.AppImage > /usr/local/bin/orca.AppImage &&\
    chmod +x /usr/local/bin/orca.AppImage &&\
    echo '#!/bin/bash' > /usr/local/bin/orca &&\
    echo 'xvfb-run -a /usr/local/bin/orca.AppImage "$@"' >> /usr/local/bin/orca &&\
    chmod +x /usr/local/bin/orca
COPY requirements-full.txt /pipelines/
COPY requirements.txt /pipelines/
RUN python3 -m pip install -r requirements-full.txt
RUN python3 -m pip install 'celery<5'
COPY . /pipelines
RUN python3 -m pip install -e .
RUN sed -i 's/count % 100 ==/count % 100000 ==/' /usr/local/lib/python3.6/dist-packages/datapackage_pipelines/lib/internal/sink.py
ARG GITHUB_SHA=_
RUN echo "${GITHUB_SHA}" > /pipelines/GITHUB_SHA
ENV DISABLE_TQDM=yes
ENTRYPOINT ["/pipelines/entrypoint.sh"]
