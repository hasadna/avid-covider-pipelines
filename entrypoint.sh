#!/bin/sh

! [ -e /COVID19-ISRAEL ] && echo missing /COVID19-ISRAEL directory, it should be a volume && exit 1

export GIT_SSH_COMMAND="ssh -i $COVID19_ISRAEL_PRIVATE_KEY_FILE -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

! [ -e /COVID19-ISRAEL/.git ] &&\
  git clone "git@github.com:${COVID19_ISRAEL_REPOSITORY}.git" /COVID19-ISRAEL

( cd /COVID19-ISRAEL && git pull origin master && python3 -m pip install -r requirements-preprocess.txt )

/dpp/docker/run.sh "$@"
