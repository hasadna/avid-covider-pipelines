#!/usr/bin/env bash

python3 -m corona_data_collector.tests.test_isolation_fields &&\
python3 -m corona_data_collector.tests.test_flow
