#!/usr/bin/env bash

for FILE in `ls corona_data_collector/tests/test_*.py`; do
  if [ "${FILE}" != "corona_data_collector/tests/test_flow.py" ]; then
    MODULE="${FILE//\//.}"
    MODULE="${MODULE/\.py/}"
    echo Running test module "${MODULE}"...
    ! python3 -m "${MODULE}" && echo FAILED! && exit 1
  fi
done

echo Running test_flow module

! python3 -m corona_data_collector.tests.test_flow && echo FAILED! exit 1

echo Great Success
exit 0
