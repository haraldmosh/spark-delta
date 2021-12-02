#!/bin/bash
set -ex

pip3 install -U delta-spark==1.0.0

sudo aws s3 cp s3://airflow-resources-store/dependencies/delta-core_2.12-1.0.0.jar /usr/lib/spark/jars/

ls -Al /usr/lib/spark/jars/