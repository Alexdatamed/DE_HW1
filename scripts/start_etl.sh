#!/bin/bash
# Запускає etl_transform.py в контейнері etl_env

docker-compose run --rm etl_env python etl_transform.py
