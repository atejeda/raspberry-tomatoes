#!/usr/bin/env bash

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

BASEPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
STREAMLIT=`which streamlit`

cd ${BASEPATH} && ${STREAMLIT} \
        run application.py \
        --browser.serverAddress="0.0.0.0" \
        --server.enableCORS=false \
        --server.headless=true \
        --server.port=80 \
        2>&1
