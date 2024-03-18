#! /bin/sh
screen -dmS flask_server python3 preproc.py; while ! nc -z localhost 8000; do sleep 1; done;