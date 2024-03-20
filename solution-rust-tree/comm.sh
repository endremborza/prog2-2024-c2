#! /bin/sh
screen -dmS rust_server cargo run --release; while ! nc -z localhost 5678; do sleep 1; done;

