#!/bin/bash
for dir in "$@"
do
    python engine.py "$dir"
done