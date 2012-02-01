#!/bin/sh

EXECUTABLE="tweet_tracker.py"
DUMPED="freq_dump.pckl"

kill -s INT `ps ax | grep "python.*${EXECUTABLE}" | grep -v grep | cut -d' ' -f1`
./${EXECUTABLE} -d ${DUMPED} &