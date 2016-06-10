#!/bin/env bash

# stop jupyterhub
PROCESSES="$(ps -aef | grep jupyterhub | awk '$8 != "bash" {print $2}')"

# Kill the pids
for x in $PROCESSES
do
    echo "sudo kill -9 $x"
    sudo kill -9 $x
done

PROCESSES="$(ps -aef | grep node | awk '$8 != "bash" {print $2}')"
for x in $PROCESSES
do
    echo "sudo kill -9 $x"
    sudo kill -9 $x
done
