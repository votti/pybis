#!/bin/env bash

# Stop openbis and jupyterhub

# stop openbis
sudo -u openbis /home/openbis/bin/alldown.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$DIR/stop_jupyterhub.sh
