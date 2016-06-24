#!/bin/env bash

# Start openbis and jupyterhub

# start openbis
sudo -u openbis /home/openbis/bin/allup.sh

# start jupyterhub
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
$DIR/start_jupyterhub.sh
