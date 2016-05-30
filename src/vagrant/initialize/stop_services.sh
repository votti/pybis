#!/bin/env bash

# Stop openbis and jupyterhub

# stop jupyterhub
# -- find the pid and kill it

# stop openbis
sudo -u openbis /home/openbis/bin/alldown.sh
