#!/bin/env bash

# Start openbis and jupyterhub

# start openbis
sudo -u openbis /home/openbis/bin/allup.sh

# start jupyterhub
pushd .
cd /etc/jupyterhub
sudo jupyterhub -f /vagrant/config/jupyterhub/jupyterhub_config.py --no-ssl &
popd
