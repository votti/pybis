#!/bin/env bash

pushd .
cd /etc/jupyterhub
sudo jupyterhub -f /vagrant/config/jupyterhub/jupyterhub_config.py --no-ssl &
popd
