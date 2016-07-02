#!/bin/env bash

# Setup the groups we need to simplify authentication
sudo groupadd jupyterhub
sudo usermod -a -G jupyterhub vagrant
chgrp jupyterhub /home/vagrant
chmod g+rx /home/vagrant


# Install git
sudo yum -y install git

# Install checkpolicy -- needed to configure SELinux to allow jupyterhub to do its thing
sudo yum -y install checkpolicy
sudo yum -y install policycoreutils policycoreutils-python


# Get and install miniconda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
/bin/bash Miniconda3-latest-Linux-x86_64.sh -b -p /home/vagrant/miniconda3
echo export PATH=\"/home/vagrant/miniconda3/bin:\$PATH\" >> .bashrc
export PATH=/home/vagrant/miniconda3/bin:$PATH
sudo chown -R vagrant:jupyterhub miniconda3/


# Put Jupyter and R into conda
conda install jupyter
conda install -c r r-essentials

# Install JupyterHub
sudo npm install -g configurable-http-proxy
pip install --upgrade pip
pip install jupyterhub
pip install git+https://github.com/jupyter/sudospawner

# To set up jupyter hub, follow the instructions https://github.com/jupyterhub/jupyterhub/wiki/Using-sudo-to-run-JupyterHub-without-root-privileges
# Though sudospanwer has already been installed

# Install often used python packages
pip install scipy
pip install numpy
pip install pandas
pip install matplotlib
pip install requests

# Install our python packages
pip install -e /vagrant_python/PyBis/
pip install -e /vagrant_python/JupyterBis/

sudo ln -s /home/vagrant/miniconda3/bin/* /usr/bin
sudo chgrp -R jupyterhub /home/vagrant/miniconda3/bin

# Create a directory for jupyterhub to store state
sudo mkdir /etc/jupyterhub
sudo chgrp jupyterhub /etc/jupyterhub
