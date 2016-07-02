#!/bin/env bash

# set localtime
sudo rm /etc/localtime
sudo ln -s /usr/share/zoneinfo/Europe/Zurich /etc/localtime

# install some often used packages
sudo yum -y install wget
sudo yum -y install epel-release
sudo yum -y install nodejs
sudo yum -y install npm
sudo yum -y install unzip
sudo yum -y install vim
