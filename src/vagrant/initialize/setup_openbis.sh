#!/bin/env bash

# Install prerequisites for openbis -- java and postgresql
# NB openbis must be downloaded an installed separately because it is not available anonymously

# Install Java
wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u92-b14/jdk-8u92-linux-x64.rpm"
sudo rpm -ivh jdk-8u92-linux-x64.rpm

# Install postgresql
sudo yum -y install postgresql-server postgresql-contrib
sudo postgresql-setup initdb
sudo cp /home/vagrant/sync/config/postgres/pg_hba.conf /var/lib/pgsql/data/pg_hba.conf
sudo systemctl start postgresql
sudo systemctl enable postgresql


# Create the openbis user
sudo useradd openbis
sudo -u postgres createuser openbis

# Add an entry for this hostname to /etc/hosts -- otherwise java complains
echo "Add to /etc/hosts: 127.0.0.1 localhost `hostname`"
echo "Run the openbis installer to get openbis installed."
