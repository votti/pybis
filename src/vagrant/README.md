# Use

Vagrant automates the creation of virtual environments. Here, we use vagrant to define an environment for running jupyterhub together with openBIS.

## First Run

1. cd to src/vagrant
2. `vagrant up` -- this will provision a VM and install most software prerequisites (python, jupyterhub, etc.)
3. Download openBIS and put it in the vagrant folder so it is visible within the VM.
4. `vagrant ssh` -- log into the machine.
5. Install openbis
  1. `cd /vagrant/[openbis-installation]`
  2. `sudo su openbis`
  4. `cp /vagrant/config/openbis/console.properties ./` -- Use the provided console.properties or create your own.
  5. `./run-console.sh`
  6. `/home/openbis/bin/post-install/0-create-initial-users.sh` -- this script does not automatically run for some reason
6. Make sure the dss service.properties file has the correct password for the etlserver
7. `exit` -- Reverse the sudo su openbis and return to being the vagrant user
8. `/vagrant/initialize/start_services.sh` -- start openbis and jupyterhub

## Subsequent Runs

1. `vagrant up`
2. `vagrant ssh`
3. `/vagrant/initialize/start_services.sh`
