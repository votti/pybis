# Use

Vagrant automates the creation of virtual environments. Here, we use vagrant to define an environment for running jupyterhub together with
openBIS. You can download Vagrant from this website:

https://www.vagrantup.com

Vagrant needs a virtualizing software in order to run the virtual machine. Vagrant works well with many backend providers; by default it works with VirtualBox (https://www.virtualbox.org/), but you can use other providers as well. Vagrant acts like a remote control to start virtual machines.

When setting up a machine the first time, Vagrant reads a file called «Vagrantfile». This file contains information about which OS template to start with (we use CentOS 7). It then continues with all the shell commandos in order to set up our virtual machine.


## First Run

0. cd to src/vagrant
1. `vagrant plugin install vagrant-vbguest`
2. `vagrant up --provision --provider virtualbox` -- this will read Vagrantfile and provision a CentOS 7 VM and install most software prerequisites (python, jupyterhub, etc.). This can take a while and needs a fast internet connection too.
3. Download openBIS and put it in the vagrant folder so it is visible within the VM.
4. `vagrant ssh` -- log into the machine.
5. Install openbis
  1. `cd sync/[openbis-installation]`
  2. `cp /vagrant/config/openbis/console.properties ./` -- Use the provided console.properties or create your own.
  2. `chmod a+rw ./console.properties` -- This file needs to be read and writable for installation to work.
  3. `sudo su openbis`
  4. `./run-console.sh`
  5. `/home/openbis/bin/post-install/0-create-initial-users.sh` -- this script does not automatically run for some reason
6. Make sure the dss service.properties file has the correct password for the etlserver
7. `exit` -- Reverse the sudo su openbis and return to being the vagrant user
8. `sync/initialize/start_services.sh` -- start openbis and jupyterhub


## Subsequent Runs

1. `vagrant up`
2. `vagrant ssh`
3. `sync/initialize/start_services.sh`

