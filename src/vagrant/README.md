# Getting started with Vagrant, openBIS and JupyterHub

## How this folder is organized

- `Vagrantfile` -- includes all information how the virtual machine needs to be set up: including synched folders, port forwarding, shell scripts to run, memory consumption etc.
- `initialize/` -- folder contains shell scripts that are run during the provisioning process (`setup_*`) and some `start_*` and `stop_*` scripts to start services on the virtual machine (eg. JupyterHub, openBIS)
- `config/` -- folder contains configuration files for JupyterHub, openBIS and Postgres database.

The `initialize/` folder is synched, which means it is visible inside the virtual machine. 


## About Vagrant virtual environment

Vagrant automates the creation of virtual environments. Here, we use vagrant to define an environment for running JupyterHub together with
openBIS. You can download Vagrant from this website:

https://www.vagrantup.com

Vagrant needs a virtualizing software in order to run the virtual machine. Vagrant works well with many backend providers; by default it works with VirtualBox (https://www.virtualbox.org/), but you can use other providers as well. Vagrant acts like a remote control to start virtual machines.

When setting up a machine the first time, Vagrant reads a file called «Vagrantfile». This file contains information about which OS template to start with (we use CentOS 7). It then continues with all the shell commandos in order to set up our virtual machine.


## Setting up the virtual machine (vagrant)

0. cd to `src/vagrant`
1. `vagrant plugin install vagrant-vbguest`
2. `vagrant up --provision --provider virtualbox` -- this will read `Vagrantfile` and provision a CentOS 7 VM and install most software prerequisites (python, JupyterHub, etc.). This can take a while and needs a fast internet connection too.
3. Commands to control the machine:
    - `vagrant halt` -- shut down machine
    - `vagrant up`   -- restart machine
    - `vagrant ssh`  -- log in
4. all vagrant commands need to be executed inside the `/vagrant` directory, because the command always reads the `Vagrantfile`


## using an existing openBIS instance and start JupyterHub
If you already have a running openBIS instance and want your JupyterHub users authenticate against that server:
1. edit the file `src/vagrant/config/jupyterhub/jupyterhub_config.py`
2. change the last line to point to your openBIS instance: `c.OpenbisAuthenticator.server_url = "https://localhost:8443"`
3. start JupyterHub: `vagrant ssh -c "sudo sync/initialize/start_jupyterhub.sh"`
4. point your browser to `https://localhost:8000` and log in with your openBIS username and password.
3. stop JupyterHub `vagrant ssh -c sync/initialize/stop_jupyterhub.sh`
5. if you want to change change the default-port of JupyterHub (:8000), edit the `jupyterhub_config.py` and change the line `c.JupyterHub.port = 8000`


## installing openBIS on the virtual machine (optional)
1. download openBIS https://wiki-bsse.ethz.ch/display/bis/Production+Releases
2. untar it and put the folder in the `src/vagrant/` folder
4. restart the virtual machine to make the openBIS folder visible within the virtual machine. 
   - `vagrant halt` -- shut down machine
   - `vagrant up`   -- restart machine
5. `vagrant ssh`  -- log into the virtual machine
6. install openBIS (inside the vagrant machine)
   - `cd /vagrant/[openbis-installation]`
   - `cp /vagrant/config/openbis/console.properties ./` -- Use the provided console.properties or create your own.
   - `chmod a+rw ./console.properties` -- This file needs to be read and writable for installation to work.
   - `sudo su openbis`
   - `./run-console.sh`
   - wait until `/home/openbis/bin/post-install/0-create-initial-users.sh` appears. This script does not automatically run for some reason. Terminate it by pressing CTRL-C
   - run `/home/openbis/bin/post-install/0-create-initial-users.sh` manually
   - enter a password for the **admin** and the **etlserver** user when asked
7. Edit the file `/home/openbis/servers/datastore_server/etc/service.properties` 
   - look for the `etlserver` user and enter its password
8. `exit` -- Reverse the sudo su openbis and return to being the vagrant user
9. enter the command ``
ame` ``
10. copy the output and add it to /etc/hosts: `sudo vi /etc/hosts` so that Java is happy
11. `exit` -- log off the virtual machine


## start openBIS and JupyterHub

1. `vagrant ssh` -- to log into the virtual machine
2. `sudo -u openbis /home/openbis/bin/allup.sh` -- start openBIS (if installed)
3. `sudo sync/initialize/start_jupyterhub.sh` -- start JupyterHub
1. `sudo sync/initialize/start_services.sh` -- start both openBIS and JupyterHub
1. point your browser to `https://localhost:8443/openbis/` and check whether your server is up and running. Try logging in as an admin user, for example.
1. check the AS and DSS logs if you encounter any problems
   * `/home/openbis/bin/bislog.sh` -- openbis AS logfile
   * `/home/openbis/bin/dsslog.sh` -- datastore server DSS logfile

## create a new user in openBIS 

1. point your browser to the ELN-LIMS Lab notebook running at `https://localhost:8443/openbis/webapp/eln-lims/` and log in as admin
1. go to Utilities -> User Manager, click on the Operations dropdown and choose "ceate User"
1. enter a username (User ID) and a password to create a new user in openBIS (this may a while)



