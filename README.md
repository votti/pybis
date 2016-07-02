# Summary

Python module for interacting with openBIS.

# Plan

We plan to implement the following commands:

- login
- get_sample_with_data
- get_data_set
- create_data_set
- logout

The methods get_sample_with_data and get_data_set will write files to the file system that can be read in using standard libraries. We hope to store the files as json, but we need to verify that they are easy to work with from python and R.

# Setup

We use Vagrant to create an environment for running jupyter/jupyterhub and openBIS. To set up this environment, run

```
$ vagrant up
```

in the folder with the `Vagrantfile`. This will create a CentOS-based VM and install the basic prerequisites for running jupyterhub and openbis: python3, nodejs, npm, and necessary python packages. Some manual setup is still necessary:

  https://github.com/jupyterhub/jupyterhub/wiki/Using-sudo-to-run-JupyterHub-without-root-privileges

Note: The python packages required should already be installed, but you will need to create a group for jupyterhub, create the rhea user, and ensure that any users that should have access to jupyterhub are part of the jupyterhub group.

```
$ sudo useradd rhea
[edit sudoers as per instructions above, using the group-based variant]
$ useradd -G jupyterhub [newuser]
```
You may also need to ensure that other users can access the miniconda installation:

  chmod a+rx ~/

Furthermore, it is necessary to give the rhea user access to the shadow password file so PAM authentication works:

```
$ sudo groupadd shadow
$ sudo chgrp shadow /etc/shadow
$ sudo chmod g+r /etc/shadow
$ sudo usermod -a -G shadow rhea
```
# Some Links

- Setting up jupyter to use sudo instead of running as root: https://github.com/jupyterhub/jupyterhub/wiki/Using-sudo-to-run-JupyterHub-without-root-privileges
