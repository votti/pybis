# Use

Vagrant automates the creation of virtual environments. Here, we use vagrant to define an environment for running jupyterhub together with openBIS.

Running

    vagrant up

in the `src/vagrant` directory will provision a VM if necessary and start it.

The provisioning scripts install all prerequisites: python, jupyterhub, java, etc. You must, however, download and install openBIS yourself because openBIS cannot be downloaded anonymously. There is, however, a `console.properties` configuration file in the config/openbis directory that can be used to configure the installer.
