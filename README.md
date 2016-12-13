# pyBIS

pyBIS is a Python module for interacting with openBIS. It can be used completely independent from Jupyter or JupyterHub. However, in combination with Jupyter it offers a sort of IDE for openBIS, making the life of a normal user with moderate programming skills very easy.

# Requirements and organization

### Requirements
pyBIS uses the openBIS API v3. Because of some compatibility problems, openBIS version 16.05.1 is the minimal requirement. On the Python side, pyBIS uses Python 3.5 and pandas.

### Organization
pyBIS is devided in several parts:

- the **pyBIS module** which holds all the method to interact with openBIS
- the **JupyterHub authenticator** which uses pyBIS for authenticating against openBIS, validating and storing the session token
- the **Vagrantfile** to set up a complete virtual machine, based on Cent OS 7, including JupyterHub
- the **dataset-uploader-api.py**, an ingestion plug-in for openBIS, allowing people to upload new datasets