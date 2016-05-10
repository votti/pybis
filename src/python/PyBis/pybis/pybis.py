#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pybis.py

A class with methods for interacting with openBIS.

Created by Chandrasekhar Ramakrishnan on 2016-05-10.
Copyright (c) 2016 ETH Zuerich. All rights reserved.
"""

import os


class OpenbisCredentials:
    """Credentials for communicating with openBIS."""

    def __init__(self, token=None, uname_and_pass=None):
        """A connection can be authenticated either by a token or a username and password combination
        :param token: An authentication token for openBIS, can be None.
        :param uname_and_pass: A tuple with username and password, in that order.
        """
        self.token = token
        self.uname_and_pass = uname_and_pass

    def has_token(self):
        return self.token is not None

    def has_username_and_password(self):
        return self.uname_and_pass is not None

    @property
    def username(self):
        return self.uname_and_pass[0]

    @property
    def password(self):
        return self.uname_and_pass[1]


class OpenbisCredentialStore:
    """Cache login tokens for reuse."""

    def __init__(self, store_folder):
        """Cache credentials on the file system at store_path.
        If the store_folder does not exist, it will be created with the umask inherited from the shell.
        :param store_folder: The folder to write the credentials to. It will be created if necessary.
        """
        self.store_folder = store_folder

    @property
    def store_path(self):
        return os.path.join(self.store_folder, "bis_token.txt")

    def read(self):
        """Read the cached credentials and return a credentials object.
        :return: A credentials object with a token, or an empty credentials object if no store was found.
        """
        if not os.path.exists(self.store_path):
            return OpenbisCredentials()
        with open(self.store_path, "r") as f:
            token = f.read()
        return OpenbisCredentials(token)

    def write(self, credentials):
        """Write a credentials object to the store, overwriting any previous information.
        :param credentials: The credentials with a token to write. If it has no token, nothing is written.
        """
        if not credentials.has_token():
            return
        token = credentials.token
        if not os.path.exists(self.store_folder):
            os.makedirs(self.store_folder)
        with open(self.store_path, "w") as f:
            f.write(token)


class Openbis:
    """Interface for communicating with openBIS."""

    def __init__(self, url, credentials):
        """Initialize an interface to openBIS with information necessary to connect to the server.
        :param url:
        :param credentials:
        """
        self.url = url
        self.credentials = credentials

    def login(self):
        """Log into openBIS.
        Expects credentials with username and password and updates the token on the credentials object.
        Clients may want to store the credentials object in a credentials store after successful login.
        Throw a ValueError with the error message if login failed.
        """
        if not self.credentials.has_username_and_password:
            raise ValueError('Cannot log into openBIS without a username and password')
            # TODO Implement the logic of this method.

    def is_token_valid(self):
        """Check if the connection to openBIS is valid.
        This method is useful to check if a token is still valid or if it has timed out, requiring the
        user to login again.
        :return: Return True if the token is valid, False if it is not valid.
        """
        if not self.credentials.has_token():
            return False
            # TODO Implement the logic of this method.

    def get_samples(self, sample_identifiers):
        """Retrieve metadata for the sample.
        Get metadata for the sample and any directly connected parents of the sample to allow access
        to the same information visible in the ELN UI. The metadata will be on the file system.
        :param sample_identifiers: A list of sample identifiers to retrieve.
        """
        pass
        # TODO Implement the logic of this method

    def get_samples_with_data(self, sample_identifiers):
        """Retrieve metadata for the sample, like get_sample_metadata, but retrieve any data sets as well,
        like get_data_set.
        :param sample_identifiers: A list of sample identifiers to retrieve.
        """
        pass
        # TODO Implement the logic of this method

    def get_data_sets(self, data_set_identifiers):
        """Retrieve data set metadata and content.
        The metadata will be on the file system. The file will also include the location of the data.
        """
        pass
        # TODO Implement the logic of this method

    def create_data_set_from_notebook(self, path_to_notebook, owner_identifier, paths_to_files,
                                      parent_identifiers):
        """Register a new data set with openBIS.
        :param path_to_notebook: The path to the Jupyter notebook that created this data set
        :param owner_identifier: The identifier of the sample that owns this data set.
        :param paths_to_files: A list of paths to files that should be in the data set.
        :param parent_identifiers: A list of parents for the data set.
        :return:
        """
        pass
        # TODO Implement the logic of this method