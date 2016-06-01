#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pybis.py

A class with methods for interacting with openBIS.

Created by Chandrasekhar Ramakrishnan on 2016-05-10.
Copyright (c) 2016 ETH Zuerich. All rights reserved.
"""

import os
import requests
import json
import re



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

    def __init__(self, host=None):
        """Initialize an interface to openBIS with information necessary to connect to the server.
        :param host:
        """
        self.host = host
        self.as_port = ':20000'
        self.ds_port = ':20001'
        self.token = None
        self.v3_as_gi = '/openbis/openbis/rmi-application-server-v3.json'
        self.v1_as_gi = '/openbis/openbis/rmi-general-information-v1.json'
        self.v1_ds_screening = '/rmi-datastore-server-screening-api-v1.json'

    def token(self):
        if self.token is None:
            raise ValueError('no valid session available')

    def post_request(self, resource, data):
        resp = requests.post(self.host + resource, json.dumps(data))

        if resp.ok:
            if 'error' in resp.json():
                raise ValueError('an error has occured: ' + resp.json()['error'] )
            elif 'result' in resp.json():
                return resp.json()['result']
            else:
                raise ValueError('request did not return either result nor error')
        else:
            raise ValueError('general error while performing post request')

    def logout(self):

        logout_request = {
            "method":"logout",
            "params":[self.token],
            "id":"1",
            "jsonrpc":"2.0"
        }
        resp = requests.post(self.host + self.v3_as_gi, json.dumps(logout_request))
        if resp.ok:
            self.token = None


    def login(self, username='openbis_test_js', password='password', store_credentials=False):
        """Log into openBIS.
        Expects a username and a password and updates the token (session-ID).
        The token is then used for every request.
        Clients may want to store the credentials object in a credentials store after successful login.
        Throw a ValueError with the error message if login failed.
        """

        login_request = {
            "method":"login",
            "params":[username, password],
            "id":"1",
            "jsonrpc":"2.0"
        }
        result = self.post_request(self.v3_as_gi, login_request)
        self.token = result
        

    def is_token_valid(self):
        """Check if the connection to openBIS is valid.
        This method is useful to check if a token is still valid or if it has timed out,
        requiring the user to login again.
        :return: Return True if the token is valid, False if it is not valid.
        """

        if not self.token:
            return False



    def get_dataset(self, permid):

        dataset_request = {
            "method": "getDataSets",
            "params": [
                self.token,
                [
                    {
                        "@id": 0,
                        "permId": permid,
                        "@type": "as.dto.dataset.id.DataSetPermId"
                    }
                ],
                {
                "@id": 0,
                "parents": {
                    "@id": 1,
                    "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                },
                "containers": {
                    "@id": 3,
                    "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                },
                "physicalData": {
                    "@id": 5,
                    "@type": "as.dto.dataset.fetchoptions.PhysicalDataFetchOptions"
                },
                "linkedData": {
                    "@id": 6,
                    "@type": "as.dto.dataset.fetchoptions.LinkedDataFetchOptions",
                    "externalDms": None,
                    "sort": None
                },
                "dataStore": {
                    "@id": 9,
                    "@type": "as.dto.datastore.fetchoptions.DataStoreFetchOptions",
                    "sort": None
                },
                "sample": {
                    "@id": 14,
                    "@type": "as.dto.sample.fetchoptions.SampleFetchOptions"
                },
                "properties": {
                    "@id": 15,
                    "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                },
                "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                }
            ],
            "id": "1",
            "jsonrpc": "2.0"
        }

        resp = self.post_request(self.v3_as_gi, dataset_request)
        if resp is not None:
            for permid in resp:
                return DataSet(self, permid, resp[permid])


    def get_samples(self, sample_identifiers):
        """Retrieve metadata for the sample.
        Get metadata for the sample and any directly connected parents of the sample to allow access
        to the same information visible in the ELN UI. The metadata will be on the file system.
        :param sample_identifiers: A list of sample identifiers to retrieve.
        """

        if not isinstance(sample_identifiers, list):
            sample_identifiers = [sample_identifiers]


        searches = []

        for ident in sample_identifiers:

            # assume we got a sample identifier e.g. /TEST/TEST-SAMPLE
            match = re.match('/', ident)
            if match:
                searches.append({
                    "identifier": ident,
                    "@type": "as.dto.sample.id.SampleIdentifier"
                })
                continue

            # look if we got a PermID eg. 234567654345-123
            match = re.match('\d+\-\d+', ident)
            if match:
                searches.append({
                    "permId": ident,
                    "@type": "as.dto.sample.id.SamplePermId"
                })
                continue

            raise ValueError(
                '"' + ident + '" is neither a Sample Identifier nor a PermID'
            )

        sample_request = {
            "method": "getSamples",
            "params": [
                self.token,
                searches,
                {
                    "type": {
                        "@type": "as.dto.sample.fetchoptions.SampleTypeFetchOptions"
                    },
                    "properties": {
                        "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                    },
                    "parents": {
                        "@id": 20,
                        "@type": "as.dto.sample.fetchoptions.SampleFetchOptions",
                        "properties": {
                        "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                        }
                    },
                    "dataSets": {
                        "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions",
                        "properties": {
                        "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                        }
                    }
                }
            ],
            "id": "1",
            "jsonrpc": "2.0"
        }

        resp = requests.post(self.host + self.v3_as_gi, json.dumps(sample_request))
        if resp.ok:
            data = resp.json()
            if "error" in data:
                raise ValueError("Request produced an error: " + data["message"])
            else:
                return data['result']


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


class DataSet(Openbis):
    """objects which contain datasets"""

    def __init__(self, openbis_obj, permid, data):
        self.openbis = openbis_obj
        self.permid  = permid
        self.data    = data
        self.v1_ds_api = '/datastore_server/rmi-dss-api-v1.json'
        self.downloadUrl = self.data['dataStore']['downloadUrl']


    def ensure_folder_exists(folder):
        if not os.path.exists(folder):
            os.makedirs(folder)


    def save_dataset(self):
        base_url = self.downloadUrl + '/datastore_server/' + self.permid + '/'

        for file in self.get_file_list:
            if file['isDirectory']:
                ensure_folder_exists(file['pathInDataSet'])
                continue
            else:
                download_url = base_url + file['pathInDataSet'] + '?sessionID=' + self.openbis.token 

        download_url = 'http://localhost:20001/datastore_server/20130412142942295-198/thumbnails_256x256.h5ar/wA10_d1-1_cCy5.png?sessionID=openbis_test_js-160530221217822xC35C2A54D2BB472471D3D110EA68F936'

        
    def get_file_list(self, recursive=True):
        request = {
            "method" : "listFilesForDataSet",
            "params" : [ 
                self.openbis.token,
                self.permid, 
                "/",
                recursive,
            ],
            "id":"1"
        }

        resp = requests.post(self.downloadUrl + self.v1_ds_api, json.dumps(request))

        if resp.ok:
            if 'error' in resp.json():
                raise ValueError('an error has occured: ' + resp.json()['error'] )
            elif 'result' in resp.json():
                return resp.json()['result']
            else:
                raise ValueError('request did not return either result nor error')
        else:
            raise ValueError('general error while performing post request')
