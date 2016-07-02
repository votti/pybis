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
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import json
import re
from urllib.parse import urlparse

import threading
from threading import Thread
from queue import Queue
DROPBOX_PLUGIN = "jupyter-uploader-api"

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

    def __init__(self, url, verify_certificates=True, use_cached_token=False ):
        """Initialize an interface to openBIS with information necessary to connect to the server.
        :param host:
        """

        url_obj = urlparse(url)
        if  url_obj.netloc is None:
            raise ValueError("please provide the url in this format: https://openbis.host.ch:8443")

        self.url_obj = url_obj
        self.url     = url_obj.geturl()
        self.port    = url_obj.port
        self.hostname = url_obj.hostname
        self.as_v3 = '/openbis/openbis/rmi-application-server-v3.json'
        self.as_v1 = '/openbis/openbis/rmi-general-information-v1.json'
        self.reg_v1 = '/openbis/openbis/rmi-query-v1.json'
        self.verify_certificates = verify_certificates
        self.token = None
        if use_cached_token:
            self.initialize_token()


    def initialize_token(self):
        """Read the token from the cache, and set the token ivar to it, if there, otherwise None."""
        token_path = self.token_path()
        if not os.path.exists(token_path):
            self.token = None
            return
        try:
            with open(token_path) as f:
                self.token = f.read()
                if not self.is_token_valid():
                    self.token = None
                    os.remove(token_path)
        except FileNotFoundError:
            self.token = None

    def token(self):
        if self.token is None:
            raise ValueError('no valid session available')


    def token_path(self, parent_folder=None):
        """Return the path to the token file."""
        if parent_folder is None:
            parent_folder = os.path.expanduser("~")
        path = os.path.join(parent_folder, '.pybis', self.hostname + '.token')
        return path


    def save_token(self, parent_folder=None):
        token_path = self.token_path(parent_folder)
        os.makedirs(os.path.dirname(token_path), exist_ok=True)
        with open(token_path, 'w') as f:
            f.write(self.token)


    def post_request(self, resource, data):
        resp = requests.post(self.url + resource, json.dumps(data), verify=self.verify_certificates)

        if resp.ok:
            data = resp.json()
            if 'error' in data:
                raise ValueError('an error has occured: ' + data['error']['message'] )
            elif 'result' in data:
                return data['result']
            else:
                raise ValueError('request did not return either result nor error')
        else:
            raise ValueError('general error while performing post request')


    def logout(self):
        if self.token is None:
            return

        logout_request = {
            "method":"logout",
            "params":[self.token],
            "id":"1",
            "jsonrpc":"2.0"
        }
        resp = self.post_request(self.as_v3, logout_request)
        return resp


    def login(self, username=None, password=None, store_credentials=False):
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
        result = self.post_request(self.as_v3, login_request)
        if result is None:
            raise ValueError("login to openBIS failed")
        else:
            self.token = result


    def get_datastores(self):
        datastores = []
        request = {
            "method": "listDataStores",
            "params": [ self.token ],
            "id": "1",
            "jsonrpc": "2.0"
        }
        resp = self.post_request(self.as_v1, request)
        return resp
        

    def is_token_valid(self, token=None):
        """Check if the connection to openBIS is valid.
        This method is useful to check if a token is still valid or if it has timed out,
        requiring the user to login again.
        :return: Return True if the token is valid, False if it is not valid.
        """
        if token is None:
            token = self.token
        
        if token is None:
            return False

        request = {
            "method": "isSessionActive",
            "params": [ token ],
            "id": "1",
            "jsonrpc": "2.0"
        }
        resp = self.post_request(self.as_v1, request)
        return resp


    def get_dataset(self, permid):
        """fetch a dataset and some metadata attached to it:
        - properties
        - sample
        - parents
        - children
        - containers
        - dataStore
        - physicalData
        - linkedData
        :return: a DataSet object
        """

        dataset_request = {
            "method": "getDataSets",
            "params": [
                self.token,
                [
                    {
                        "permId": permid,
                        "@type": "as.dto.dataset.id.DataSetPermId"
                    }
                ],
                {
                "parents": {
                    "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                },
                "children": {
                  "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                },
                "containers": {
                    "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                },
                "physicalData": {
                    "@type": "as.dto.dataset.fetchoptions.PhysicalDataFetchOptions"
                },
                "linkedData": {
                    "@type": "as.dto.dataset.fetchoptions.LinkedDataFetchOptions",
                },
                "dataStore": {
                    "@type": "as.dto.datastore.fetchoptions.DataStoreFetchOptions",
                },
                "sample": {
                    "@type": "as.dto.sample.fetchoptions.SampleFetchOptions"
                },
                "properties": {
                    "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                },
                "@type": "as.dto.dataset.fetchoptions.DataSetFetchOptions"
                }
            ],
            "id": permid,
            "jsonrpc": "2.0"
        }

        resp = self.post_request(self.as_v3, dataset_request)
        if resp is not None:
            for permid in resp:
                return DataSet(self, permid, resp[permid])


    def get_sample(self, sample_ident):
        """Retrieve metadata for the sample.
        Get metadata for the sample and any directly connected parents of the sample to allow access
        to the same information visible in the ELN UI. The metadata will be on the file system.
        :param sample_identifiers: A list of sample identifiers to retrieve.
        """

        if self.token is None:
            raise ValueError("Please login first")

        search_request = None

        # assume we got a sample identifier e.g. /TEST/TEST-SAMPLE
        match = re.match('/', sample_ident)
        if match:
            search_request = {
                "identifier": sample_ident,
                "@type": "as.dto.sample.id.SampleIdentifier"
            }
        else:
            # look if we got a PermID eg. 234567654345-123
            match = re.match('\d+\-\d+', sample_ident)
            if match:
                search_request = {
                    "permId": sample_ident,
                    "@type": "as.dto.sample.id.SamplePermId"
                }
            else:
                raise ValueError(
                    '"' + sample_ident + '" is neither a Sample Identifier nor a PermID'
                )

        sample_request = {
            "method": "getSamples",
            "params": [
                self.token,
                [
                    search_request, 
                ],
                {
                    "type": {
                        "@type": "as.dto.sample.fetchoptions.SampleTypeFetchOptions"
                    },
                    "properties": {
                        "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                    },
                    "parents": {
                        "@type": "as.dto.sample.fetchoptions.SampleFetchOptions",
                        "properties": {
                            "@type": "as.dto.property.fetchoptions.PropertyFetchOptions"
                        }
                    },
                    "children": {
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
                    },
                    "registrator": {
                        "@type": "as.dto.person.fetchoptions.PersonFetchOptions",
                    },
                    "tags": {
                        "@type": "as.dto.tag.fetchoptions.TagFetchOptions",
                    },
                }
            ],
            "id": sample_ident,
            "jsonrpc": "2.0"
        }
        resp = self.post_request(self.as_v3, sample_request)
        if resp is not None:
            for sample_ident in resp:
                return Sample(self, resp[sample_ident])


    def delete_sample(self, permid, reason):
        sample_delete_request = {
            "method": "deleteSamples",
            "params": [
                self.token,
                [
                    {
                        "permId": permid,
                        "@type": "as.dto.sample.id.SamplePermId"
                    }
                ],
                {
                    "reason": reason,
                    "@type": "as.dto.sample.delete.SampleDeletionOptions"
                }
            ],
            "id": "1",
            "jsonrpc": "2.0"
        }
        resp = self.post_request(self.as_v3, sample_delete_request)
        return


    def new_sample(self, sample_name=None, space_name=None, sample_type=None, tags=[]):
        if sample_type is None:
            sample_type = "UNKNOWN"

        if isinstance(tags, str):
            tags = [tags]
        tag_ids = []
        for tag in tags:
            tag_dict = {
                "code":tag,
                "@type":"as.dto.tag.id.TagCode"
            }
            tag_ids.append(tag_dict)


        sample_create_request = {
            "method":"createSamples",
            "params":[
                self.token,
                [
                    {
                    "properties":{},
                    "typeId":{
                        "permId": sample_type,
                        "@type":"as.dto.entitytype.id.EntityTypePermId"
                    },
                    "code": sample_name,
                    "spaceId":{
                        "permId": space_name,
                        "@type":"as.dto.space.id.SpacePermId"
                    },
                    "tagIds":tag_ids,
                    "@type":"as.dto.sample.create.SampleCreation",
                    "experimentId":None,
                    "containerId":None,
                    "componentIds":None,
                    "parentIds":None,
                    "childIds":None,
                    "attachments":None,
                    "creationId":None,
                    "autoGeneratedCode":None
                    }
                ]
            ],
            "id":"1",
            "jsonrpc":"2.0"
        }
        resp = self.post_request(self.as_v3, sample_create_request)
        if 'permId' in resp[0]:
            return self.get_sample(resp[0]['permId'])
        else:
            raise ValueError("error while trying to fetch sample from server: " + str(resp))


    def new_analysis(self, name, description=None, sample=None, dss_code='DSS1', result_files=None, notebook_files=None, parents=[]):
        """Register a new data set with openBIS.
        :param path_to_notebook: The path to the Jupyter notebook that created this data set
        :param owner_identifier: The identifier of the sample that owns this data set.
        :param paths_to_files: A list of paths to files that should be in the data set.
        :param parent_identifiers: A list of parents for the data set.
        :return:
        """
        if sample is None:
            raise ValueError("please provide a sample object or id")

        if name is None:
            raise ValueError("please provide a name for the analysis")

        sample_ident = None
        if isinstance(sample, str):
            sample_ident = sample
        else:
            sample_ident = sample.ident

        result_folder = 'results/'
        notebook_folder = 'notebooks/'
        parameters = {
            "method"   : "insertDataSet",
            "sample"   : { "identifier": sample_ident },
            "container": { "name": name, "description": description },
        }
        
        # 1. upload the files to the session workspace
        if result_files is not None:
            wsp_files = self.upload_files(files=result_files, folder=result_folder)
            parameters['result'] = { "fileNames" : wsp_files } 
        if notebook_files is not None:
            wsp_files = self.upload_files(files=notebook_files, folder=notebook_folder)
            parameters['notebook'] = { "fileNames" : wsp_files }

#        # 2. start registering files using the jupyter
        register_request = {
            "method": "createReportFromAggregationService",
            "params": [
                self.token,
                dss_code,
                DROPBOX_PLUGIN,
                parameters,
            ],
            "id": "1",
            "jsonrpc": "2.0"
        }
        resp = self.post_request(self.reg_v1, register_request)


    def upload_files(self, dss_code=None, files=None, folder='', wait_until_finished=False):

        for dss in self.get_datastores():
            if dss_code is None:
                # just take first DSS of all available DSSes if none was given
                self.datastore_url = dss['downloadUrl']
                break
            elif dss['code'] == dss_code:
                self.datastore_url = dss['downloadUrl']
                break

        if self.datastore_url is None:
            raise ValueError("No such DSS code found: " + dss_code)
        
        if files is None:
            raise ValueError("Please provide a filename.")

        if isinstance(files, str):
            files = [files]

        self.files = files
        self.startByte = 0
        self.endByte   = 0
    
        # define a queue to handle the upload threads
        queue = DataSetUploadQueue()

        real_files = []
        for filename in files:
            if os.path.isdir(filename):
                real_files.extend([os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(filename)) for f in fn])
            else:
                real_files.append(os.path.join(filename))

        # compose the upload-URL and put URL and filename in the upload queue 
        files_in_wsp = []
        for filename in real_files:
            file_in_wsp = os.path.join(folder, filename)
            files_in_wsp.append(file_in_wsp)
            upload_url = (
                self.datastore_url + '/session_workspace_file_upload'
                + '?filename=' + os.path.join(folder,filename)
                + '&id=1'
                + '&startByte=0&endByte=0'
                + '&sessionID=' + self.token
            )
            queue.put([upload_url, filename, self.verify_certificates])

        # wait until all files have uploaded
        if wait_until_finished:
            queue.join()

        # return files with full path in session workspace
        return files_in_wsp


class DataSetUploadQueue:
   
    def __init__(self, workers=20):
        # maximum files to be uploaded at once
        self.upload_queue = Queue()

        # define number of threads and start them
        for t in range(workers):
            t = Thread(target=self.upload_file)
            t.daemon = True
            t.start()


    def put(self, things):
        """ expects a list [url, filename] which is put into the upload queue
        """
        self.upload_queue.put(things)


    def join(self):
        """ needs to be called if you want to wait for all uploads to be finished
        """
        self.upload_queue.join()


    def upload_file(self):
        while True:
            # get the next item in the queue
            upload_url, filename, verify_certificates = self.upload_queue.get()

            # upload the file to our DSS session workspace
            with open(filename, 'rb') as f:
                resp = requests.post(upload_url, data=f, verify=verify_certificates)
                resp.raise_for_status()

            # Tell the queue that we are done
            self.upload_queue.task_done()


class DataSetDownloadQueue:
    
    def __init__(self, workers=20):
        # maximum files to be downloaded at once
        self.download_queue = Queue()

        # define number of threads
        for t in range(workers):
            t = Thread(target=self.download_file)
            t.daemon = True
            t.start()


    def put(self, things):
        """ expects a list [url, filename] which is put into the download queue
        """
        self.download_queue.put(things)


    def join(self):
        """ needs to be called if you want to wait for all downloads to be finished
        """
        self.download_queue.join()


    def download_file(self):
        while True:
            url, filename, verify_certificates = self.download_queue.get()
            # create the necessary directory structure if they don't exist yet
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            # request the file in streaming mode
            r = requests.get(url, stream=True, verify=verify_certificates)
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)

            self.download_queue.task_done()


class DataSet():
    """objects which contain datasets"""

    def __init__(self, openbis_obj, permid, data):
        self.openbis = openbis_obj
        self.permid  = permid
        self.data    = data
        self.v1_ds = '/datastore_server/rmi-dss-api-v1.json'
        self.downloadUrl = self.data['dataStore']['downloadUrl']


    def download(self, files=None, wait_until_finished=False, workers=10):
        """ download the actual files and put them in the following folder:
        __current_dir__/hostname/dataset_permid/
        """

        if files == None:
            files = self.file_list()
        elif isinstance(files, str):
            files = [files]

        base_url = self.downloadUrl + '/datastore_server/' + self.permid + '/'

        queue = DataSetDownloadQueue(workers=workers)

        # get file list and start download
        for filename in files:
            download_url = base_url + filename + '?sessionID=' + self.openbis.token 
            filename = os.path.join(self.openbis.hostname, self.permid, filename)
            queue.put([download_url, filename, self.openbis.verify_certificates])

        # wait until all files have downloaded
        if wait_until_finished:
            queue.join()


    def get_parents(self):
        parents = []
        for item in self.data['parents']:
            parent = self.openbis.get_dataset(item['code'])
            if parent is not None:
                parents.append(parent)
        return parents


    def get_children(self):
        children = []
        for item in self.data['children']:
            child = self.openbis.get_dataset(item['code'])
            if child is not None:
                children.append(child)
        return children


    def file_list(self):
        files = []
        for file in self.get_file_list(recursive=True):
            if file['isDirectory']:
                pass
            else:
                files.append(file['pathInDataSet'])
        return files

        

    def get_file_list(self, recursive=True, start_folder="/"):
        request = {
            "method" : "listFilesForDataSet",
            "params" : [ 
                self.openbis.token,
                self.permid, 
                start_folder,
                recursive,
            ],
            "id":"1"
        }

        resp = requests.post(self.downloadUrl + self.v1_ds, json.dumps(request))

        if resp.ok:
            data = resp.json()
            if 'error' in data:
                raise ValueError('an error has occured: ' + data['error'] )
            elif 'result' in data:
                return data['result']
            else:
                raise ValueError('request did not return either result nor error')
        else:
            raise ValueError('general error while performing post request')


class AttrDict(dict):
    """ this class is just transforming a dictionary into an object
    """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class Sample(dict):
    """ managing openBIS samples
    """

    def __init__(self, openbis_obj, *args, **kwargs):
        super(Sample, self).__init__(*args, **kwargs)
        self.__dict__ = self
        self.openbis = openbis_obj
        self.permid = self.permId['permId']
        self.ident = self.identifier['identifier']


    def delete(self, permid, reason):
        self.openbis.delete_sample(permid, reason)


    def get_datasets(self):
        datasets = []
        for item in self.dataSets:
            datasets.append(self.openbis.get_dataset(item['permId']['permId']))
        return datasets


    def get_parents(self):
        parents = []
        for item in self.parents:
            parent = self.openbis.get_sample(item['permId']['permId'])
            if parent is not None:
                parents.append(parent)
        return parents


    def get_children(self):
        children = []
        for item in self.children:
            child = self.openbis.get_sample(item['permId']['permId'])
            if child is not None:
                children.append(child)
        return children
