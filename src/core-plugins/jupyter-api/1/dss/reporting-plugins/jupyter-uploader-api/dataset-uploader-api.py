#
# Copyright 2014 ETH Zuerich, Scientific IT Services
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# IDataSetRegistrationTransactionV2 Class
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause, SearchOperator, MatchClauseAttribute
from ch.systemsx.cisd.openbis.dss.generic.shared import ServiceProvider

from org.apache.commons.io import IOUtils
from java.io import File
from java.io import FileOutputStream
from java.lang import System
#from net.lingala.zip4j.core import ZipFile
from ch.systemsx.cisd.common.exceptions import UserFailureException

import time
import subprocess
import os
import re
import sys



def getSampleByIdentifier(transaction, identifier):
    space = identifier.split("/")[1]
    code = identifier.split("/")[2]

    criteria = SearchCriteria()
    criteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.SPACE, space))
    criteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, code))
    criteria.setOperator(SearchOperator.MATCH_ALL_CLAUSES)

    search_service = transaction.getSearchService()
    found = list(search_service.searchForSamples(criteria))
    if len(found) == 1:
        return transaction.makeSampleMutable(found[0]);
    else:
        raise UserFailureException(identifier + "Not found by search service.");


def get_dataset_for_name(transaction, dataset_name):

    search_service = transaction.getSearchService()
    criteria = SearchCriteria()
    criteria.addMatchClause(MatchClause.createPropertyMatch('NAME', dataset_name))

    found = list(search_service.searchForDataSets(criteria))
    if len(found) == 1:
        return found[0]
    else:
        return None

def get_dataset_for_permid(transaction, permid):

    search_service = transaction.getSearchService()
    criteria = SearchCriteria()
    criteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, permid));

    found = list(search_service.searchForDataSets(criteria))
    if len(found) == 1:
        return found[0]
    else:
        return None

def get_username_sessionid(sessionToken):
    """ divides a session-token into username and sessionId. Username may contain a dash (-)
    """
    m = re.compile('(.*)-([^-]*)').match(sessionToken)
    if m:
        return m.group(1), m.group(2)


def process(transaction, parameters, tableBuilder):
    ''' 
    This method is called from openBIS DSS.
    The transaction object has a number of methods described in ...
    The parameters are passed with the createReportFromAggregationService method
    and need to be accessed like this:
       parameters.get('my_param')
    tableBuilder is needed to create an appropiate return message.
    A number of magic variables are present, described in PluginScriptRunnerFactory:
    - userSessionToken : the Session Token used by every call
    - userId           : the username
    - searchService    :
    - searchServiceUnfiltered :
    - queryService     :
    - mailService      :
    - authorizationService :
    - contentProvider  :
    - contentProviderUnfiltered 

    '''
    # check for mandatory parameters
    for param in ('sample', 'container'):
        if parameters.get(param) is None:
            raise UserFailureException("mandatory parameter " + param + " is missing")

#    print(str(transaction.getOpenBisServiceSessionToken()))
    # all print statements are written to openbis/servers/datastore_server/log/startup_log.txt
    print('userSessionToken: ' + userSessionToken)

    # get mandatory sample to connect the container to
    sampleIdentifier = parameters.get("sample").get("identifier")
    print('sampleIdentifier: ' + sampleIdentifier)
    if sampleIdentifier is None:
        raise UserFailureException('mandatory parameter sample["identifier"] is missing')
    sample = getSampleByIdentifier(transaction, sampleIdentifier)
    if sample == None: 
        raise UserFailureException("no sample found with this identifier: " + sampleIdentifier)

    #container = registerContainer(transaction, sample, parameters)
    #everything_ok = register_files(transaction, sample, container, parameters)
    if parameters.get("result").get("fileNames") is not None:
        everything_ok = register_files(
            transaction, 
            "JUPYTER_RESULT",
            sample, 
            'container', 
            parameters.get("result").get("fileNames")
        )

    if parameters.get("notebook").get("fileNames") is not None:
        everything_ok = register_files(
            transaction, 
            "JUPYTER_NOTEBOOK",
            sample, 
            'container',
            parameters.get("notebook").get("fileNames")
        )

    
    # create the dataset
    if everything_ok:
        # Success message
        tableBuilder.addHeader("STATUS")
        tableBuilder.addHeader("MESSAGE")
        tableBuilder.addHeader("RESULT")
        row = tableBuilder.addRow()
        row.setCell("STATUS","OK")
        row.setCell("MESSAGE", "Dataset registration successful")
        row.setCell("RESULT", None)

    else:
        # Error message
        tableBuilder.addHeader("STATUS")
        tableBuilder.addHeader("MESSAGE")
        row = tableBuilder.addRow()
        row.setCell("STATUS","FAIL")
        row.setCell("MESSAGE", "Dataset registration failed")


def getThreadProperties(transaction):
  threadPropertyDict = {}
  threadProperties = transaction.getGlobalState().getThreadParameters().getThreadProperties()
  for key in threadProperties:
    try:
      threadPropertyDict[key] = threadProperties.getProperty(key)
    except:
      pass
  return threadPropertyDict

def registerContainer(transaction, sample, parameters):

    container_name = parameters.get("container").get("name")
    container_description = parameters.get("container").get("description")

    # make sure container dataset doesn't exist yet
    container = get_dataset_for_name(transaction, container_name)
    if container is None:
        print("creating JUPYTER_CONTAINER dataset...")
        # Create new container (a dataset of type "JUPYTER_CONTAINER")
        container = transaction.createNewDataSet("JUPYTER_CONTAINER")
        container.setSample(sample)
        container.setPropertyValue("NAME", container_name)
        container.setPropertyValue("DESCRIPTION", container_description)
        #container.setParentDatasets(parameters.get("parents"))
        print("JUPYTER_CONTAINER permId: " + container.getDataSetCode())

    # Assign Data Set properties
    # set name and description
    for key in parameters.get("container").keySet():
        propertyValue = unicode(parameters.get(key))
        if propertyValue == "":
            propertyValue = None
        container.setPropertyValue(key,propertyValue)

    # TODO: container registrieren, aber wie???
    #transaction.moveFile(None, container);
    return container

def register_files(transaction, dataset_type, sample, container, file_names):
    """ creates a new dataset of type JUPYTER_RESULT.
    the parent dataset is the JUPYTER_CONTAINER we just created
    - the result files are copied from the session workspace
      to a temp dir close to the DSS: prepareFilesForRegistration()
    - from there, the files are moved to the DSS: transaction.moveFile()
    - finally, the remaining files are deleted from the session workspace
    """
    print("creating " + dataset_type + " dataset...")
    result_ds = transaction.createNewDataSet(dataset_type)
    result_ds.setSample(sample)
    #result_ds.setParentDatasets([container.getDataSetCode()])
    #print("JUPYTER RESULT permId: " + container.getDataSetCode())
    
    # copy the files to the temp dir
    temp_dir = prepareFilesForRegistration(transaction, file_names)
    transaction.moveFile(File(temp_dir).getAbsolutePath(), result_ds);

    # ...and delete all files from the session workspace
    # TODO: delete it later
    #dss_service = ServiceProvider.getDssServiceRpcGeneric().getService()
    #for file_name in file_names:
    #    file_path = os.path.join(temp_dir, file_name)
    #    dss_service.deleteSessionWorkspaceFile(userSessionToken, file_name)

    return True


def prepareFilesForRegistration(transaction, files=[]):
    """ Bring files to the same file system as the dropbox.
    The session workspace may be on a different file system from the dropbox.
    We need to ensure that all files are on the dropbox file system.
    """
    # create a local temp dir with a timestamp
    threadProperties = getThreadProperties(transaction)
    #temp_dir =  os.path.join( threadProperties[u'incoming-dir'], str(time.time()) )
    temp_dir =  threadProperties[u'incoming-dir']
    File(temp_dir).mkdirs()

    dss_service = ServiceProvider.getDssServiceRpcGeneric().getService()

    # download all files from the session workspace to the temp dir
    for file_name in files:
        # create input stream
        print("file_name: " + file_name)
        inputStream = dss_service.getFileFromSessionWorkspace(userSessionToken, file_name)

        # ensure that all necessary folders exist
        file_path = os.path.join(temp_dir, file_name)
        print("file_path: "+file_path)
        
        try:
            os.makedirs(os.path.dirname(file_path))
        except:
            pass
        print("dirname: " + os.path.dirname(file_path))

        # create output stream
        tempFile = File(file_path)
        outputStream = FileOutputStream(tempFile)
        IOUtils.copyLarge(inputStream, outputStream)
        IOUtils.closeQuietly(inputStream)
        IOUtils.closeQuietly(outputStream)

    return temp_dir

def registerNotebook(transaction, sample, container, parameters):
    """ creates a new dataset of type JUPYTER_NOTEBOOK.
    the parent dataset is the container.
    """
    dataSet = transaction.createNewDataSet("JUPYTER_NOTEBOOK")
    dataSet.setSample(sample)
    parent_codes = [container.getDataSetCode()]
    dataSet.setParentDatasets(parent_codes)

    files = parameters.get("notebook").get("fileNames")
    for file in files:
        pass

    return dataSet


def getThreadProperties(transaction):
  threadPropertyDict = {}
  threadProperties = transaction.getGlobalState().getThreadParameters().getThreadProperties()
  for key in threadProperties:
    try:
      threadPropertyDict[key] = threadProperties.getProperty(key)
    except:
      pass
  return threadPropertyDict

