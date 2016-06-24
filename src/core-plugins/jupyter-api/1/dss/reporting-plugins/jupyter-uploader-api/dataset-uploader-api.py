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

CONTAINER_NAME = 'NAME'


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
    criteria.addMatchClause(MatchClause.createPropertyMatch(CONTAINER_NAME, dataset_name))

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

    # check for mandatory parameters
    for param in ('sample', 'container', 'sessionToken'):
        if parameters.get(param) is None:
            raise UserFailureException("mandatory parameter " + param + " is missing")

    # Obtain the user using the dropbox
    # TODO Is this necessary? user is already in the transaction...
    sessionToken = parameters.get("sessionToken")
    username, sessionId = get_username_sessionid(sessionToken)

    ## TODO where does userId come from?
    #if sessionId == userId:
    #    transaction.setUserId(userId)
    #else:
    #    errorMessage = "[SECURITY] User " + userId + " tried to use " + sessionId + " account, this will be communicated to the admin."
    #    print(errorMessage)
    #    raise UserFailureException(errorMessage)


    # get mandatory sample to connect the container to
    sampleIdentifier = parameters.get("sample").get("identifier")
    if sampleIdentifier is None:
        raise UserFailureException('mandatory parameter sample["identifier"] is missing')
    sample = getSampleByIdentifier(transaction, sampleIdentifier)
    if sample == None: 
        raise UserFailureException("no sample found with this identifier: " + sampleIdentifier)

    container = registerContainer(transaction, sample, parameters)
    result_ds = registerResultFiles(transaction, sample, container, parameters)
    notebook = registerNotebook(transaction, sample, container, parameters)

    everything_ok = True
    
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

    # make sure container dataset doesn't exist yet
    container = get_dataset_for_name(transaction, parameters.get("container").get("name"))
    if container is None:
        # Create new container
        container = transaction.createNewDataSet("JUPYTER_CONTAINER")
        container.setSample(sample)
        container.setParentDatasets(parameters.get("parents"))

    # Assign Data Set properties
    # set name and description
    for key in parameters.get("container").keySet():
        propertyValue = unicode(parameters.get(key))
        if propertyValue == "":
            propertyValue = None
        container.setPropertyValue(key,propertyValue)

    return container

def registerResultFiles(transaction, sample, container, parameters):
    """ creates a new dataset of type JUPYTER_RESULT.
    the parent dataset is the container.
    - copies the result files in a temp dir close to the DSS
    - moves the result files to the DSS
    """
    result_ds = transaction.createNewDataSet("JUPYTER_RESULT")
    result_ds.setSample(sample)
    parent_codes = [container.getDataSetCode()]
    result_ds.setParentDatasets(parent_codes)
    
    files = parameters.get("result").get("fileNames")
    folder = parameters.get("result").get("folerName")
    temp_dir = prepareFilesForRegistration(transaction, files, 'results', parameters)

    dss_service = ServiceProvider.getDssServiceRpcGeneric().getService()
    session_token = parameters.get("sessionToken")

    # Move result files to the result dataset
    transaction.moveFile(temp_dir, result_ds)

    # ...and delete all files from the session workspace
    for file_name in files:
        file_path = os.path.join(temp_dir, file_name)
        dss_service.deleteSessionWorkspaceFile(session_token, file_name)

    return result_ds


def prepareFilesForRegistration(transaction, files, folder_name, parameters):
    """ Bring files to the same file system as the dropbox.
    The session workspace may be on a different file system from the dropbox.
    We need to ensure that all files are on the dropbox file system.
    """
    # create a local temp dir with a timestamp
    threadProperties = getThreadProperties(transaction)
    temp_dir =  threadProperties[u'incoming-dir'] + '/' + folder_name + '/' + str(time.time())
    temp_dirFile = File(temp_dir)
    temp_dirFile.mkdirs()

    session_token = parameters.get("sessionToken")
    dss_service = ServiceProvider.getDssServiceRpcGeneric().getService()

    # download all files from the session workspace to the temp dir
    for file_name in files:
        inputStream = dss_service.getFileFromSessionWorkspace(session_token, file_name)
        file_path = File(os.path.join(temp_dir, file_name))
        # file_name may contain a subfolder:
        # create the necessary directory structure if they don't exist yet
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        outputStream = FileOutputStream(file_path)

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

