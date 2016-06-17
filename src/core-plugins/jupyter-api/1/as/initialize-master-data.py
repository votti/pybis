#
# Copyright 2016 ETH Zuerich, Scientific IT Services
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

##
## Configuration
##
import sys

# MasterDataRegistrationTransaction Class
import os
import ch.systemsx.cisd.openbis.generic.server.jython.api.v1.DataType as DataType

##
## Globals
##
propertiesCache = {};
samplesCache = {};
tr = service.transaction();

##
## API Facade
##
	
	
def createDataSetTypeWithProperties(dataSetCode, kind, description, properties):
	newDataSet = tr.getOrCreateNewDataSetType(dataSetCode);
	newDataSet.setDataSetKind(kind);
	newDataSet.setDescription(description);
	addProperties(newDataSet, properties);
	
def addProperties(entity, properties):
	for property in properties:
		addProperty(entity, property[0], property[1], property[2], property[3], property[4], property[5], property[6], property[7]);
	
def addProperty(entity, propertyCode, section, propertyLabel, dataType, vocabularyCode, propertyDescription, managedScript, dynamicScript):
	property = None;
	
	if propertyCode in propertiesCache:
		property = propertiesCache[propertyCode];
	else:
		property = createProperty(propertyCode, dataType, propertyLabel, propertyDescription, vocabularyCode);
	
	propertyAssignment = tr.assignPropertyType(entity, property);
	if section is not None:
		propertyAssignment.setSection(section);
	propertyAssignment.setShownEdit(True);
	
	if managedScript != None:
		propertyAssignment.setManaged(True);
		propertyAssignment.setScriptName(managedScript);
	if dynamicScript != None:
		propertyAssignment.setDynamic(True);
		propertyAssignment.setShownEdit(False);
		propertyAssignment.setScriptName(dynamicScript);

def createProperty(propertyCode, dataType, propertyLabel, propertyDescription, vocabularyCode):
	property = tr.getOrCreateNewPropertyType(propertyCode, dataType);
	property.setDescription(propertyDescription);
	property.setLabel(propertyLabel);
	propertiesCache[propertyCode] = property;
	if dataType == DataType.CONTROLLEDVOCABULARY:
		property.setVocabulary(vocabulariesCache[vocabularyCode]);
	return property;

def initJupyterMasterData():
	##
	## Property Types for annotations
	##
		
	##
	## DataSet Types
	##
	createDataSetTypeWithProperties("JUPYTER_CONTAINER", "CONTAINER", "Jupyter Analysis Results", [
		["NAME", None, "Name", DataType.VARCHAR, None,	"Name", None, None],
		["DESCRIPTION", None, "Description", DataType.MULTILINE_VARCHAR, None, "A Description", None, None],
	]);
	
	createDataSetTypeWithProperties("JUPYTER_RESULT", "PHYSICAL", "Analysis Results Files", []);
	createDataSetTypeWithProperties("JUPYTER_NOTEBOOK", "PHYSICAL", "Analysis Notebook Files", []);
	
	
initJupyterMasterData();