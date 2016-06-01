import json
from pybis import DataSet
from pybis import Openbis

def test_token(openbis_instance):
    assert openbis_instance.token is not None


def test_get_samples_by_id(openbis_instance):
    response = openbis_instance.get_samples('/TEST/TEST-SAMPLE-2-CHILD-1')
    assert response is not None
    assert response['/TEST/TEST-SAMPLE-2-CHILD-1'] is not None


def test_get_samples_by_permid(openbis_instance):
    response = openbis_instance.get_samples('20130415091923485-402')
    assert response is not None
    assert response['20130415091923485-402'] is not None


def test_get_parents(openbis_instance):
    id = '/TEST/TEST-SAMPLE-2'
    response = openbis_instance.get_samples(id)
    assert response is not None
    #print(json.dumps(response))
    assert 'parents' in response[id]
    assert 'identifier' in response[id]['parents'][0]
    assert response[id]['parents'][0]['identifier']['identifier'] == '/TEST/TEST-SAMPLE-2-PARENT'


def test_get_dataset_by_permid(openbis_instance):
    permid = '20130412142942295-198'
    dataset = openbis_instance.get_dataset(permid)
    assert dataset is not None
    assert isinstance(dataset, DataSet)
    assert isinstance(dataset, Openbis)
    assert 'dataStore' in dataset.data
    assert 'downloadUrl' in dataset.data['dataStore']
    file_list = dataset.get_file_list(recursive=False)
    assert file_list is not None
    assert isinstance(file_list, list)
    assert len(file_list) == 1
    
    
    
