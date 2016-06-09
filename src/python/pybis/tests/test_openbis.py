import json
from pybis import DataSet
from pybis import Openbis

def test_token(openbis_instance):
    assert openbis_instance.hostname is not None
    new_instance = Openbis(openbis_instance.url)
    new_instance.login()
    assert new_instance.token is not None
    assert new_instance.is_token_valid() is True
    new_instance.logout()
    assert new_instance.is_token_valid() is False

    openbis_instance.save_token()
    another_instance = Openbis(openbis_instance.url)
    assert another_instance.is_token_valid() is True


def test_get_sample_by_id(openbis_instance):
    response = openbis_instance.get_sample('/TEST/TEST-SAMPLE-2-CHILD-1')
    assert response is not None
    assert response.ident == '/TEST/TEST-SAMPLE-2-CHILD-1'


def test_get_sample_by_permid(openbis_instance):
    response = openbis_instance.get_sample('20130415091923485-402')
    assert response is not None
    assert response.permid == '20130415091923485-402'


def test_get_sample_parents(openbis_instance):
    id = '/TEST/TEST-SAMPLE-2'
    sample = openbis_instance.get_sample(id)
    assert sample is not None
    assert 'parents' in sample.data
    assert 'identifier' in sample.data['parents'][0]
    assert sample.data['parents'][0]['identifier']['identifier'] == '/TEST/TEST-SAMPLE-2-PARENT'
    parents = sample.get_parents()
    assert isinstance(parents, list)
    assert parents[0].ident == '/TEST/TEST-SAMPLE-2-PARENT' 

def test_get_sample_children(openbis_instance):
    id = '/TEST/TEST-SAMPLE-2'
    sample = openbis_instance.get_sample(id)
    assert sample is not None
    assert 'children' in sample.data
    assert 'identifier' in sample.data['children'][0]
    assert sample.data['children'][0]['identifier']['identifier'] == '/TEST/TEST-SAMPLE-2-CHILD-1'
    children = sample.get_children()
    assert isinstance(children, list)
    assert children[0].ident == '/TEST/TEST-SAMPLE-2-CHILD-1' 


def test_get_dataset_parents(openbis_instance):
    permid = '20130415093804724-403'
    parent_permid = '20130415100158230-407'
    dataset = openbis_instance.get_dataset(permid)
    assert dataset is not None
    parents = dataset.get_parents()
    assert isinstance(parents, list)
    assert parents[0] is not None
    assert isinstance(parents[0], DataSet)
    assert parents[0].permid == parent_permid

    children = parents[0].get_children()
    assert isinstance(children, list)
    assert children[0] is not None
    assert isinstance(children[0], DataSet)


def test_get_dataset_by_permid(openbis_instance):
    permid = '20130412142942295-198'
    permid = '20130412153118625-384'
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

    file_list = dataset.get_file_list(recursive=True)
    assert file_list is not None
    assert len(file_list) > 10

 
