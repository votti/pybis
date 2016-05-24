import json

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

    response.parents[0].id
