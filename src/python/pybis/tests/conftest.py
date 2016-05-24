import pytest

from pybis import Openbis

@pytest.yield_fixture(scope="module")
def openbis_instance():
    instance = Openbis("http://localhost:20000/openbis/openbis/rmi-application-server-v3.json")
    print("\nLOGGING IN...")
    instance.login()
    yield instance
    instance.logout()
    print("LOGGED OUT...")
