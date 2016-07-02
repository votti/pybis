import pytest

from pybis import Openbis

@pytest.yield_fixture(scope="module")
def openbis_instance():
    instance = Openbis("http://localhost:20000")
    print("\nLOGGING IN...")
    instance.login('admin','anypassword')
    yield instance
    instance.logout()
    print("LOGGED OUT...")
