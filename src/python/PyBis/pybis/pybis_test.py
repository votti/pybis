#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pybis_test.py

Tests for the pybis module. Written using pytest.

Created by Chandrasekhar Ramakrishnan on 2016-05-10.
Copyright (c) 2016 ETH Zuerich. All rights reserved.
"""

from .pybis import OpenbisCredentials, OpenbisCredentialStore


def test_credentials_store(tmpdir):
    credentials = OpenbisCredentials("magic_token")
    store = OpenbisCredentialStore(str(tmpdir))
    store.write(credentials)
    disk_credentials = store.read()
    assert credentials.token == disk_credentials.token
    assert not disk_credentials.has_username_and_password()
