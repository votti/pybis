#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
auth.py


Created by Chandrasekhar Ramakrishnan on 2016-05-25.
Copyright (c) 2016 ETH Zuerich All rights reserved.
"""

from jupyterhub.auth import LocalAuthenticator
from pybis.pybis import Openbis, OpenbisCredentials

import re

from tornado import gen
from traitlets import Unicode


class OpenbisAuthenticator(LocalAuthenticator):
    server_url = Unicode(
        config=True,
        help='URL of openBIS server to contact'
    )

    valid_username_regex = Unicode(
        r'^[a-z][.a-z0-9_-]*$',
        config=True,
        help="""Regex to use to validate usernames before sending to openBIS."""
    )

    @gen.coroutine
    def authenticate(self, handler, data):
        username = data['username']
        password = data['password']

        # Protect against invalid usernames as well as LDAP injection attacks
        if not re.match(self.valid_username_regex, username):
            self.log.warn('Invalid username')
            return None

        # No empty passwords!
        if password is None or password.strip() == '':
            self.log.warn('Empty password')
            return None

        openbis = Openbis(self.server_url)
        try:
            openbis.login(username, password, True)
            return username
        except ValueError as err:
            self.log.warn(str(err))
            return None