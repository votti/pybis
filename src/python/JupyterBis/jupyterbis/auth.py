#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
auth.py


Created by Chandrasekhar Ramakrishnan on 2016-05-25.
Copyright (c) 2016 ETH Zuerich All rights reserved.
"""

from jupyterhub.auth import Authenticator
from pybis.pybis import Openbis, OpenbisCredentials

import re

from tornado import gen
from traitlets import Unicode, Int, Bool


class OpenbisAuthenticator(Authenticator):
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

        print("Authenticate user " + username)

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
            credentials = OpenbisCredentials(uname_and_pass=(username, password))
            # openbis.login()
            return username
        except:
            self.log.warn('Invalid password')
            return None
