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
    server_address = Unicode(
        config=True,
        help='Address of openBIS server to contact'
    )
    server_port = Int(
        config=True,
        help='Port on which to contact openBIS server',
    )

    def _server_port_default(self):
        if self.use_ssl:
            return 443  # default SSL port for openBIS
        else:
            return 80  # default plaintext port for openBIS

    use_ssl = Bool(
        True,
        config=True,
        help='Use SSL to encrypt connection to openBIS server'
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

        openbis = Openbis(self.server_address + ":" + str(self.server_port))
        try:
            credentials = OpenbisCredentials(uname_and_pass=(username, password))
            # openbis.login()
            return username
        except:
            self.log.warn('Invalid password')
            return None
