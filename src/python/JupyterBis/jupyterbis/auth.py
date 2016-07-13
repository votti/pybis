#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
auth.py


Created by Chandrasekhar Ramakrishnan on 2016-05-25.
Copyright (c) 2016 ETH Zuerich All rights reserved.
"""

import os
import re

from jupyterhub.auth import LocalAuthenticator
from tornado import gen
from traitlets import Unicode, Bool

from pybis.pybis import Openbis

user_to_openbis_dict = {}


class OpenbisAuthenticator(LocalAuthenticator):
    server_url = Unicode(
        config=True,
        help='URL of openBIS server to contact'
    )

    verify_certificates = Bool(
        config=True,
        default_value=True,
        help='Should certificates be verified? Normally True, but maybe False for debugging.'
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


        openbis = Openbis(self.server_url, verify_certificates=self.verify_certificates)
        try:
            # authenticate against openBIS and store the token (if possible)
            openbis.login(username, password)
            user_to_openbis_dict[username] = openbis
            self.refresh_token(username)
            return username
        except ValueError as err:
            self.log.warn(str(err))
            return None


    def refresh_token(self, username):
        if username in user_to_openbis_dict:
            openbis = user_to_openbis_dict[username]
        else:
            return None

        # user has no home directory yet:
        # there is no reason to save the token
        homedir = os.path.expanduser("~"+username)
        if not os.path.exists(homedir):
            return None

        # remove existing token
        parent_folder = os.path.join(homedir, '.pybis' )
        token_path = openbis.gen_token_path(parent_folder)
        try:
            openbis.delete_token(token_path)
        except:
            pass

        # save the new token
        openbis.save_token(
            token=openbis.token,
            parent_folder=parent_folder
        )

        # change the ownership of the token to make sure it is not owned by root
        change_ownership = "sudo chown %s:%s %s" % (username, username, parent_folder)
        os.system(change_ownership)
        change_ownership = "sudo chown %s:%s %s" % (username, username, openbis.token_path)
        os.system(change_ownership)


    def pre_spawn_start(self, user, spawner):
        """After successful login and creating user on the system,
        write the token to a file"""

        self.refresh_token(user.name)


    def logout_url(self, base_url):
        ''' Custon logout
        '''
        pass
