#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pybis.py

A class with methods for interacting with openBIS.

Created by Chandrasekhar Ramakrishnan on 2016-05-10.
Copyright (c) 2016 ETH Zuerich. All rights reserved.
"""


class OpenbisCredentials:
    """Credentials for communicating with openBIS."""

    def __init__(self, token = None, uname_and_pass = None):
        """A connection can be authenticated either by a token or a username and password combination
        :param token: An authentication token for openBIS, can be None.
        :param uname_and_pass: A tuple with username and password, in that order.
        """
        self.token = token
        self.uname_and_pass = uname_and_pass

    def has_token(self):
        return self.token is not None

    def has_username_and_password(self):
        return self.uname_and_pass is not None

    @property
    def username(self):
        return self.uname_and_pass[0]

    @property
    def password(self):
        return self.uname_and_pass[1]
