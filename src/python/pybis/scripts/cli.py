#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
cli.py

Command-line interface to interact with openBIS.

Created by Chandrasekhar Ramakrishnan on 2016-05-09.
Copyright (c) 2016 ETH Zuerich. All rights reserved.
"""

from datetime import datetime

import click


def click_progress(progress_data):
    timestamp = datetime.now().strftime("%H:%M:%S")
    click.echo("{} {}".format(timestamp, progress_data['message']))


def click_progress(progress_data):
    timestamp = datetime.now().strftime("%H:%M:%S")
    click.echo("{} {}".format(timestamp, progress_data['message']))


@click.group()
@click.option('-q', '--quiet', default=False, is_flag=True, help='Suppress status reporting.')
@click.pass_context
def cli(ctx, quiet):
    ctx.obj['quiet'] = quiet


@cli.command()
@click.pass_context
@click.option('-u', '--username', prompt=True)
@click.option('-p', '--password', prompt=True, hide_input=True, confirmation_prompt=False)
def login(ctx, username, password):
    click.echo("login {}".format(username))


def main():
    cli(obj={})


if __name__ == '__main__':
    main()
