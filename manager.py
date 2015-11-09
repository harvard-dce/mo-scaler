#!/usr/bin/env python

import click
import dotenv
import unipath
import moscaler
from click.exceptions import UsageError
from moscaler import OpsworksController, utils
from os import getenv as env

base_dir = unipath.Path(__file__).absolute().parent
dotenv.load_dotenv(base_dir.child('.env'))

class ManagerState(object):
    def __init__(self):
        self.cluster = env('CLUSTER_NAME')

@click.group()
@click.option('-c','--cluster', help="opsworks cluster name")
@click.option('-p','--profile', help="set/override default aws credentials profile")
@click.version_option(moscaler.__version__)
@click.pass_context
def cli(ctx, cluster, profile):

    state = ManagerState()
    if cluster is not None:
        state.cluster = cluster
    elif state.cluster is None:
        raise UsageError("No cluster specified")
    state.aws_profile = profile

    ctx.obj = state


@cli.command()
@click.pass_obj
def status(state):

    controller = OpsworksController(state.cluster, aws_profile=state.aws_profile)
    status = controller.status()
#    log.debug("Status for cluster '%s': %s", state.cluster, status, extra=status)
#    utils.print_status(status)

@cli.group()
def scale():
    pass


@scale.command()
def up(state):
    pass

@scale.command()
def down(state):
    pass

@scale.command()
def auto(state):
    pass

if __name__ == "__main__":
    cli()
