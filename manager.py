#!/usr/bin/env python

import boto3
import click
import dotenv
import unipath
import moscaler
from click.exceptions import UsageError
from moscaler import OpsworksController, utils
from os import getenv as env

base_dir = unipath.Path(__file__).absolute().parent
dotenv.load_dotenv(base_dir.child('.env'))

@click.group()
@click.option('-c','--cluster', help="opsworks cluster name")
@click.option('-p','--profile', help="set/override default aws credentials profile")
@click.version_option(moscaler.__version__)
@click.pass_context
def cli(ctx, cluster, profile):

    if cluster is None:
        cluster = env('MOSCALER_CLUSTER')
        if cluster is None:
            raise UsageError("No cluster specified")

    if profile is not None:
        boto3.setup_default_session(profile_name=profile)

    ctx.obj = OpsworksController(cluster)


@cli.command()
@click.pass_obj
def status(controller):

    status = controller.status()
    utils.print_status(status)

@cli.group()
@click.option('-f', '--force', is_flag=True)
@click.pass_obj
def scale(controller, force):
    if force:
        controller.force = True

@scale.command()
@click.argument('num_workers', type=int)
@click.pass_obj
def to(controller, num_workers):

    controller.scale_to(num_workers)

@scale.command()
@click.pass_obj
def up(controller):
    pass

@scale.command()
@click.pass_obj
def down(controller):
    pass

@scale.command()
@click.pass_obj
def auto(controller):
    pass

if __name__ == "__main__":
    cli()
