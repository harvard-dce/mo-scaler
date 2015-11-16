#!/usr/bin/env python

import boto3
import click
import dotenv
import unipath
import logging
import moscaler
from click.exceptions import UsageError
from moscaler.opsworks import OpsworksController
from moscaler import utils
from os import getenv as env

base_dir = unipath.Path(__file__).absolute().parent
dotenv.load_dotenv(base_dir.child('.env'))

log = logging.getLogger('moscaler')


@click.group()
@click.option('-c', '--cluster', help="opsworks cluster name")
@click.option('-p', '--profile', help="set/override default aws profile")
@click.option('-d', '--debug', help="enable debug output", is_flag=True)
@click.version_option(moscaler.__version__)
@click.pass_context
def cli(ctx, cluster, profile, debug):

    if cluster is None:
        cluster = env('MOSCALER_CLUSTER')
        if cluster is None:
            raise UsageError("No cluster specified")

    if profile is not None:
        boto3.setup_default_session(profile_name=profile)

    init_logging(debug)

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
@click.argument('num_workers', type=int, default=1)
@click.pass_obj
def up(controller, num_workers):

    controller.scale_up(num_workers)


@scale.command()
@click.argument('num_workers', type=int, default=1)
@click.pass_obj
def down(controller, num_workers):

    controller.scale_down(num_workers)


@scale.command()
@click.pass_obj
def auto(controller):
    pass


def init_logging(debug):
    import logging.config
    level = logging.getLevelName(debug and logging.DEBUG or logging.INFO)
    config = {
        'version': 1,
        'loggers': {
            'moscaler': {
                'handlers': ['stdout', 'stderr'],
                'level': level
            }
        },
        'handlers': {
            'stdout': {
                'class': 'logging.StreamHandler',
                'level': level,
                'stream': 'ext://sys.stdout',
                'formatter': 'basic'
            },
            'stderr': {
                'class': 'logging.StreamHandler',
                'level': 'ERROR',
                'stream': 'ext://sys.stderr',
                'formatter': 'basic'
            }
        },
        'formatters': {
            'basic': {
                'format': ("%(asctime)s %(levelname)s "
                           "%(module)s:%(funcName)s %(message)s")
            }
        }
    }

    if env('LOGGLY_TOKEN'):
        config['handlers']['loggly'] = {
            'class': 'pyloggly.LogglyHandler',
            'level': level,
            'formatter': 'basic',
            'token': env('LOGGLY_TOKEN'),
            'host': 'https://logs-01.loggly.com',
            'tags': 'mo-scaler'
        }

    logging.config.dictConfig(config)


if __name__ == "__main__":
    cli()
