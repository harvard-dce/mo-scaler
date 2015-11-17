#!/usr/bin/env python

import sys
import boto3
import click
import dotenv
import unipath
import logging
from functools import wraps
from os import getenv as env
from click.exceptions import UsageError

import moscaler
from moscaler.opsworks import OpsworksController
from moscaler.exceptions import OpsworksControllerException
from moscaler import utils

base_dir = unipath.Path(__file__).absolute().parent
dotenv.load_dotenv(base_dir.child('.env'))

LOGGER = logging.getLogger('moscaler')


def handle_exit(cmd):
    """
    execute the command and catch any cluster exceptions. The return value
    will be used as the arg for sys.exit().
    """
    @wraps(cmd)
    def exit_wrapper(cluster, *args, **kwargs):
        try:
            cmd(cluster, *args, **kwargs)
            return 0
        except OpsworksControllerException as exc:
            LOGGER.info(str(exc))
            return str(exc)
    return exit_wrapper


@click.group()
@click.option('-c', '--cluster', help="opsworks cluster name")
@click.option('-p', '--profile', help="set/override default aws profile")
@click.option('-d', '--debug', help="enable debug output", is_flag=True)
@click.option('-f', '--force', is_flag=True)
@click.option('-n', '--dry-run', is_flag=True)
@click.version_option(moscaler.__version__)
@click.pass_context
def cli(ctx, cluster, profile, debug, force, dry_run):

    if cluster is None:
        cluster = env('MOSCALER_CLUSTER')
        if cluster is None:
            raise UsageError("No cluster specified")

    if profile is not None:
        boto3.setup_default_session(profile_name=profile)

    init_logging(debug)

    if force:
        LOGGER.warn("--force mode enabled")
    if dry_run:
        LOGGER.warn("--dry-run mode enabled")

    ctx.obj = OpsworksController(cluster, force, dry_run)


@cli.resultcallback()
def exit_with_code(result, *args, **kwargs):
    exit_code = result
    sys.exit(exit_code)


@cli.command()
@click.pass_obj
@handle_exit
def status(controller):

    status = controller.status()
    utils.print_status(status)


@cli.group()
def scale():
    pass


@scale.command()
@click.argument('num_workers', type=int)
@click.pass_obj
@handle_exit
def to(controller, num_workers):

    controller.scale_to(num_workers)


@scale.command()
@click.argument('num_workers', type=int, default=1)
@click.pass_obj
@handle_exit
def up(controller, num_workers):

    controller.scale('up', num_workers)


@scale.command()
@click.argument('num_workers', type=int, default=1)
@click.pass_obj
@handle_exit
def down(controller, num_workers):

    controller.scale('down', num_workers)


@scale.command()
@click.pass_obj
@handle_exit
def auto(controller):

    controller.scale('auto')


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
