#!/usr/bin/env python

import os
import sys
import json
import boto3
import click
import dotenv
import unipath
import logging
from functools import wraps
from os import getenv as env
from tabulate import tabulate
from click.exceptions import UsageError

import moscaler
from moscaler.opsworks import OpsworksController
from moscaler.exceptions import OpsworksControllerException

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
            return 1
#            return str(exc)
    return exit_wrapper


def log_before_after_stats(cmd):
    @wraps(cmd)
    def wrapped(controller, *args, **kwargs):
        status = controller.status()
        LOGGER.info('Cluster status: %s',
                    status_summary(status), extra=status)
        result = cmd(controller, *args, **kwargs)
        actions = controller.actions()
        LOGGER.info('Action summary: %s',
                    action_summary(actions), extra=actions)
        return result
    return wrapped


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

    init_logging(cluster, debug)

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
@click.option('-f','--format', default='table')
@click.pass_obj
@handle_exit
def status(controller, format):

    status = controller.status()
    print_status(status, format=format)


@cli.group()
@click.pass_obj
def scale(controller):
    pass


@scale.command()
@click.argument('num_workers', type=int)
@click.option('--scale-available', is_flag=True, default=False)
@click.pass_obj
@handle_exit
@log_before_after_stats
def to(controller, num_workers, scale_available):

    controller.scale_to(num_workers, scale_available)


@scale.command()
@click.argument('num_workers', type=int, default=1)
@click.option('--scale-available', is_flag=True, default=False)
@click.pass_obj
@handle_exit
@log_before_after_stats
def up(controller, num_workers, scale_available):

    controller.scale('up', num_workers, scale_available)


@scale.command()
@click.argument('num_workers', type=int, default=1)
@click.pass_obj
@handle_exit
@log_before_after_stats
def down(controller, num_workers):

    controller.scale('down', num_workers)


@scale.command()
@click.option('-c', '--config', envvar='AUTOSCALE_CONFIG',
              help='json string or path to json file containing autoscale configuration')
@click.pass_obj
@handle_exit
@log_before_after_stats
def auto(controller, config):

    if config is None:
        raise click.ClickException("No autoscale config provided")

    try:
        if os.path.isfile(config):
            with open(config, 'r') as f:
                config = json.load(f)
        else:
            config = json.loads(config)
    except Exception, e:
        raise click.BadParameter("Failed to parse autoscale config: %s" % str(e))

    controller.autoscale(config)


def init_logging(cluster, debug):
    import logging.config

    if debug:
        level = logging.getLevelName(logging.DEBUG)
        format = "[%(levelname)s] [" \
                 + cluster \
                 + "] [%(module)s:%(funcName)s:%(lineno)d] %(message)s"
    else:
        level = logging.getLevelName(logging.INFO)
        format = "[%(levelname)s] [" \
                 + cluster \
                 + "] %(message)s"

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
                'format': format
            }
        }
    }

    if env('LOGGLY_TOKEN'):
        config['loggers']['moscaler']['handlers'].append('loggly')
        config['handlers']['loggly'] = {
            'class': 'pyloggly.LogglyHandler',
            'level': level,
            'token': env('LOGGLY_TOKEN'),
            'host': 'logs-01.loggly.com',
            'tags': 'mo-scaler,%s' % cluster.replace(' ', '-')
        }

    logging.config.dictConfig(config)


def status_summary(status):

    return ", ".join([
        "workers: %d" % status['workers'],
        "online workers: %d" % status['workers_online'],
        "queued high load jobs: %d" % status['job_status']['queued_jobs_high_load'],
        "running jobs: %d" % status['job_status']['running_jobs']
    ])


def action_summary(actions):
    return "stopped: %d, started: %d" % \
           (actions['total_stopped'], actions['total_started'])


def print_status(status, format='table'):
    if format == 'json':
        print json.dumps(status, indent=2)
    elif format == 'table':
        cluster = [
            ['Name', status['cluster']],
            ['Workers', status['workers']],
            ['Workers Online', status['workers_online']],
            ['Workers Pending', status['workers_pending']],
            ['MH Online', status['matterhorn_online']],
            ['Running Jobs', status['job_status']['running_jobs']],
            ['Queued High Load Jobs', status['job_status']['queued_jobs_high_load']],
        ]
        print tabulate(cluster)
        instance_headers = [
            'Opsworks Id',
            'Ec2 Id',
            'State',
            'Hostname',
            'Uptime',
            'Billed Minutes',
            'Idle',
            'Maintenance',
            'Registered',
            'MH Host Url',
        ]
        instances = [
            [
                x['opsworks_id'],
                x['ec2_id'],
                x['state'],
                x['hostname'],
                x['uptime'],
                x['billed_minutes'],
                x['idle'],
                x['maintenance'],
                x['registered'],
                x['mh_host_url']
            ] for x in status['worker_details']
        ]
        print tabulate(instances, headers=instance_headers)


if __name__ == "__main__":
    cli()
