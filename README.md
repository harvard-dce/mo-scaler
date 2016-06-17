
[![Build Status](https://travis-ci.org/harvard-dce/mo-scaler.svg)](https://travis-ci.org/harvard-dce/mo-scaler)
[![Code Health](https://landscape.io/github/harvard-dce/mo-scaler/master/landscape.svg?style=flat)](https://landscape.io/github/harvard-dce/mo-scaler/master)

# mo-scaler

Matterhorn + Opsworks + Horizontal Scaling == MOrizontal Scaling!

## Getting started

1. Git clone this repo and cd into it.
1. Create a virtual environement: `virtualenv venv && source venv/bin/activate` (optional but recommended)
1. Install the requirements: `pip install -r requirements.txt`
1. Create a `.env` file: `cp .env.dist .env`. See **Settings** below for required values.

## Usage

    Usage: manager.py [OPTIONS] COMMAND [ARGS]...

    Options:
      -c, --cluster TEXT  opsworks cluster name
      -p, --profile TEXT  set/override default aws profile
      -d, --debug         enable debug output
      -f, --force
      -n, --dry-run
      --version           Show the version and exit.
      --help              Show this message and exit.

    Commands:
      scale
      status

### General Options

* *-c/--cluster* - name of the opsworks cluster to operate on
* *-d/--debug* - adds more detailed logging output
* *-n/--dry-run* - the script will go through the motions but not actually change anything
* *-f/--force* - ignore some condition guards (see more below)
* *-p/--profile* - use a specific AWS credentials profile (overrides $AWS_PROFILE)

## Settings & the .env file

Some options can/must be controlled via environment variables. Copy the provided `.env.dist` file to `.env` and edit.

### Required

The user/pass combo for the matterhorn system account.

* `MATTERHORN_USER` 
* `MATTERHORN_PASS`

### Optional

* `CLUSTER_NAME` - Name of the opsworks cluster to operate on
* `AWS_PROFILE` - Use a specific AWS credentials profile. Note that this will not override an existing `$AWS_PROFILE`  in your environment.
* `LOGGLY_TOKEN` - send log events to loggly
* `AWS_DEFAULT_PROFILE` - this should only be necessary in an environment relying on AWS instance profile authentication

### Other settings of note

* `MOSCALER_MIN_WORKERS` - minimum number of worker nodes to employ
* `MOSCALER_IDLE_UPTIME_THRESHOLD` - minutes of its billing hour that an instance must be up before it is considered for reaping

See below for additional settings related to autoscaling.

## Commands

### status

`./manager.py status`

Get a json dump of the cluster's status summary. 

### scale

Command group with the following subcommands:

#### up

Scale up one instance:
`./manager.py scale up`

Scale up *x* instances:
`./managerpy scale up 3`

#### down

Scale down one instance:
`./manager scale down`

Scale down *x* instances:
`./manager.py scale down 3`

#### to

Scale up/down to a specific number of instances
`./manager scale to 8`

#### auto

Scale up/down some number of instances using the logic of one of the "pluggable"
autoscaling mechanisms (see below).

`./manager scale auto`

### --force option

In the case of the `--force` option has the following effects:

* The `MOSCALER_MIN_WORKERS` will be ignored.
* Idle state of workers will be ignored
* uptime/billing hour usage of workers will be ignored

### --dry-run option

If `--dry-run` is enabled the process will run through and report what it would potentially do, but no instances will be started, stopped or taken in/out of maintenance.

### --scale-available option

Tells the `scale up` and `scale to` commands to ignore "not enough worker" conditions and just scale up whatever is available.

**Example**: a cluster has 10 total workers, 2 online, 1 in a failed setup state, and 7 offline. By default, if asked to
`scale to 10` the mo-scaler process will observe that it only has 7 available workers to spin up (i.e., there's no way to
get to 10 workers,) and exit with a complaint about the cluster not having enough workers available. 
With the `--scale-available` flag the process will only warn about not having enough workers and spin up the 7 available.

### Autoscaling

The `scale auto` command executes one of a set of configurable scaling mechanism. 
The currently implemented options are defined via classes in `moscaler/autoscalers.py`.o

* `LayerLoad`
* `LayerLoadPlusOnlineWorkers`
* `HighLoadJobs`

#### LayerLoad

`LayerLoad` uses the value of an Opsworks layer's cloudwatch metric to decide when to scale up/down.

#### LayerLoadPlusOnlineWorkers

`LayerLoadPlusOnlineWorkers` is a slight variation on `LayerLoad` that takes into
account the number of existing online workers when deciding to scale up.

#### HighLoadJobs

`HighLoadJobs` uses data from the Matterhorn statistics API. Note that `HighLoadJobs` is problematic
due to the less-than-desirable behavior of Matterhorn's job dispatching logic.

#### Environment variable settings

These are the settings that apply to both implementations:

* `AUTOSCALE_TYPE`: Selects which method to use: one of either "LayerLoad" or "HighLoadJobs"

* `AUTOSCALE_UP_THRESHOLD`: Value that the scaling mechanism should use when determining
  whether to start more workers.

* `AUTOSCALE_DOWN_THRESHOLD`: Value that the scaling mechanism should use when determining
  whether to stop workers.

* `AUTOSCALE_INCREMENT`: How many workers to start per scale up event

* `AUTOSCALE_DOWN_INCREMENT`: How many workers to stop per scale down event

* `AUTOSCALE_PAUSE_INTERVAL`: How long (in seconds) after a successful scale up
  (or down, conceivably) event to block additional scale up (or down) events in order to
  allow the starting (or stopping) of workers to influence the overall workload
  of the cluster. **Note**: the `LayerLoad` autoscaler currently only pauses
  when scaling up.

Specific to the `LayerLoad` mechanism:

* `AUTOSCALE_LAYERLOAD_METRIC`: name of the cloudwatch metric that should be queried
  when determining whether to start/stop workers
  
* `AUTOSCALE_LAYERLOAD_LAYER_ID`: id of the Opsworks layer for which the
  metric should be queried

* `AUTOSCALE_LAYERLOAD_SAMPLE_COUNT`: number of metric datapoints to sample.

* `AUTOSCALE_LAYERLOAD_SAMPLE_PERIOD`: granularity of the datapoints.

For some context on the LayerLoad settings it would probably be helpful to review
the docs for the boto3 CloudWatch client's `get_metric_statistics` method, which is
what these config values eventually get passed to.

#### Billing considerations

When an ec2 instance is started it is billed for 1 hour of usage, regardless of how
much of that hour it is actually running. Therefore, it is potentially costly to
be "flapping" instances up and down on a minute-by-minute basis. To avoid this, 
once the `scale auto` command has identified idle workers, it then looks at how 
long the instances have been "up" and rejects those whose uptime calculations 
indicate the instance has not used the bulk of it's billed hour. For example, 
if an instance is seen to have only been up for 40 minutes (or 1:40, 3:33, etc.) 
it will not be shut down even if idle. If it has been up for 53 minutes (or 1:53, 
5:53, etc.) it will be shut down. The threshold of what constitutes "the bulk 
of it's billed hour" is 50 by default but can be overridden by setting 
`$IDLE_UPTIME_THRESHOLD`.

## Logging

All log output is directed to stdout with warnings and errors also going
to stderr.

If a `$LOGGLY_TOKEN` env value is available the program will
add an additional log output handler to send events to loggly. 
Events will can be identified by both the 'mo-scaler' tag and
a tag corresponding to the cluster prefix.

### Before/After status events

Prior to command being executed a log event
will be emitted containing a summary of the cluster status, including
number of instances, workers, workers online, etc.

Just after the command is executed a final log event will be emitted 
summarizing the actions taken (instances stopped/started)

### Cluster naming conventions / assumptions

* instances are identified using their `Hostname` value
* A cluster will have one admin instance with a hostname beginning with "admin"
* Worker node hostnames will begin with "worker"
* Engage node hostnames will begin with "engage"

