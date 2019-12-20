#!/usr/bin/env python

# Author: Razikh Ulla (razikh@gmail.com)
# (c) 2019


import argparse
import boto3
import sys
import re
import os
import time
import json
import logging
import textwrap
from datetime import datetime
from collections import defaultdict

config_defaults = defaultdict(lambda: None, {
    'aws_profile_name': 'default',
    'ec2_region_name': '',
    'tag_name': 'MakeSnapshot', #Edit this to name as per your requirement
    'tag_value': 'true',
    'tag_type': 'volume', #Snapshots are taken of Volume. These snapshots can be used to create image followed by Instances
    'keep_hour': 4, #Number of hourly snapshots to be maintained on the account
    'keep_day': 3,  #Number of daily snapshots to be maintained on the account
    'keep_week': 4, #Number of weekly snapshots to be maintained on the account
    'keep_month': 3, #Number of monthly snapshots to be maintained on the account
    'keep_year': 10, #Number of yearly snapshots to be maintained on the account
    'skip_create': False, #Toggle to True to skip snapshot creation
    'skip_delete': False, #Toggle to True to skip snapshot deletion
    'log_file': '',
})

now_format = {
    'hour': '%R',
    'day': '%a',
    'week': '%U',
    'month': '%b',
    'year': '%Y'
}

log = logging.getLogger('makesnap3')


def dump_stats(stats):
    total = stats['total_errors'] + stats['snap_errors']
    if total > 0:
        exitcode = 3
        logstats = log.error
        subj = 'Error making snapshots'
    else:
        exitcode = 0
        logstats = log.info
        subj = 'Completed making snapshots'

    stat = ['']
    stat.append("Finished making snapshots at {} for {} volume(s), {} errors".format(
        datetime.today().strftime('%d-%m-%Y %H:%M:%S'), stats['total_vols'], total))
    stat.append("Created: {}, deleted: {}, errors: {}".format(
        stats['snap_creates'], stats['snap_deletes'], stats['snap_errors']))
    for s in stat:
        logstats(s)

    return exitcode


def read_config(defaults):
    new = defaults.copy()
    return new


def get_vols(ec2_resource, tag_name, tag_value, tag_type):
    log.debug("looking for tags of type %s " % tag_type)
    if tag_type == 'volume':
        vols = ec2_resource.volumes.filter(
            Filters=[{'Name': 'tag:' + tag_name, 'Values': [tag_value]}]).all()
        print vols
        return vols
    elif tag_type == 'instance':
        #instance_filters = [{'Name': 'tag:' + tag_name, 'Values': [tag_value]}]
        instance_filters = [{'Name': 'tag:' + tag_name, 'Values': [tag_value]}]
        print "Entering the block"
       
        instances = ec2_resource.instances.filter(
            Filters=instance_filters).all()
        print "print instances"
        print instances
        instance_ids = []
        for instance in instances:
            print "for loop"
            instance_ids.append(instance.id)
            print instance_id
        vols = ec2_resource.volumes.filter(Filters=[
            {'Name': 'attachment.instance-id', 'Values': instance_ids}
        ]).all()
        
        return vols
    else:
        # reserved for new tag types
        pass


def log_setup(logfile=None):
    """Setup console logging by default
    if logfile is defined, log there too
    """

    if logfile:
        fh = logging.FileHandler(logfile)
        fhf = logging.Formatter(
            '%(asctime)s %(name)s: %(levelname)s %(message)s')
        fh.setFormatter(fhf)
        log.addHandler(fh)
    else:
        log.addHandler(logging.StreamHandler())
        log.setLevel(logging.INFO)


def calc_rotate(config, snaplist, period):
    """ Create a list of snapshots to delete in this <period> run
    """
    candidates = []
    for snap in snaplist:
        if re.findall("^(hour|day|week|month|year)_snapshot", snap.description) == [period]:
            candidates.append(snap)
            log.debug("     Added to candidate list: %s '%s'",
                      snap.id, snap.description)
            print snaplist
        else:
            log.debug("     Skipped, not adding: %s '%s'",
                      snap.id, snap.description)
    candidates.sort(key=lambda x: x.start_time)

    deletelist = []
    for i in range(len(candidates) - config['keep_' + period]):
        deletelist.append(candidates[i])

    return deletelist


def main(period):
    log_setup()
    config = read_config(config_defaults)

    if config['log_file']:
        log_setup(config['log_file'])

    # Set profile name only if it's explicitly defined if config file
    # otherwise it messes with the boto's order of credentials search
    # (environment is not checked)
    if config.get('aws_profile_name'):
        boto3.setup_default_session(profile_name=(
            config['aws_profile_name'] or 'default'))

    stats = {
        'total_vols': 0,
        'total_errors': 0,
        'snap_deletes': 0,
        'snap_creates': 0,
        'snap_errors': 0,
    }

    date_suffix = datetime.today().strftime(now_format[period])
    log.info("Started taking %ss snapshots at %s", period,
             datetime.today().strftime('%d-%m-%Y %H:%M:%S'))

    # 'None' resorts to boto default region
    ec2_region = config['ec2_region_name'] or None
    try:
        ec2 = boto3.resource('ec2', region_name=ec2_region)
        vols = get_vols(ec2_resource=ec2, tag_name=config['tag_name'], tag_value=config[
                        'tag_value'], tag_type=config['tag_type'])
        print "calling get vols"
        print vols
        for vol in vols:
            print "Inside for loop"
            log.info("Processing volume %s:", vol.id)
            stats['total_vols'] += 1
            description = '%(period)s_snapshot %(vol_id)s_%(period)s_%(date_suffix)s by snapshot script at %(date)s' % {
                'period': period,
                'vol_id': vol.id,
                'date_suffix': date_suffix,
                'date': datetime.today().strftime('%d-%m-%Y %H:%M:%S')
            }

            if not config['skip_create']:
                try:
                    log.info(
                        ">> Creating snapshot for volume %s: '%s'", vol.id, description)
                    current_snap = vol.create_snapshot(Description=description)
                    current_snap.create_tags(Tags=vol.tags)
                    stats['snap_creates'] += 1
                except Exception as err:
                    stats['snap_errors'] += 1
                    log.error("Unexpected error making snapshot:" +
                              str(sys.exc_info()[0]))
                    log.error(err)
                    pass

            if not config['skip_delete']:
                for del_snap in calc_rotate(config, vol.snapshots.all(), period):
                    log.info(">> Deleting snapshot %s", del_snap.description)
                    try:
                        del_snap.delete()
                        stats['snap_deletes'] += 1
                    except Exception as err:
                        stats['snap_errors'] += 1
                        log.error(
                            "Unexpected error deleting snapshot:" + str(sys.exc_info()[0]))
                        log.error(err)
                        pass

            time.sleep(3)

    except Exception as err:
        stats['total_errors'] += 1
        log.critical("Can't access volume list:" + str(sys.exc_info()[0]))
        log.critical(err)

    return dump_stats(stats)


if __name__ == '__main__':
    # period = sys.argv[1]

    # Command Line Args
    arg_parser = argparse.ArgumentParser(description=textwrap.dedent('''\
        The values of hour, day, week, month & year are considered periods for taking snapshots.
        hour: This takes snapshot of the current hour.
        day: This takes snapshot of the current day.
        week:  This takes snapshot of the current week.
        month: This takes snapshot of the current month.
        year:  This takes snapshot of the current year.
        
        Pass one of these values at the commandline. 
        Eg: python Snapshots.py day 
         '''),
        formatter_class=argparse.RawTextHelpFormatter)

    arg_parser.add_argument('period', choices=['hour', 'day', 'week', 'month', 'year'])
    args = arg_parser.parse_args()

    #config_file = str(args.config)
    period = str(args.period)

    sys.exit(main(period))
