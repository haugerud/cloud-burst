#!/usr/bin/env python
import basics
import boto3
import yaml
import syslog
import getopt
import signal
import sys
import time
import datetime
import dateutil
import json
import urllib2
from math import ceil,floor

def print_usage():
    print "Automated cloud bursting script\n"
    print "usage: " + __file__ + " [arguments]\n"
    print "Arguments:"
    print "   --help\t\t\t Prints this help message"
    print "   -v [--verbose]\t\t Verbose output"
    print "   -c [--config]\t\t Specify another config file"

def get_params():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vc:", ["verbose","help","config="])
    except getopt.GetoptError as e:
        basics.handle_error(e)

    return opts, args

def set_options(opts):
    global verbose
    global config_path
    verbose = False
    config_path = False

    for o,p in opts:
        if o in ["-v", "--verbose"]:
            verbose = True
        elif o in ["--help"]:
            print_usage()
            exit()
        elif o in ["-c", "--config"]:
            config_path = p

def print_verbose(message):
    global verbose
    if verbose:
        print message

def sleep(start_timestamp, duration):
    loop_time = time.time() - start_timestamp
    sleep_time = duration - loop_time
    print_verbose("The script used " + str(loop_time) + " seconds this loop")
    if sleep_time <= 0:
        print_verbose("Time used >= the set interval. Skipping sleep.")
        sys.stdout.flush()
    else:
        print_verbose("Sleeping additional " + str(sleep_time) + " seconds")
        sys.stdout.flush()
        time.sleep(sleep_time)

def import_config():
    global config_path
    if config_path == False:
        config_path = 'config.yml'

    if not basics.check_file_exists(config_path):
        print_verbose('Attempted to find config file: %s' % config_path)
        basics.handle_error('No configuration file found')

    try:
        with open(config_path, 'r') as configfile:
            config = yaml.load(configfile)
    except StandardError as e:
        print_verbose(e)
        basics.handle_error('Error when attempting to read the configuration file')

    return config

def purge_old_spot_requests(ec2client,cur_spot_requests,timeout,max_bid):
    now_time = datetime.datetime.utcnow()
    now_time = now_time.replace(tzinfo=dateutil.tz.tzutc())

    for request in cur_spot_requests:
        if request[u'State'] == 'open':
            #print now_time - request[u'CreateTime']
            if (int(now_time.strftime('%s')) - int(request[u'CreateTime'].strftime('%s')) > timeout and
                not request[u'SpotPrice'] == max_bid):
                try:
                    response = ec2client.cancel_spot_instance_requests(SpotInstanceRequestIds=[request[u'SpotInstanceRequestId']])
                except Exception as e:
                    print_verbose(e)
                    basics.handle_error('Some error ocurred. Could not cancel old spot instance request.')


def get_current_spot_slaves(ec2resource):
    spot_slaves = []
    filter = [{
                'Name': 'instance-lifecycle',
                'Values': ['spot'],
              },
              {
                'Name': 'instance-state-name',
                'Values': ['pending','running','rebooting']
              },
             ]

    for instance in ec2resource.instances.filter(Filters=filter):
        spot_slaves.append(instance)

    return spot_slaves,ec2resource.instances.filter(Filter=filter)

def get_current_spot_requests(ec2client,states):
    spot_requests = []

    data = ec2client.describe_spot_instance_requests()['SpotInstanceRequests']
    if states == 'all':
        return data

    for request in data:
        if request['State'] in states:
            spot_requests.append(request['SpotInstanceRequestId'])

    return spot_requests

def import_launch_config():
    config_path = 'launch_config.yml'
    if not basics.check_file_exists(config_path):
        print_verbose('Attempted to find config file: %s' % config_path)
        basics.handle_error('No configuration file found')

    try:
        with open(config_path, 'r') as configfile:
            config = yaml.load(configfile)
    except StandardError as e:
        print_verbose(e)
        basics.handle_error('Error when attempting to read the configuration file')

    return config

def request_spot_instances(ec2client,num_to_boot,instance_type,max_bid):
    launch_config = import_launch_config()

    try:
        response = ec2client.request_spot_instances(SpotPrice=str(max_bid),
                                                    InstanceCount=num_to_boot,
                                                    LaunchSpecification=launch_config)
    except Exception as e:
        print_verbose(e)
        basics.handle_error('Requesting spot instances failed')

def cancel_spot_requests(ec2client,spot_requests,num_to_cancel):
    try:
        response = ec2client.cancel_spot_instance_requests(SpotInstanceRequestIds=spot_requests[-num_to_cancel:])
    except Exception as e:
        print_verbose(e)
        basics.handle_error('Termination of spot requests failed')

def terminate_spot_instances(ec2client,spot_instances,raw_spot_info,num_to_terminate,partial_hour_limit):
    try:
        list_with_timestamp = dict()
        now_time = datetime.datetime.utcnow()
        now_time = now_time.replace(tzinfo=dateutil.tz.tzutc())
        terminated = 0

        for instance in spot_instances:
            list_with_timestamp[instance.instance_id] = instance.launch_time.strftime('%s')

        for instance in sorted(list_with_timestamp, key=list_with_timestamp.get):
            if terminated == num_to_terminate:
                break

            instance_lifetime_delta = int(now_time.strftime('%s')) - int(list_with_timestamp[instance])

            # Calculate minutes in a partial hour used
            if instance_lifetime_delta < partial_hour_limit:
                part_seconds = instance_lifetime_delta
            else:
                part_seconds = instance_lifetime_delta - (floor(instance_lifetime_delta/3600)*partial_hour_limit)

            if part_seconds > partial_hour_limit:
                print_verbose('Terminating %s...' % instance)
                response = ec2client.terminate_instances(InstanceIds=[instance])
            else:
                print_verbose('%s has not reached the set partial hour limit. %.0f minutes has passed.' % (instance,part_seconds/60))

            terminated += 1

    except Exception as e:
        print_verbose(e)
        basics.handle_error('Termination of spot requests failed')

def fetch_current_mesos_master(mesos_zkurl):
    print_verbose('Resolving ZooKeeper url for the working Mesos Master')

    #return '52.17.132.212:5050'

    try:
        mesos_master = basics.run_command('mesos-resolve %s' % mesos_zkurl)
    except Exception as e:
        print_verbose(e)
        basics.handle_error('Could not resolve the ZooKeeper url for the leading master node.')

    return mesos_master

def fetch_and_parse_json(request):
    try:
        print_verbose('Fetching %s' % request)
        json_data = urllib2.urlopen(request).read()
        parsed_data = json.loads(json_data)
    except Exception as e:
        print_verbose(e)
        basics.handle_error('Failed JSON fetch and parse.')

    return parsed_data

def get_scaling_decision(resources_in_use,current_percent_in_use,active,pending,config):

    # Set some settings from the configration
    burst_point = config['burst_point_percentage']
    max_slaves = config['maximum_spot_slaves']

    baseline_cpus = config['baseline_cpus']
    baseline_mem = config['baseline_mem']
    baseline_disk = config['baseline_disk']

    instance_cpus = config['instance_cpus']
    instance_mem = config['instance_mem']
    instance_disk = config['instance_disk']

    pending_active_slaves = active + pending

    # Calculate pending resources
    pending_active_spot_resources = {'cpus': pending_active_slaves * instance_cpus,
                                     'mem': pending_active_slaves * instance_mem,
                                     'disk': pending_active_slaves * instance_disk}

    # Total of pending, active and baseline resources
    total_resources_apb = {'cpus': baseline_cpus + pending_active_spot_resources['cpus'],
                           'mem': baseline_mem + pending_active_spot_resources['mem'],
                           'disk': baseline_disk + pending_active_spot_resources['disk']}

    # Calculate usage percentage if the pending resources would have been available
    pending_usage_percent = {'cpus': resources_in_use['cpus']/total_resources_apb['cpus'],
                             'mem': resources_in_use['mem']/total_resources_apb['mem'],
                             'disk': resources_in_use['disk']/total_resources_apb['disk']}

    print_verbose('   |------------------------------------------')
    print_verbose('   |       Burst point value set to %.2f      ' % burst_point)
    print_verbose('   |------------------------------------------')
    print_verbose('   |------------------------------------------')
    print_verbose('   | Resource usage: |  Percent\t | Count      ')
    print_verbose('   |------------------------------------------')
    print_verbose('   | CPUs            |  %.2f%%\t | %.2f       ' % (current_percent_in_use['cpus']*100,resources_in_use['cpus']))
    print_verbose('   | Memory          |  %.2f%%\t | %i MB      ' % (current_percent_in_use['mem']*100,resources_in_use['mem']))
    print_verbose('   | Disk            |  %.2f%%\t | %i MB      ' % (current_percent_in_use['disk']*100,resources_in_use['disk']))
    print_verbose('   |------------------------------------------')


    # Scale up if the burst point is lower than the resources used
    if any(pending_usage_percent[i] >= burst_point for i in pending_usage_percent):
        if pending_active_slaves + 1 > max_slaves:
            print_verbose('The specified limit for max number of slaves has been hit. Will not scale up.')
            return 0
        else:
            return 1

    # Stop evaluating if the number of slaves is zero
    if pending_active_slaves == 0:
        print_verbose('')
        return 0

    # Calculate the number of slaves that can potentially be terminated
    scale_down_num = 0
    while pending_active_slaves >= 0:

        ## Calculate the usage percent if we remove scale_down_num slave
        x_less_usage = {'cpus': (resources_in_use['cpus'])/(total_resources_apb['cpus'] - scale_down_num*instance_cpus),
                        'mem': (resources_in_use['mem'])/(total_resources_apb['mem'] - scale_down_num*instance_mem),
                        'disk': (resources_in_use['disk'])/(total_resources_apb['disk'] - scale_down_num*instance_disk)}

        # Set any negative values to zero
        for i in x_less_usage:
            if x_less_usage[i] < 0:
                x_less_usage[i] = 0.0

        # Check if the usage exceeds the burst point
        if not any(x_less_usage[i] >= burst_point for i in x_less_usage):
            scale_down_num += 1
            pending_active_slaves -= 1
            continue

        break

    # Subtract 1 from scale_down_num, otherwise the script will scale up and down
    # every minute. This will ensure that the number stays right above the the burst point
    scale_down_num -= 1

    if scale_down_num > 0:
        return - scale_down_num

    # No change
    return 0

def fetch_current_price(ec2client,avail_zone,instance_type,max_limit):

    # Fetch the most recent price entry for the set zone and instance type
    price_entry = ec2client.describe_spot_price_history(InstanceTypes=[instance_type],
                                                          AvailabilityZone=avail_zone,
                                                          ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
                                                          MaxResults=1)

    bid = float(price_entry['SpotPriceHistory'][0][u'SpotPrice']) + 0.001


    print_verbose('   |--------------------------------')
    print_verbose('   | Curent market price:    %.3f   ' % bid)
    print_verbose('   | Maximum bid limit       %.3f   ' % max_limit)

    final_bid = bid

    if bid > max_limit:
        final_bid = max_limit


    print_verbose('   | Our bid                 %.3f   ' % final_bid)
    print_verbose('   |--------------------------------')

    return final_bid

def main():
    # Get paramaters and process them
    opts, args = get_params()
    set_options(opts)

    # Import the configuration and set some session settings
    config = import_config()

    try:
        session = boto3.session.Session(aws_access_key_id=config['aws_access_key_id'],
                                        aws_secret_access_key=config['aws_secret_access_key'],
                                        region_name=config['default_region'])
    except Exception as e:
        print_verbose(e)
        basics.handle_error('Could not establish a session towards AWS API, check config')

    # Start EC2 and resource and client session
    try:
        ec2resource = session.resource('ec2')
        ec2client = session.client('ec2')
    except Exception as e:
        print_verbose(e)
        basics.handle_error('Could not establish a session towards EC2.')

    # Main execution
    while True:
        start_time = time.time()
        print ''

        #################################
        ### Collect hybrid cloud metrics
        #################################

        ## Fetch current Mesos master
        mesos_master = fetch_current_mesos_master(config['mesos_zkurl'])

        ## Create a Marathon url
        marathon_url = '%s:%i' % (mesos_master[:-5], config['marathon_port'])
        print_verbose('   Current Mesos master %s' % mesos_master[:-5])

        ## Collect Mesos metrics
        mesos_data = fetch_and_parse_json('http://%s/metrics/snapshot' % mesos_master)

        ## Collect the usage values of the resources
        resources_in_use = {'cpus': float(mesos_data[u'master/cpus_used']),
                            'mem': float(mesos_data[u'master/mem_used']),
                            'disk': float(mesos_data[u'master/disk_used'])}
        current_percent_in_use = {'cpus': float(mesos_data[u'master/cpus_percent']),
                                  'mem': float(mesos_data[u'master/mem_percent']),
                                  'disk': float(mesos_data[u'master/disk_percent'])}

        ### Collect EC2 metrics
        cur_slaves,cur_slaves_raw = get_current_spot_slaves(ec2resource)
        cur_spot_requests = get_current_spot_requests(ec2client,'all')
        cur_open_spot_requests = get_current_spot_requests(ec2client,[u'open'])
        num_active_pending_slaves = len(cur_slaves) + len(cur_open_spot_requests)

        #################################
        ### Calculate the bidding price
        #################################

        # Fetch the current price
        bid = fetch_current_price(ec2client,
                                  config['availability_zone'],
                                  config['instance_type'],
                                  config['maximum_bid_limit']
                                  )

        ########################################################
        ### Make a descision of whether or not to cloud burst
        ########################################################
        slaves_to_adjust = get_scaling_decision(resources_in_use,
                                                current_percent_in_use,
                                                len(cur_slaves),
                                                len(cur_open_spot_requests),
                                                config)

        desired_slaves = len(cur_open_spot_requests) + len(cur_slaves) + slaves_to_adjust

        print_verbose('   |----------------------------')
        print_verbose('   | Number of:         | Count ')
        print_verbose('   |----------------------------')
        print_verbose('   | Desired instances  |   %i  ' % desired_slaves)
        print_verbose('   | Pending requests   |   %i  ' % len(cur_open_spot_requests))
        print_verbose('   | Active instances   |   %i  ' % len(cur_slaves))
        print_verbose('   |----------------------------')

        ############################
        ### Execute the descision
        ############################

        # Remove spot requests that exceeded the timeout and that does bid at max limit
        purge_old_spot_requests(ec2client,
                                cur_spot_requests,
                                config['spot_request_timeout'],
                                config['maximum_bid_limit'])

        ## The number of pending and active instances are ok
        if desired_slaves == num_active_pending_slaves:
            print_verbose('The number of pending and active slaves are ok')
            try:
                sleep(start_time,config['execution_interval'])
            except KeyError as e:
                basics.handle_error('%s has not been set in the config.' % e)

            continue

        ## Request new spot instances
        if desired_slaves > num_active_pending_slaves:
            print_verbose('Not enough pending or active slave nodes. Requesting new ones')
            request_spot_instances(ec2client,
                                   desired_slaves-num_active_pending_slaves,
                                   config['instance_typed'],
                                   bid)

        ## Terminate excessive pending spot requests
        if (num_active_pending_slaves > desired_slaves
            and not len(cur_open_spot_requests) == 0):
            excessive_slaves = num_active_pending_slaves - desired_slaves

            if excessive_slaves < len(cur_open_spot_requests):
                print_verbose('Excessive spot requests. Attempting to cancel %i' % excessive_slaves)
                cancel_spot_requests(ec2client,cur_open_spot_requests,excessive_slaves)
                num_active_pending_slaves = num_active_pending_slaves - excessive_slaves
            else:
                print_verbose('Excessive spot requests. Attempting to cancel %i' % len(cur_open_spot_requests))
                cancel_spot_requests(ec2client,cur_open_spot_requests,len(cur_open_spot_requests))
                num_active_pending_slaves = num_active_pending_slaves - len(cur_open_spot_requests)

        ## Terminate excessive spot instances
        if (num_active_pending_slaves > desired_slaves):
            excessive_slaves = num_active_pending_slaves - desired_slaves
            print_verbose('Excessive spot instances. Attempting to terminate %i' % excessive_slaves)

            terminate_spot_instances(ec2client,cur_slaves,cur_slaves_raw,excessive_slaves,config['partial_hour_limit'])


        ### Sleep and repeat
        try:
            sleep(start_time,config['execution_interval'])
        except KeyError as e:
            basics.handle_error('%s has not been set in the config.' % e)

if __name__ == '__main__':
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, basics.exit_script)
    main()
