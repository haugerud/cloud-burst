#!/usr/bin/env python
import syslog, subprocess, time, os

def handle_error(e):
    write_to_syslog('error', e)
    print "\n" + str(e)
    exit(1)

def exit_script(signum, frame):
    print ""
    print "Exiting the script..."
    exit(0)

def write_to_syslog(log_level, message):
    if log_level == 'info':
        syslog.syslog(syslog.LOG_INFO, str(message))
    elif log_level == 'error':
        syslog.syslog(syslog.LOG_INFO, str(message))

def print_verbose(message):
    global verbose
    if verbose:
        print message

    logging.info(message)
    write_to_syslog('info', message)

def run_command(command):
    output = subprocess.Popen(command.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    return output.stdout.read().strip()

def check_path_exists(path):
    if not os.path.exists(path):
        return False

    return True

def check_file_exists(path):
    if not os.path.isfile(path):
        return False

    return True

def save_to_file(data, path, filename):
    if path == False:
        path = os.path.dirname(os.path.abspath(__file__))

    filename = "%s/%s" % (path, filename)
    f = open(filename , "a")
    f.write(data)
    f.close()

def run_piped_command(commands_as_nested_list):
    try:
        process_dict = []
        for command_group in commands_as_nested_list:
            i = commands_as_nested_list.index(command_group)
            command = command_group

            if i == 0: # First time
                process_dict.append(subprocess.Popen(command, stdout=subprocess.PIPE))
            elif i == len(commands_as_nested_list)-2: # Last time
                process_dict.append(subprocess.Popen(command, stdin=process_dict[i-1].stdout, stdout=subprocess.PIPE))
            else: # In the middle
                process_dict.append(subprocess.Popen(command, stdin=process_dict[i-1].stdout, stdout=subprocess.PIPE))

        return process_dict[-1].communicate()[0]
    except Exception as e:
        handle_error(e)

def sleep(start_timestamp, duration):
    loop_time = time.time() - start_timestamp
    sleep_time = duration - loop_time
    print_verbose("The script used " + str(loop_time) + " seconds this loop")
    if sleep_time <= 0:
        print_verbose("Time used >= the set interval. Skipping sleep.")
        return 0
    else:
        print_verbose("Sleeping additional " + str(sleep_time) + " seconds")
        time.sleep(sleep_time)
        return 0
