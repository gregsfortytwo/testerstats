#!/usr/bin/python
import argparse
import json
from collections import defaultdict
import sys

def parse_args():
    parser = argparse.ArgumentParser(description='Read JSON lab data and generate statistics.')
    parser.add_argument(
        '--json-file', dest='json_file_string',
        required=True,
        help = 'JSON file describing jobs, from pulpito',
        )
    parser.add_argument(
        '--suites', dest='suites_string',
        required=True,
        help = 'suites to grab statistics on, as a comma-separated string',
        )
    parser.add_argument(
        '--machine-types', dest='machine_types_string',
        required=True,
        help = 'machine types to include, as a comma-separated string'
    )
    parser.add_argument(
        '--include-failed', help='do not exclude non-passed runs',
        action='store_true'
    )

    args = parser.parse_args()

    args.suites = args.suites_string.split(',')
    args.machine_types = args.machine_types_string.split(',')

    def stringcheck(strings):
        for entry in strings:
            if not isinstance(entry, basestring):
                print "'{entry}'' is not a string".format(entry=entry)
                sys.exit(1)
    stringcheck(args.suites)
    stringcheck(args.machine_types)

    return args

def parse_json_to_dict(ctx, data):
    try:
        json_data = json.loads(data)
    except ValueError, e:
        raise ValueError('could not parse json data')
    d = defaultdict(dict) # suite -> run -> list of jobs
    including = 0
    for record in json_data:
        if (record['suite'] in ctx.suites) and \
            (record['status'] == "pass" or ctx.include_failed):
            including += 1
            run_name = record['job'].split('/')[0]
            if not run_name in d[record['suite']]:
                d[record['suite']][run_name] = list()
            d[record['suite']][run_name].append(record)

    print "filtered out {num} results for {suites}".format(num=including, suites=ctx.suites_string)
    return d
    
def sum_data(suite_data):
    """suite_data: run -> [job1, job2, ...]
    Returns two-element tuple; dict of suite names to total machine times, and
        dict of job descriptions to list of runs
    """
    suite_run_results = {}
    job_results = defaultdict(list) # description -> [job1, job2, ...]
    for run_name, jobs in suite_data.iteritems():
        run_machine_time_sum = 0
        for job in jobs:
            run_machine_time_sum += job['duration'] * job['nodes']
            job_results[job['description']].append(job)
        suite_run_results[run_name] = run_machine_time_sum
    return (suite_run_results, job_results)

def combine_job_results(job_results):
    """job_results: description -> [job1, job2, ...]
    Returns a dict of job description -> tuple(total machine time, num runs, num machines)
    """
    averaged_results = {} # description -> (total machine runtime, num runs, num machines)
    for description, jobs in job_results.iteritems():
        total_machine_time = 0
        num_job_runs = 0
        num_machines = 0
        warned_on_change = False
        for job in jobs:
            total_machine_time += job['duration'] * job['nodes']
            num_job_runs += 1
            if num_machines is not 0 and num_machines != job['nodes'] and not warned_on_change:
                print "{desc} changed required machine number".format(desc=description)
                warned_on_change = True
            num_machines = job['nodes']
            averaged_results[description] = (total_machine_time, num_job_runs, num_machines)
    return averaged_results

def print_suite_stats(suite_totals):
    total_time = 0
    largest_time = ("", 0)
    run_count = 0
    for run_name, time in suite_totals.iteritems():
        total_time += time
        if time > largest_time[1]:
            largest_time = (run_name, time)
        run_count += 1
    print "Average machine runtime: {time} seconds".format(time=total_time/run_count)
    print "Longest machine runtime: {name} in {time} seconds".format(
        name=largest_time[0], time=largest_time[1] )        

def print_job_stats(job_results):
    print "(machine time, number of runs, machines used):description"
    results_list = list()
    for job_name, results in job_results.iteritems():
        list_tuple = (job_name, results)
        results_list.append(list_tuple)
    results_list.sort(key=lambda result: int(result[1][0])/result[1][1])
    for result_tuple in results_list:
        results = result_tuple[1]
        average = results[0]/results[1]
        print "({avg},{num},{mcount}):{name}".format(name=result_tuple[0],avg=average,num=results[1],mcount=results[2])

if __name__ == '__main__':
    ctx = parse_args()
    try:
        json_stream = open(ctx.json_file_string).read()
    except IOError as e:
        print 'cannot open %s' % json_stream
        print e
        sys.exit(1)
    try:
        suite_data = parse_json_to_dict(ctx, json_stream)
    except ValueError, e:
        print e
        sys.exit(1)
    for (suite_name, suite_results) in suite_data.iteritems():
        (suite_total_times, job_results) = sum_data(suite_results)
        combined_job_results = combine_job_results(job_results)
        print "********** Results for suite {name} **********".format(name=suite_name)
        print_suite_stats(suite_total_times)
        print "     ***** Job results *****     "
        print_job_stats(combined_job_results)
