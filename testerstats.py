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
        '--combine-machines', help='do not split up results by machine',
        action='store_true'
    )

    args = parser.parse_args()

    args.suites = split(args.suites_string, ',')
    args.machine_types = split(args.machine_types_string, ',')

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
    d = defaultdict(defaultdict(list)) # suite -> run -> list of jobs
    including = 0
    for record in json_data:
        include = True
        if (record['suite'] in args.suites):
            including += 1
            run_name = split(record['job'], '/')
            d[record['suite']][run_name].append(record)

    print "filtered out {num} results for {suites}".format(num=including, suites=ctx.suites_string)
    return d

def average_data(ctx, suite_data):
    # Okay, this method needs to average across runs. Much change required.
    averaged_data = {}
    for key, entries in suite_data.iteritems():
        sum = 0;
        for entry in entries:
            sum += entry[ctx.average_field]
        averaged_data[key] = sum / len(entries)
    return averaged_data

def sum_data(ctx, suite_data):
    sum = 0
    for key, entries in suite_data.iteritems():
        for entry in entries:
            sum += entry[ctx.summate_field]
    return sum

def dump_results(data):
    for k,v in data.iteritems():
        print "{desc}: {results}".format(desc=k, results=v)

if __name__ == '__main__':
    ctx = parse_args()
    try:
        json_stream = open(ctx.json_file).read()
    except IOError as e:
        print 'cannot open %s' % json_stream
        print e
        sys.exit(1)
    try:
        suite_data = parse_json_to_dict(ctx, json_stream)
    except ValueError, e:
        print e
        sys.exit(1)
    for suite in suite_data:
        output = average_data(ctx, suite_data)
        print "***** average of {field} *****".format(field=ctx.average_field)
        dump_results(output)
    if len(ctx.summate_field) is not 0:
        print "summing on {field}".format(field=ctx.summate_field)
        sum = sum_data(ctx, suite_data)
        print "***** sum of {field} is {sum} *****".format(field=ctx.summate_field, sum=sum)
