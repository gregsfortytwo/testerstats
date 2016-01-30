#!/usr/bin/python
import argparse
import json
from collections import defaultdict
import sys

def parse_args():
    parser = argparse.ArgumentParser(description='Read JSON lab data and generate statistics.')
    parser.add_argument(
        '--json-file', dest='json_file',
        required=True,
        help = 'JSON file describing jobs, from pulpito',
        )
    parser.add_argument(
        '--suite', dest='suite',
        required=True,
        help = 'suite to summarize',
        )
    parser.add_argument(
        '--sort-by', dest='sort_element', default='description',
        help = 'job element to use as key'
    )
    parser.add_argument(
        '--filter', dest='filters_raw', default='status:fail',
        help = 'filter out elements matching your key:value[,k:v] string'
    )
    parser.add_argument(
        '--average', dest='average_field', default="",
        help = 'generate an average of the values in the named field'
    )
    args = parser.parse_args()
    args.filters = {}
    if len(args.filters_raw) > 2:
        for filter in args.filters_raw.split(','):
            [k,v] = filter.split(':')
            args.filters[k]=v
    return args

def parse_json_to_dict(ctx, data):
    try:
        json_data = json.loads(data)
    except ValueError, e:
        raise ValueError('could not parse json data')
    d = defaultdict(list)
    including = 0
    for record in json_data:
        include = True
        if (ctx.suite == record['suite']):
            for k,v in ctx.filters.iteritems():
                if (record[k]==v):
                    include = False
            if include is True:
                including += 1
                d[record[ctx.sort_element]].append(record)

    print "filtered out {num} results for {suite}".format(num=len(d), suite=ctx.suite)
    return d

def average_data(ctx, suite_data):
    averaged_data = {}
    print "averaging over {num} keys".format(num=len(suite_data))
    for key, entries in suite_data.iteritems():
        sum = 0;
        for entry in entries:
            sum += entry[ctx.average_field]
        averaged_data[key] = sum / len(entries)
    return averaged_data

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
        output = suite_data = parse_json_to_dict(ctx, json_stream)
    except ValueError, e:
        print e
        sys.exit(1)
    if (len(ctx.average_field) is not 0):
        print "averaging on {field}".format(field=ctx.average_field)
        output = average_data(ctx, suite_data)
    dump_results(output)
