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
        '--sort-by', dest='filter_element', default='description',
        help = 'job element to filter on'
    )
    args = parser.parse_args()
    return args

def parse_json_to_dict(data, suite, filter_element):
    try:
        json_data = json.loads(data)
    except ValueError, e:
        raise ValueError('could not parse json data')
    d = defaultdict(list)
    for record in json_data:
        if (suite == record['suite']):
            d[filter_element].append(record)

    print "filtered out {num} results for {suite}".format(num=len(d), suite=ctx.suite)
    return d

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
        suite_data = parse_json_to_dict(json_stream, ctx.suite, ctx.filter_element)
    except ValueError, e:
        print e
        sys.exit(1)
    dump_results(suite_data)
