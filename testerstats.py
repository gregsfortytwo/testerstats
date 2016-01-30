#!/usr/bin/python
import argparse
import json
import yaml
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser(description='Read JSON lab data and generate statistics.')
    parser.add_argument(
        'json_file',
        help = 'JSON output file',
        )
    args = parser.parse_args()
    return args

def parse_json(config, data):
    try:
        json_data = json.loads(data)
    except ValueError, e:
        raise ValueError('could not parse json data')
    d = defaultdict(list)
    for record in json_data:
        data = {}
        total = {}
        for value in ['status', 'job', 'nodes', 'duration', 'runtime', 'waited', 'description', 'suite']:
            data[value] = record[value]
        data['wallclockseconds'] = record['nodes'] * record['duration']
        d[record['suite']].append(data)
    return dict(d)

if __name__ == '__main__':
    ctx = parse_args()
    try:
        json_file = open(ctx.json_file).read()
    except IOError as e:
        print 'cannot open %s' % json_file
        sys.exit(1)
    try:
        suite_data = parse_json(json_file)
    except ValueError, e:
        sys.exit(1)
