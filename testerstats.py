#!/usr/bin/python
import argparse
import json
import yaml
from collections import defaultdict

def read_config(config_file):
    config = {}
    try:
        with file(config_file) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config.update(new)
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))
    return config

def parse_args():
    parser = argparse.ArgumentParser(description='Read JSON lab data and generate statistics.')
    parser.add_argument(
        'json_file',
        help = 'JSON output file',
        )
    parser.add_argument(
        'config_file',
        help = 'YAML config file.',
        )
    args = parser.parse_args()
    return args

def parse_json(config, data):
    ignore_filters = config.get('ignore_filters', {})

    try:
        json_data = json.loads(data)
    except ValueError, e:
        raise ValueError('could not parse json data')
    d = defaultdict(list)
    for record in json_data: 
        cont = False
        for filt,values in ignore_filters.iteritems():
            if record[filt] in values:
                cont = True
                break
        if cont:
            continue
        data = {}
        total = {}
        
        for value in ['status', 'job', 'nodes', 'duration', 'runtime', 'waited']:
            data[value] = record[value]
        data['wallclockseconds'] = record['nodes'] * record['duration']
        d[record['suite']].append(data)
    return dict(d)

def get_node_dict(config):
    nc = config.get('node', {})
    initial_cost = nc.get('initial_cost', 0)
    yearlife = nc.get('yearlife', 3)
    hourcost = nc.get('hourcost', 0)
    cost = float(initial_cost) / (yearlife*365*24) + hourcost
    return {'initial_cost':initial_cost, 'yearlife':yearlife, 'hourcost':hourcost, 'cost':cost}

def get_dev_dict(config, suite):
    nd = get_node_dict(config)

    dev_config = config.get('dev', {})
    dev_suite = dev_config.get(suite, {})
    if not dev_suite:
        dev_suite = dev_config.get("global", {})

    hourcost = dev_suite.get('hourcost', 0)
    hours = dev_suite.get('hours', 0)
    improvement = dev_suite.get('improvement', 1)
    yearlife = dev_suite.get('yearlife', nd['yearlife'])
    cost = float(hourcost * hours) / (yearlife*365*24)

    return {'hourcost':hourcost, 'hours':hours, 'improvement':improvement, 'yearlife':yearlife, 'cost':cost}

def process_suites(config, d):
    node_cost = get_node_dict(config)['cost']

    td  = defaultdict(list)
    td['header1'] = ['suite', 'job_count', 'total duration', '', 'avg duration', '', 'total wallclock hours', '', 'avg wallclock hours', '', 'total job cost', '', 'avg job cost', '']
    td['header2'] = ['', '', 'base', 'base + dev', 'base', 'base + dev', 'base', 'base + dev', 'base', 'base + dev', 'base', 'base + dev', 'base', 'base + dev']

    for suite, v in d.iteritems():
        dd = get_dev_dict(config,suite)
        dev_imp = dd['improvement']
        dev_cost = dd['cost']

        td[suite].append(suite)
        td[suite].append(len(v))

        td[suite].append(gettotalhours(v, 'duration'))
        td[suite].append(gettotalhours(v, 'duration', dev_imp))
        td[suite].append(getavghours(v, 'duration'))
        td[suite].append(getavghours(v, 'duration', dev_imp))

        td[suite].append(gettotalhours(v, 'wallclockseconds'))
        td[suite].append(gettotalhours(v, 'wallclockseconds', dev_imp))
        td[suite].append(getavghours(v,'wallclockseconds'))
        td[suite].append(getavghours(v,'wallclockseconds', dev_imp))

        td[suite].append(gettotalhours(v, 'wallclockseconds') * node_cost)
        td[suite].append(gettotalhours(v, 'wallclockseconds', dev_imp) * (node_cost + dev_cost))
        td[suite].append(getavghours(v, 'wallclockseconds') * node_cost)
        td[suite].append(getavghours(v, 'wallclockseconds', dev_imp) * (node_cost + dev_cost))
    return dict(td)

def print_results(d):
    print '{0}'.format('\t'.join(d.pop('header1')))
    print '{0}'.format('\t'.join(d.pop('header2')))

    for suite, v in d.iteritems():
        vc = list(v)
        start = '%s\t%d\t' % (vc.pop(0), vc.pop(0))
        rest = '\t'.join(['{:.2f}'.format(x) for x in vc])
        print start+rest 
    
    totals = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for suite, v in d.iteritems():
        vc = list (v)
        vc.pop(0)
        totals = [x + y for x, y in zip(totals, vc)]
    print 'TOTAL\t'+'%i\t'%(totals.pop(0))+'\t'.join(['{:.2f}'.format(x) for x in totals])

def gettotalhours(suite, value, dev_imp=1):
    return float(sum(job[value] for job in suite)) / dev_imp / 3600

def getavghours(suite, value, dev_imp=1):
    return gettotalhours(suite, value, dev_imp) / len(suite)

def print_config(d, indent=0):
    addline = False
    for key, value in d.iteritems():
        if isinstance(value, dict):
            print '\t' * indent + str(key)
            if print_config(value, indent+1):
                print '' 
        else:
            print '\t' * indent + str(key) + '\t' + str(value)
            addline = True
    return addline

def print_csv(conifg, suite_data):
    print 'CONFIG'
    print ''
    print_config(config, 0)
    print ''
    print 'DATA'
    print ''
    print_results(process_suites(config, suite_data))


if __name__ == '__main__':
    ctx = parse_args()
    config = read_config(ctx.config_file)
    try:
        json_file = open(ctx.json_file).read()
    except IOError as e:
        print 'cannot open %s' % json_file
        sys.exit(1)
    try:
        suite_data = parse_json(config, json_file)
    except ValueError, e:
        sys.exit(1)
    print_csv(config, suite_data)
