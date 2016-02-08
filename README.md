# testerstats
Python script to analyze the json results of pulpito job statistics

Be nice to me; I'm not much good at writing python from scratch.

Sample command line:
./testerstats.py --json-file /Users/gfarnum/Downloads/node_stats/mira.json --suites fs,rados --machine-types mira

By default, it excludes any failed runs. If you want to include them in your
results, pass --include-failed. It'll be somewhat noisy: runs that never
lock machines result in a node count of 0, which gets flagged.
