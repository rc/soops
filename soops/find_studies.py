#!/usr/bin/env python
"""
Find parametric studies with parameters satisfying a given query.

Option-like parameters are transformed to valid Python attribute names removing
initial dashes and replacing other dashes by underscores. For example
'--output-dir' becomes 'output_dir'.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os.path as op

import pandas as pd

from soops.base import output
from soops.ioutils import locate_files

helps = {
    'query'
    : 'pandas query expression applied to collected parameters',
    'engine'
    : 'pandas query evaluation engine [default: %(default)s]',
    'mode'
    : 'output mode [default: %(default)s]',
    'key'
    : 'column key. If given, forces "single" output mode [default: output_dir]',
    'shell'
    : 'run ipython shell after all computations',
    'directories'
    : """one or more root directories with sub-directories containing
         parametric study results""",
}

def parse_args(args=None):
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-q', '--query', metavar='pandas-query-expression',
                        action='store', dest='query',
                        default=None, help=helps['query'])
    parser.add_argument('--engine', action='store', dest='engine',
                        choices=['numexpr', 'python'],
                        default='numexpr', help=helps['engine'])
    parser.add_argument('-m', '--mode', action='store', dest='mode',
                        choices=('truncated', 'full', 'single'),
                        default='truncated', help=helps['mode'])
    parser.add_argument('-k', '--key', action='store', dest='key',
                        default=None, help=helps['key'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('directories', nargs='+', help=helps['directories'])
    options = parser.parse_args(args=args)

    if options.key is not None:
        options.mode = 'single'

    elif options.mode == 'single':
        options.key = 'output_dir'

    return options

def find_studies(options):
    output.prefix = 'find:'

    dfs = []
    for root_dir in options.directories:
        for fname in locate_files('soops-parameters.csv', root_dir=root_dir):
            if op.exists(fname):
                try:
                    df = pd.read_csv(fname, index_col='pkey')

                except pd.errors.EmptyDataError:
                    continue

                else:
                    dfs.append(df)

    if len(dfs):
        apdf = pd.concat(dfs)
        apdf = apdf.rename(columns=lambda x: x.lstrip('-').replace('-', '_'))
        apdf = apdf.sort_values('output_dir', ignore_index=True)

        if options.query is not None:
            sdf = apdf.query(options.query, engine=options.engine)

            if options.mode in ('truncated', 'full'):
                if options.mode == 'full':
                    pd.set_option('display.max_colwidth', None)

                for ii in range(len(sdf)):
                    row = sdf.iloc[ii]
                    output('result {} in {}:\n{}'
                           .format(ii, row['output_dir'], row))

            elif options.mode == 'single':
                output.prefix = ''
                for ii in range(len(sdf)):
                    row = sdf.iloc[ii]
                    output(row[options.key])

    else:
        apdf = pd.DataFrame()

    if options.shell or (options.query is None):
        output('{} parameter sets stored in `apdf` DataFrame'.format(len(apdf)))
        output('column names:\n{}'.format(apdf.keys()))
        from soops.base import shell; shell()

    return apdf

def main():
    options = parse_args()
    find_studies(options)
    return

if __name__ == '__main__':
    sys.exit(main())
