#!/usr/bin/env python
"""
Get parametric study configuration information.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os.path as op
import re

import pandas as pd

from soops.base import output, import_file

def collect_keys(run_cmd, opt_args, omit=()):
    keys = set(re.findall(r'\{(.+?)\}', run_cmd))
    keys.update(opt_args.keys())
    keys.difference_update(omit)
    return sorted(keys)

def explain_dir(dirname, keys):
    fname = op.join(dirname, 'soops-parameters.csv')
    df = pd.read_csv(fname, index_col='pkey')
    lmax = max(map(len, df.keys()))
    fmt = '{{}} {{:>{}s}}: {{}}'.format(lmax)
    for key, val in df.iloc[0].to_dict().items():
        output(fmt.format('*' if key in keys else ' ', key, val))

helps = {
    'explain' :
    'explain parameters used in the given output directory/directories',
    'shell' :
    'run ipython shell after all computations',
    'run_mod' :
    'the importable script/module with get_run_info()',
}

def parse_args(args=None):
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-e', '--explain', metavar='dirname',
                        action='store', dest='explain', nargs='+',
                        default=None, help=helps['explain'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('run_mod', help=helps['run_mod'])
    options = parser.parse_args(args=args)

    return options

def print_info(options):
    output.prefix = 'info:'

    run_mod = import_file(options.run_mod)
    if hasattr(run_mod, 'get_run_info'):
        (run_cmd, opt_args, output_dir_key,
         _is_finished) = run_mod.get_run_info()

    else:
        output('no get_run_info() in {}, exiting'.format(options.run_mod))
        return

    keys = collect_keys(run_cmd, opt_args,
                        omit=(output_dir_key, 'script_dir'))

    if options.explain is None:
        for ik, key in enumerate(keys):
            output('{:3d}: {}'.format(ik, key))

    else:
        for dirname in options.explain:
            output(dirname)
            explain_dir(dirname, keys)

    if options.shell:
        from soops.base import shell; shell()

def main():
    options = parse_args()
    return print_info(options)

if __name__ == '__main__':
    sys.exit(main())
