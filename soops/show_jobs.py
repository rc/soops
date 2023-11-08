#!/usr/bin/env python
"""
Show running jobs launched by soops-run.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import psutil
import sys
import os.path as op

import pandas as pd

from soops.parsing import parse_as_dict

helps = {
    'verbose'
    : 'print more information',
    'shell' :
    'run ipython shell after all computations',
}

def parse_args(args=None):
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--verbose',
                        action='store_true', dest='verbose',
                        default=False, help=helps['verbose'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    options = parser.parse_args(args=args)
    return options

def find_jobs():
    jobs = [proc for proc in psutil.process_iter(['pid', 'cwd', 'cmdline'])
            if 'soops-run' in ''.join(proc.info['cmdline'])]
    return jobs

def print_jobs_info(jobs, options):
    if options.verbose:
        apdfs = []
        for job in jobs:
            print(job.pid, job.status())

            cmdline = job.info['cmdline']
            try:
                ii = cmdline.index('--output-dir')

            except ValueError:
                ii = cmdline.index('-o')

            aux = [ii for ii in job.info['cmdline'] if 'output_dir' in ii][0]
            run_options = parse_as_dict(aux, free_word=True)
            odir = run_options['output_dir'].strip(op.sep).replace('%s', '')
            print('output in:', odir)

            output_dir = cmdline[ii+1]
            pfilename = op.join(job.info['cwd'], output_dir,
                                'all_parameters.csv')
            apdf = pd.read_csv(pfilename, index_col='pkey')
            num = len(apdf)
            n_finished = apdf['finished'].sum()
            print(f'finished: {n_finished}/{num}')

            apdfs.append(apdf)

    else:
        print([(job.pid, job.status()) for job in jobs])

def show_jobs(options):
    jobs = find_jobs()
    print_jobs_info(jobs, options)
    return jobs

def main():
    options = parse_args()
    show_jobs(options)

    if options.shell:
        from soops.base import shell; shell()

if __name__ == '__main__':
    sys.exit(main())
