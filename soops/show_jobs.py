#!/usr/bin/env python
"""
Show running jobs launched by soops-run.

Examples
--------

- Print jobs information::

  soops-jobs -v

- Follow the output of the last modified output log in the bash shell::

  tail -f $(soops-jobs -vv | tail -1)
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import psutil
import sys
import os
import os.path as op
from functools import partial

import pandas as pd

from soops.base import Struct
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
                        action='count', dest='verbose',
                        default=0, help=helps['verbose'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    options = parser.parse_args(args=args)
    return options

def find_jobs():
    jobs = [proc for proc in psutil.process_iter(['pid', 'cwd', 'cmdline'])
            if (proc.info['cmdline']
                and ('soops-run' in ''.join(proc.info['cmdline'])))]
    return jobs

def get_job_info(job):
    cmdline = job.info['cmdline']
    try:
        ii = cmdline.index('--output-dir')

    except ValueError:
        ii = cmdline.index('-o')

    aux = [ii for ii in job.info['cmdline'] if 'output_dir' in ii][0]
    run_options = parse_as_dict(aux, free_word=True)
    odir = run_options['output_dir'].strip(op.sep).replace('%s', '')

    inodir = partial(op.join, job.info['cwd'])

    output_dir = cmdline[ii+1]
    pfilename = inodir(output_dir, 'all_parameters.csv')
    apdf = pd.read_csv(pfilename, index_col='pkey')
    num = len(apdf)
    n_finished = apdf['finished'].sum()

    subdirs = [inodir(odir, ii) for ii in os.listdir(inodir(odir))
               if op.isdir(inodir(odir, ii))]
    last_dir = max(subdirs, key=op.getmtime)
    log_file = op.join(last_dir, 'output_log.txt')
    if not op.exists(log_file):
        log_file = ''

    info = Struct(
        job_output_dir=odir,
        num=num,
        n_finished=n_finished,
        apdf=apdf,
        last_dir=last_dir,
        log_file=log_file,
    )

    return info

def print_jobs_info(jobs, infos, options):
    if options.verbose:
        for job, info in zip(jobs, infos):
            print(f'job: {job.pid} ({job.status()})')
            print('output in:', info.job_output_dir)
            print(f'finished: {info.n_finished}/{info.num}')
            if options.verbose > 1:
                print('last log:')
                print(info.log_file)

    else:
        print([(job.pid, job.status()) for job in jobs])

def show_jobs(options):
    jobs = find_jobs()
    infos = [get_job_info(job) for job in jobs]
    print_jobs_info(jobs, infos, options)
    return jobs, infos

def main():
    options = parse_args()
    jobs, infos = show_jobs(options)

    if options.shell:
        from soops.base import shell; shell()

if __name__ == '__main__':
    sys.exit(main())
