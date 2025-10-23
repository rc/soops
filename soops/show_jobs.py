#!/usr/bin/env python
"""
Show running jobs launched by soops-run.

Examples
--------

- Print jobs information::

  soops-jobs -v
  soops-jobs -vv

- Follow the output of the last modified output log in the bash shell::

  tail -f $(soops-jobs -vvv | tail -1)
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import psutil
import sys
import os
import os.path as op
from functools import partial

import pandas as pd

from soops.base import import_file, Struct
from soops.run_parametric import parse_args as pa
from soops.run_parametric import get_study_conf

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

    inodir = partial(op.join, job.info['cwd'])

    try:
        job_options = pa(args=cmdline[2:])
        run_mod = import_file(inodir(job_options.run_mod))
        (run_cmd, opt_args, output_dir_key, _is_finished) = run_mod.get_run_info()

        conf, _, _ = get_study_conf(inodir(job_options.conf), study=job_options.study,
                                    extra_conf=job_options.extra_conf)
        odir = conf[output_dir_key].strip(op.sep).replace('%s', '')

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

    except:
        info = Struct(
            job_working_dir=job.info['cwd'],
            job_output_dir='unknown',
            num=-1,
            n_finished=-1,
            apdf=None,
            last_dir=None,
            log_file=None,
            job_options=Struct(),
        )

    else:
        info = Struct(
            job_working_dir=job.info['cwd'],
            job_output_dir=odir,
            num=num,
            n_finished=n_finished,
            apdf=apdf,
            last_dir=last_dir,
            log_file=log_file,
            job_options=Struct(vars(job_options)),
        )

    return info

def print_jobs_info(jobs, infos, options):
    if options.verbose:
        for job, info in zip(jobs, infos):
            print(f'job: {job.pid} ({job.status()})')
            print('working directory:', info.job_working_dir)
            print('output in:', info.job_output_dir)
            print(f'finished: {info.n_finished}/{info.num}')

            if options.verbose > 1:
                print('options:')
                print(info.job_options)

            if options.verbose > 2:
                print('last log:')
                print(info.log_file)

    else:
        for job in jobs:
            print(job.pid, job.status())

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
