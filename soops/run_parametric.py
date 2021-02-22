#!/usr/bin/env python
"""
Run parametric studies.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os
import os.path as op
import itertools
import subprocess
import hashlib
from datetime import datetime

import pandas as pd
from dask.distributed import as_completed, Client, LocalCluster

from soops.parsing import parse_as_dict
from soops.base import output, import_file
from soops.ioutils import ensure_path, save_options, locate_files
from soops.print_info import collect_keys
from soops.timing import get_timestamp

def make_key_list(key, obj):
    return ([(ii, key, item) for ii, item in enumerate(obj)]
            if isinstance(obj, list) else [(0, key, obj)])

def make_cmd(run_cmd, opt_args, all_pars):
    cmd = run_cmd.strip()
    for key in opt_args:
        if key in all_pars:
            par = all_pars[key]
            if isinstance(par, str) and par.startswith('@'):
                if par == '@undefined':
                    continue

                elif par == '@defined':
                    pass

                else:
                    raise ValueError(
                        'unsupported specital parameter value! (%s)' % par
                    )
            cmd += ' ' + opt_args[key].strip()

    cmd = cmd.format(**all_pars)
    return cmd

def check_contracted(all_pars, options, key_order):
    if options.contract is None: return True

    ok = True
    for contract in options.contract:
        iis = [all_pars[key_order.index(key)][0] for key in contract]
        if len(set(iis)) > 1:
            ok = False
            break
    return ok

def _get_iset(path):
    iset = int(op.basename(path).split('-')[0])
    return iset

helps = {
    'dry_run':
    'perform a trial run with no commands executed',
    'recompute' :
     """recomputation strategy: 0: do not recompute,
        1: recompute only if is_finished() returns False,
        2: always recompute [default:  %(default)s]""",
    'contract' :
    'list of option keys that should be contracted to vary in lockstep',
    'compute_pars' :
    'if given, compute additional parameters using the specified class',
    'n_workers' :
    'the number of dask workers [default: %(default)s]',
    'run_function' :
    'function for running the parameterized command [default: %(default)s]',
    'silent' :
    'do not print messages to screen',
    'shell' :
    'run ipython shell after all computations',
    'output_dir' :
    'output directory [default: %(default)s]',
    'conf' :
    'a dict-like parametric study configuration',
    'run_mod' :
    'the importable script/module with get_run_info()',
}

def parse_args(args=None):
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run',
                        action='store_true', dest='dry_run',
                        default=False, help=helps['dry_run'])
    parser.add_argument('-r', '--recompute', action='store', type=int,
                        dest='recompute', choices=[0, 1, 2],
                        default=1, help=helps['recompute'])
    parser.add_argument('-c', '--contract', metavar='key1+key2+..., ...',
                        action='store', dest='contract',
                        default=None, help=helps['contract'])
    parser.add_argument('--compute-pars',
                        metavar='dict-like: class=class_name,par0=val0,...',
                        action='store', dest='compute_pars',
                        default=None, help=helps['compute_pars'])
    parser.add_argument('-n', '--n-workers', type=int, metavar='int',
                        action='store', dest='n_workers',
                        default=2, help=helps['n_workers'])
    parser.add_argument('--run-function', action='store', dest='run_function',
                        choices=['subprocess.run', 'os.system'],
                        default='subprocess.run', help=helps['run_function'])
    parser.add_argument('--silent',
                        action='store_false', dest='verbose',
                        default=True, help=helps['silent'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('-o', '--output-dir', metavar='path',
                        action='store', dest='output_dir',
                        default='output', help=helps['output_dir'])
    parser.add_argument('conf', help=helps['conf'])
    parser.add_argument('run_mod', help=helps['run_mod'])
    options = parser.parse_args(args=args)

    if options.contract is not None:
        options.contract = [[ii.strip() for ii in contract.split('+')]
                            for contract in options.contract.split(',')]

    return options

def run_parametric(options):
    output.prefix = 'run:'

    run_mod = import_file(options.run_mod)
    if hasattr(run_mod, 'get_run_info'):
        (run_cmd, opt_args, output_dir_key,
         _is_finished) = run_mod.get_run_info()

    else:
        output('no get_run_info() in {}, exiting'.format(options.run_mod))
        return

    if isinstance(_is_finished, str):
        is_finished = lambda x: op.exists(op.join(x, _is_finished))

    else:
        is_finished = _is_finished

    dconf = parse_as_dict(options.conf, free_word=True)

    keys = set(dconf.keys())
    keys.update(opt_args.keys())

    if options.compute_pars is not None:
        dcompute_pars = parse_as_dict(options.compute_pars, free_word=True)
        options.compute_pars = dcompute_pars.copy()

        class_name = dcompute_pars.pop('class')
        ComputePars = getattr(run_mod, class_name)

        keys.update(dcompute_pars.keys())

    key_order = collect_keys(run_cmd, opt_args,
                             omit=(output_dir_key, 'script_dir'))
    if not (keys.issuperset(key_order)
            and (keys.difference(key_order) == set([output_dir_key]))):
        raise ValueError('parametric keys mismatch! (conf: {},  collected: {})'
                         .format(keys, key_order))

    filename = op.join(options.output_dir, 'options.txt')
    ensure_path(filename)
    save_options(filename, [('options', vars(options))],
                 quote_command_line=True)

    output.set_output(filename=op.join(options.output_dir, 'output_log.txt'),
                      combined=options.verbose)

    recompute = options.recompute

    cluster = LocalCluster(n_workers=options.n_workers, threads_per_worker=1)
    client = Client(cluster)

    par_seqs = [make_key_list(key, dconf.get(key, '@undefined'))
                for key in key_order]

    if options.compute_pars is not None:
        compute_pars = ComputePars(dcompute_pars, par_seqs, key_order, options)

    else:
        compute_pars = lambda x: {}

    output_dir_template = dconf[output_dir_key]

    # Load existing parameter sets.
    dfs = []
    root_dir = output_dir_template.split('%s')[0]
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
        iseq = apdf['output_dir'].apply(_get_iset).max() + 1

    else:
        apdf = pd.DataFrame()
        iseq = 0

    pkeys = set(apdf.index)

    count = 0
    for _all_pars in itertools.product(*par_seqs):
        if not check_contracted(_all_pars, options, key_order): continue
        count += 1

    output('number of parameter sets:', count)

    calls = []
    for _all_pars in itertools.product(*par_seqs):
        if not check_contracted(_all_pars, options, key_order): continue

        _it, keys, vals = zip(*_all_pars)
        all_pars = dict(zip(keys, vals))
        all_pars.update(compute_pars(all_pars))
        it = ' '.join('%d' % ii for ii in _it)

        pkey = hashlib.md5(str(all_pars).encode('utf-8')).hexdigest()
        if pkey in pkeys:
            podir = apdf.loc[pkey, 'output_dir']
            iset = _get_iset(podir)

        else:
            iset = iseq
            podir = output_dir_template % ('{:03d}-{}'.format(iset, pkey))


        output('parameter set:', iset)
        output(_all_pars)

        all_pars[output_dir_key] = podir
        ensure_path(podir + op.sep)

        all_pars['script_dir'] = op.normpath(op.dirname(options.run_mod))

        if  ((not options.dry_run) and
             ((recompute > 1) or
              (recompute and not is_finished(podir)))):
            sdf = pd.DataFrame({'finished' : False, **all_pars}, index=[pkey])
            sdf.to_csv(op.join(podir, 'soops-parameters.csv'),
                       index_label='pkey')

            if pkey in pkeys:
                apdf.loc[pkey] = sdf.iloc[0]

            else:
                apdf = apdf.append(sdf)

            cmd = make_cmd(run_cmd, opt_args, all_pars)
            dtime = datetime.now()
            output('submitting at', get_timestamp(dtime=dtime))
            output(cmd)

            if options.run_function == 'subprocess.run':
                call = client.submit(subprocess.run, cmd,
                                     shell=True, pure=False)

            else:
                call = client.submit(os.system, cmd)

            call.iset = iset
            call.it = it
            call.pkey = pkey
            call.podir = podir
            call.update_parameters = True
            call.all_pars = all_pars
            call.dtime = dtime
            calls.append(call)

            iseq += 1

        else:
            call = client.submit(lambda: None)
            call.iset = iset
            call.it = it
            call.pkey = pkey
            call.podir = podir
            call.update_parameters = not apdf.loc[pkey, 'finished']
            call.all_pars = all_pars
            call.dtime = datetime.now()
            calls.append(call)

    pfilename = op.join(options.output_dir, 'all_parameters.csv')
    apdf.to_csv(pfilename, mode='w', index_label='pkey')

    for call in as_completed(calls):
        dtime = datetime.now()
        output(call.iset)
        output(call.it)
        output('in', call.podir)
        output('completed at', get_timestamp(dtime=dtime) , 'in',
               dtime - call.dtime)
        output(call.all_pars)
        output(call, call.result())

        if call.update_parameters:
            apdf.loc[call.pkey, 'finished'] = True
            sdf = apdf.loc[[call.pkey]]
            sdf.to_csv(op.join(call.podir, 'soops-parameters.csv'),
                       index_label='pkey')
            apdf.to_csv(pfilename, mode='w', index_label='pkey')

    client.close()

    if options.shell:
        from soops.base import shell; shell()

def main():
    options = parse_args()
    return run_parametric(options)

if __name__ == '__main__':
    sys.exit(main())
