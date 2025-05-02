#!/usr/bin/env python
"""
Run parametric studies.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os
import os.path as op
import subprocess
import hashlib
from datetime import datetime

import numpy as np
import pandas as pd
from dask.distributed import as_completed, Client, LocalCluster

from soops.parsing import parse_as_dict
from soops.base import output, import_file, product, Struct
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
            cmd += ' "' + opt_args[key].strip() + '"'

    cmd = cmd.format(**all_pars)
    return cmd

def gen_run_script(output_dir, cmd):
    """
    Generate a python script in `output_dir` that runs the command `cmd`.
    """
    script = f"""#!/usr/bin/env python3
import subprocess
import sys

cmd = {cmd!r}

result = subprocess.run(cmd, shell=True)
sys.exit(result.returncode)
"""
    filename = op.join(output_dir, 'run.py')
    with open(filename, 'w') as fd:
        fd.write(script)
    os.chmod(filename, 0o755)

def run_with_psutil(cmd, options):
    import psutil

    proc = psutil.Popen(cmd, shell=True)
    try:
        retcode = proc.wait(timeout=options.timeout)
        out = subprocess.CompletedProcess(proc.args, retcode)

    except psutil.TimeoutExpired as exc:
        for cproc in proc.children(recursive=True):
            cproc.kill()
        proc.kill()
        out = exc

    return out

def get_contracts(contract_seqs, par_seqs, key_order):
    if contract_seqs is not None:
        contracts = [[key_order.index(key) for key in contract]
                     for contract in contract_seqs]
        for ic, contract in enumerate(contracts):
            sizes = {len(par_seqs[ii]) for ii in contract}
            if len(sizes) != 1:
                raise ValueError('contracted parameter sequences {} have {}'
                                 ' different lengths!'
                                 .format(contract_seqs[ic], len(sizes)))

    else:
        contracts = None

    return contracts

def _get_iset(path):
    iset = int(op.basename(path).split('-')[0])
    return iset

def _get_dict_from_cfg(config, key):
    aux = list(config[key].items())
    conf =  ','.join(['='.join([ii for ii in opt]) for opt in aux])
    dconf = parse_as_dict(conf, free_word=True)
    return dconf

helps = {
    'dry_run':
    'perform a trial run with no commands executed',
    'recompute' :
     """recomputation strategy: 0: do not recompute,
        1: recompute only if is_finished() returns False,
        2: always recompute [default:  %(default)s]""",
    'n_workers' :
    'the number of dask workers [default: %(default)s]',
    'cluster_kwargs' :
    'additional keyword arguments for LocalCluster [default:  %(default)s]',
    'run_function' :
    'function for running the parameterized command [default: %(default)s]',
    'timeout' :
    """if given, the timeout in seconds; requires setting
       --run-function=psutil.Popen""",
    'generate_pars' :
    """if given, generate values of parameters using the specified function;
       the generated parameters must be set to @generate in
       the parametric study configuration. Alternatively, a section key in
       a study configuration file.""",
    'contract' :
    'list of option keys that should be contracted to vary in lockstep',
    'compute_pars' :
    'if given, compute additional parameters using the specified class',
    'study' :
    'study key when parameter sets are given by a study configuration file',
    'extra_conf' :
    """a dict-like parametric study configuration that can be used to override
       values in 'conf' positional argument""",
    'silent' :
    'do not print messages to screen',
    'shell' :
    'run ipython shell after all computations',
    'output_dir' :
    'output directory [default: %(default)s]',
    'conf' :
    """a dict-like parametric study configuration or a study configuration
       file name""",
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
    parser.add_argument('-n', '--n-workers', type=int, metavar='int',
                        action='store', dest='n_workers',
                        default=2, help=helps['n_workers'])
    parser.add_argument('--cluster-kwargs', metavar='dict-like',
                        action='store', dest='cluster_kwargs',
                        default='threads_per_worker=1',
                        help=helps['cluster_kwargs'])
    parser.add_argument('--run-function', action='store', dest='run_function',
                        choices=['subprocess.run', 'psutil.Popen', 'os.system'],
                        default='subprocess.run', help=helps['run_function'])
    parser.add_argument('-t', '--timeout', type=float, metavar='float',
                        action='store', dest='timeout',
                        default=None, help=helps['timeout'])
    parser.add_argument('--generate-pars',
                        metavar=('dict-like: function=function_name,'
                                 'par0=val0,... or str'),
                        action='store', dest='generate_pars',
                        default=None, help=helps['generate_pars'])
    parser.add_argument('-c', '--contract', metavar='key1+key2+..., ...',
                        action='store', dest='contract',
                        default=None, help=helps['contract'])
    parser.add_argument('--compute-pars',
                        metavar='dict-like: class=class_name,par0=val0,...',
                        action='store', dest='compute_pars',
                        default=None, help=helps['compute_pars'])
    parser.add_argument('-s', '--study', metavar='str',
                        action='store', dest='study',
                        default=None, help=helps['study'])
    parser.add_argument('--conf', metavar='str',
                        action='store', dest='extra_conf',
                        default='', help=helps['extra_conf'])
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

    options.extra_conf = parse_as_dict(options.extra_conf, free_word=True)
    options.cluster_kwargs = parse_as_dict(options.cluster_kwargs)

    if options.generate_pars is not None:
        if ('=' in options.generate_pars) or (':' in options.generate_pars):
            options.generate_pars = parse_as_dict(options.generate_pars,
                                                  free_word=True)

    if options.contract is not None:
        options.contract = [[ii.strip() for ii in contract.split('+')]
                            for contract in options.contract.split(',')]

    if options.compute_pars is not None:
         options.compute_pars = parse_as_dict(options.compute_pars,
                                              free_word=True)

    if ((options.timeout is not None) and
        (options.run_function != 'psutil.Popen')):
        raise ValueError('to use --timeout, "pip install psutil"'
                         ' and set --run-function=psutil.Popen')

    return options

def run_parametric(options):
    output.prefix = 'run:'

    run_mod = import_file(options.run_mod)
    if hasattr(run_mod, 'get_run_info'):
        (run_cmd, opt_args, output_dir_key,
         _is_finished) = run_mod.get_run_info()

        defaults = {}
        nonhash_pars = []
        if isinstance(opt_args, tuple):
            if len(opt_args) == 2:
                opt_args, defaults = opt_args

            else:
                opt_args, defaults, nonhash_pars = opt_args

        if isinstance(opt_args, list):
            # '--option={--option}' -> '--option' : '--option={--option}'
            opt_args = {item.split('=')[0] : item for item in opt_args}

    else:
        output('no get_run_info() in {}, exiting'.format(options.run_mod))
        return

    if isinstance(_is_finished, str):
        is_finished = (lambda pars, options:
                       op.exists(op.join(pars[output_dir_key], _is_finished)))

    else:
        is_finished = _is_finished

    if op.isfile(options.conf):
        import configparser
        config = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation(),
        )

        config.read(options.conf)
        skeys = list(config.keys())
        skeys.pop(skeys.index('DEFAULT'))

        if options.study is None:
            skey = skeys[0]

        else:
            if options.study not in skeys:
                raise ValueError('no {} key in study configuration file {}!'
                                 .format(options.study, options.conf))

            skey = options.study

        dconf = _get_dict_from_cfg(config, skey)

    else:
        dconf = parse_as_dict(options.conf, free_word=True)

    dconf.update(options.extra_conf)

    seq_keys = [key for key, val in dconf.items()
                if isinstance(val, str) and
                (val.startswith('@arange') or val.startswith('@linspace'))]
    for key in seq_keys:
        sfun = 'np.' + dconf[key][1:]
        dconf[key] = eval(sfun, {'np' : np}, {}).tolist()

    if options.generate_pars is not None:
        if op.isfile(options.conf) and (isinstance(options.generate_pars, str)):
            if options.generate_pars not in skeys:
                raise ValueError('no {} key in study configuration file {}!'
                                 .format(options.generate_pars, options.conf))
            dgenerate_pars = _get_dict_from_cfg(config, options.generate_pars)

        else:
            dgenerate_pars = options.generate_pars.copy()

        fun_name = dgenerate_pars.pop('function')
        generate_pars = getattr(run_mod, fun_name)

        gkeys = [key for key, val in dconf.items() if val == '@generate']
        output('generated parameters:', gkeys)

        gconf = generate_pars(Struct(dgenerate_pars), gkeys, dconf, options)
        if set(gkeys) != set(gconf.keys()):
            output('conf:\n{}'.format(sorted(gkeys)))
            output('generated:\n{}'.format(sorted(gconf.keys())))
            raise ValueError('generated keys mismatch! (see above)')

        dconf.update(gconf)

    keys = set(dconf.keys())
    keys.update(opt_args.keys())

    if options.compute_pars is not None:
        dcompute_pars = options.compute_pars.copy()

        class_name = dcompute_pars.pop('class')
        ComputePars = getattr(run_mod, class_name)

        keys.update(dcompute_pars.keys())

    key_order = collect_keys(run_cmd, opt_args,
                             omit=(output_dir_key, 'script_dir'))
    collected_keys = set(key_order).union({output_dir_key})
    if collected_keys != set(keys):
        output('conf:\n{}'.format(sorted(keys)))
        output('collected:\n{}'.format(sorted(collected_keys)))
        output('missing in conf:\n{}'
               .format(sorted(collected_keys.difference(keys))))
        raise ValueError('parametric keys mismatch! (see above)')

    filename = op.join(options.output_dir, 'options.txt')
    ensure_path(filename)
    save_options(filename, [('options', vars(options))],
                 quote_command_line=True)

    output.set_output(filename=op.join(options.output_dir, 'output_log.txt'),
                      combined=options.verbose)

    par_seqs = [
        make_key_list(key, dconf.get(key, defaults.get(key, '@undefined')))
        for key in key_order
    ]

    contracts = get_contracts(options.contract, par_seqs, key_order)

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
        iseq = apdf[output_dir_key].apply(_get_iset).max() + 1

    else:
        apdf = pd.DataFrame()
        iseq = 0

    pkeys = set(apdf.index)

    count = 0
    for _all_pars in product(*par_seqs, contracts=contracts):
        count += 1

    output('number of parameter sets:', count)

    cluster = LocalCluster(n_workers=options.n_workers,
                           **options.cluster_kwargs)
    client = Client(cluster)

    calls = []
    for _all_pars in product(*par_seqs, contracts=contracts):
        _it, keys, vals = zip(*_all_pars)
        all_pars = dict(zip(keys, vals))
        all_pars.update(compute_pars(all_pars))
        it = ' '.join('%d' % ii for ii in _it)

        hash_pars = {key : val for key, val in all_pars.items()
                     if key not in nonhash_pars}
        pkey = hashlib.md5(str(hash_pars).encode('utf-8')).hexdigest()
        if pkey in pkeys:
            podir = apdf.loc[pkey, output_dir_key]
            iset = _get_iset(podir)
            new = False

        else:
            iset = iseq
            podir = output_dir_template % ('{:03d}-{}'.format(iset, pkey))
            new = True

        output('parameter set:', iset)
        output(_all_pars)

        all_pars[output_dir_key] = podir
        ensure_path(podir + op.sep)

        all_pars['script_dir'] = op.normpath(op.dirname(options.run_mod))

        recompute = options.recompute
        if ((recompute > 1) or
            (recompute and not is_finished(all_pars, options))):

            sdf = pd.DataFrame(
                {
                    'finished' : False,
                    'iset' : '{:03d}'.format(iset),
                    **all_pars
                },
                index=[pkey],
            )
            sdf.to_csv(op.join(podir, 'soops-parameters.csv'),
                       index_label='pkey')

            if pkey in pkeys:
                apdf.loc[pkey] = sdf.iloc[0]

            else:
                apdf = pd.concat((apdf, sdf))

            cmd = make_cmd(run_cmd, opt_args, all_pars)
            gen_run_script(podir, cmd)
            dtime = datetime.now()
            output('submitting at', get_timestamp(dtime=dtime))
            output(cmd)

            if options.dry_run:
                call = client.submit(lambda: None)

            elif options.run_function == 'subprocess.run':
                call = client.submit(subprocess.run, cmd,
                                     shell=True, pure=False)

            elif options.run_function == 'psutil.Popen':
                call = client.submit(run_with_psutil, cmd, options,
                                     pure=False)

            else:
                call = client.submit(os.system, cmd, pure=False)

            call.iset = iset
            call.it = it
            call.pkey = pkey
            call.podir = podir
            call.update_parameters = True
            call.all_pars = all_pars
            call.dtime = dtime
            calls.append(call)

            if new:
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
        output(call)
        output(call.result())
        if call.update_parameters:
            finished = True
            if options.timeout is not None:
                import psutil
                if isinstance(call.result(), psutil.TimeoutExpired):
                    finished = False

            apdf.loc[call.pkey, 'finished'] = finished
            sdf = apdf.loc[[call.pkey]]
            sdf.to_csv(op.join(call.podir, 'soops-parameters.csv'),
                       index_label='pkey')
            apdf.to_csv(pfilename, mode='w', index_label='pkey')

    client.close()

    if options.shell:
        from soops.base import shell; shell()

    cluster.close()

def main():
    options = parse_args()
    return run_parametric(options)

if __name__ == '__main__':
    sys.exit(main())
