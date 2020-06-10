#!/usr/bin/env python
"""
Run parametric studies.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os.path as op
import itertools
import subprocess

from dask.distributed import as_completed, Client, LocalCluster

from soops.parsing import parse_as_dict
from soops.base import output, import_file
from soops.ioutils import ensure_path, save_options
from soops.print_info import collect_keys

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

helps = {
    'recompute' :
     """recomputation strategy: 0: do not recompute,
        1: recompute only if is_finished() returns False,
        2: always recompute [default:  %(default)s]""",
    'contract' :
    'list of option keys that should be contracted to vary in lockstep',
    'n_workers' :
    'the number of dask workers [default: %(default)s]',
    'create_output_dirs' :
    'create parametric output directories if necessary',
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
    parser.add_argument('-r', '--recompute', action='store', type=int,
                        dest='recompute', choices=[0, 1, 2],
                        default=1, help=helps['recompute'])
    parser.add_argument('-c', '--contract', metavar='key1+key2+..., ...',
                        action='store', dest='contract',
                        default=None, help=helps['contract'])
    parser.add_argument('-n', '--n-workers', type=int, metavar='int',
                        action='store', dest='n_workers',
                        default=2, help=helps['n_workers'])
    parser.add_argument('--create-output-dirs',
                        action='store_true', dest='create_output_dirs',
                        default=False, help=helps['create_output_dirs'])
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

    key_order = collect_keys(run_cmd, opt_args,
                             omit=(output_dir_key, 'script_dir'))
    if not (keys.issuperset(key_order)
            and (keys.difference(key_order) == set(['output_dir']))):
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
    output_dir_template = dconf[output_dir_key]

    count = 0
    for _all_pars in itertools.product(*par_seqs):
        if not check_contracted(_all_pars, options, key_order): continue
        count += 1

    output('number of parameter sets:', count)

    calls = []
    iset = 0
    for _all_pars in itertools.product(*par_seqs):
        if not check_contracted(_all_pars, options, key_order): continue
        output('parameter set:', iset)
        output(_all_pars)

        _it, keys, vals = zip(*_all_pars)
        all_pars = dict(zip(keys, vals))
        it = '_'.join('%d' % ii for ii in _it)

        podir = output_dir_template % it
        all_pars[output_dir_key] = podir
        if options.create_output_dirs:
            ensure_path(podir + op.sep)

        all_pars['script_dir'] = op.normpath(op.dirname(options.run_mod))

        if (recompute > 1) or (recompute and not is_finished(podir)):
            cmd = make_cmd(run_cmd, opt_args, all_pars)
            output(cmd)

            call = client.submit(subprocess.call, cmd, shell=True, pure=False)
            call.iset = iset
            call.it = it
            call.all_pars = all_pars
            calls.append(call)

        else:
            call = client.submit(lambda: None)
            call.iset = iset
            call.it = it
            call.all_pars = all_pars
            calls.append(call)

        iset += 1

    for call in as_completed(calls):
        output(call.iset)
        output(call.it)
        output(call.all_pars)
        output(call, call.result())

    client.close()

    if options.shell:
        from soops.base import shell; shell()

def main():
    options = parse_args()
    return run_parametric(options)

if __name__ == '__main__':
    sys.exit(main())
