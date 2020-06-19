#!/usr/bin/env python
"""
Scoop output files.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os.path as op
from datetime import datetime
import warnings

import numpy as np
import pandas as pd

from soops.base import output, import_file, Struct
from soops.parsing import parse_as_dict, parse_as_list
from soops.ioutils import load_options, locate_files, ensure_path

def load_array(filename, key='array', load_kwargs={}, rdata=None):
    arr = np.loadtxt(filename, **load_kwargs)
    return {key : arr}

def load_csv(filename, orient='list', rdata=None):
    df = pd.read_csv(filename)
    return df.to_dict(orient=orient)

def split_options(options, split_keys):
    new_options = options.copy()
    for okey, nkeys in split_keys.items():
        vals = new_options.pop(okey)
        if nkeys is None:
            new_options.update({okey + '__' + key: val
                                for key, val in vals.items()})

        else:
            new_options.update({okey + '__' + nkeys[ii]: val
                                for ii, val in enumerate(vals)})

    return new_options

def load_split_options(filename, split_keys=None, rdata=None):
    options = load_options(filename)
    if split_keys is not None:
        options = split_options(options, split_keys=split_keys)
    return options

def apply_scoops(info, directories):
    if not len(info):
        return pd.DataFrame({}), pd.DataFrame({}), None

    data = []
    metadata = []
    par_keys = set()
    for idir, directory in enumerate(directories):
        output('directory {}: {}'.format(idir, directory))

        name0 = info[0][0]
        filenames = locate_files(name0, directory)
        home = op.expanduser('~')
        for ir, filename in enumerate(filenames):
            rdir = op.dirname(filename)
            output('results directory {}: {}'.format(ir, rdir))

            rdata = {'rdir' : rdir.replace(home, '~')}
            rmetadata = {}
            output('results files:')
            for item in info:
                if len(item) == 2:
                    filename, fun = item
                    has_parameters = False

                elif len(item) == 3:
                    filename, fun, has_parameters = item

                else:
                    raise ValueError('scoop info item has to have length'
                                     ' 2 or 3! ({})'.format(item))

                output(filename)
                path = op.join(rdir, filename)
                try:
                    out = fun(path, rdata=rdata)

                except KeyboardInterrupt:
                    raise

                except Exception as exc:
                    output('- failed with:')
                    output(exc)
                    continue

                else:
                    try:
                        mtime = datetime.fromtimestamp(op.getmtime(path))

                    except FileNotFoundError:
                        mtime = np.nan

                    rmetadata.update({
                        'data_row' : len(data),
                        'data_columns' : tuple(out.keys()),
                        'filename' : path,
                        'mtime' : mtime,
                    })
                    rdata.update(out)
                    metadata.append(pd.Series(rmetadata))
                    if has_parameters:
                        par_keys.update(out.keys())

            rdata['time'] = datetime.utcnow()

            data.append(pd.Series(rdata))

    df = pd.DataFrame(data)
    mdf = pd.DataFrame(metadata)

    return df, mdf, par_keys

def get_uniques(df, columns):
    uniques = {}
    for col in sorted(columns):
        try:
            vals = sorted(df[col].unique())

        except TypeError:
            _, ir = np.unique([str(ii) for ii in df[col]], return_index=True)
            vals = df.loc[ir, col].tolist() # np.unique() sorts.

        uniques[col] = vals

    return uniques

def run_plugins(info, df, output_dir, par_keys, plugin_args=None):
    if not len(info):
        return

    if plugin_args is None: plugin_args = {}

    used = {fun.__name__ for fun in info if fun.__name__ in plugin_args}
    unused = set(plugin_args.keys()).difference(used)
    if len(unused):
        output('WARNING: unused plugin arguments:', unused)

    def wrap_fun(fun):
        args = plugin_args.get(fun.__name__)
        if args is None:
            _fun = fun

        else:
            def _fun(df, data=None):
                return fun(df, data=data, **args)

        return _fun

    output('run plugins:')
    par_uniques = get_uniques(df, par_keys)
    multi_par_keys = [key for key, vals in par_uniques.items()
                      if len(vals) > 1]
    multi_par_uniques = {key : par_uniques[key] for key in multi_par_keys}
    data = Struct(par_keys=par_keys,
                  multi_par_keys=multi_par_keys,
                  par_uniques=par_uniques,
                  multi_par_uniques=multi_par_uniques,
                  output_dir=output_dir)
    for fun in info:
        output('running {}()...'.format(fun.__name__))
        wfun = wrap_fun(fun)
        _data = wfun(df, data=data)
        data = _data if _data is not None else data
        output('...done')

    return data

helps = {
    'sort' : 'column keys for sorting of DataFrame rows',
    'results' : 'reuse previously scooped results file',
    'no_plugins' : 'do not call post-processing plugins',
    'use_plugins' : 'use only the named plugins (no effect with --no-plugins)',
    'omit_plugins' : 'omit the named plugins (no effect with --no-plugins)',
    'plugin_mod' :
    'if given, the module that has get_plugin_info() instead of scoop_mod',
    'plugin_args' :
    """optional arguments passed to plugins given as plugin_name={key1=val1,
       key2=val2, ...}, ...""",
    'shell' : 'run ipython shell after all computations',
    'output_dir' : 'output directory [default: %(default)s]',
    'scoop_mod' : 'the importable script/module with get_scoop_info()',
    'directories' : 'results directories',
}

def parse_args(args=None):
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-s', '--sort', metavar='column[,columns,...]',
                        action='store', dest='sort',
                        default=None, help=helps['sort'])
    parser.add_argument('-r', '--results', metavar='filename',
                        action='store', dest='results',
                        default=None, help=helps['results'])
    parser.add_argument('--no-plugins',
                        action='store_false', dest='call_plugins',
                        default=True, help=helps['no_plugins'])
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--use-plugins', metavar='name[,name,...]',
                       action='store', dest='use_plugins',
                       default=None, help=helps['use_plugins'])
    group.add_argument('--omit-plugins', metavar='name[,name,...]',
                       action='store', dest='omit_plugins',
                       default=None, help=helps['omit_plugins'])
    parser.add_argument('-p', '--plugin-mod', metavar='module',
                        action='store', dest='plugin_mod',
                        default=None, help=helps['plugin_mod'])
    parser.add_argument('--plugin-args', metavar='dict-like',
                        action='store', dest='plugin_args',
                        default=None, help=helps['plugin_args'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('-o', '--output-dir', metavar='path',
                        action='store', dest='output_dir',
                        default='.', help=helps['output_dir'])
    parser.add_argument('scoop_mod', help=helps['scoop_mod'])
    parser.add_argument('directories', nargs='+', help=helps['directories'])
    options = parser.parse_args(args=args)

    options.sort = parse_as_list(options.sort)

    if options.use_plugins is not None:
        options.use_plugins = parse_as_list(options.use_plugins)

    if options.omit_plugins is not None:
        options.omit_plugins = parse_as_list(options.omit_plugins)

    if options.plugin_args is not None:
        options.plugin_args = parse_as_dict(options.plugin_args)

    return options

def scoop_outputs(options):
    output.prefix = ''

    scoop_mod = import_file(options.scoop_mod)

    if (options.results is None
        or not (op.exists(options.results) and op.isfile(options.results))):

        if hasattr(scoop_mod, 'get_scoop_info'):
            scoop_info = scoop_mod.get_scoop_info()

        else:
            output('no get_scoop_info() in {}, exiting'
                   .format(options.scoop_mod))
            return

        df, mdf, par_keys = apply_scoops(scoop_info, options.directories)

    else:
        df = pd.read_hdf(options.results, 'df')
        mdf = pd.read_hdf(options.results, 'mdf')
        par_keys = set(pd.read_hdf(options.results, 'par_keys').to_list())

    output('data keys:')
    output(df.keys())
    output('metadata keys:')
    output(mdf.keys())

    if options.sort:
        df = df.sort_values(options.sort)
        df.index = np.arange(len(df))

    warnings.simplefilter(action='ignore',
                          category=pd.errors.PerformanceWarning)

    filename = op.join(options.output_dir, 'results.csv')
    ensure_path(filename)
    df.to_csv(filename)
    filename = op.join(options.output_dir, 'results-meta.csv')
    mdf.to_csv(filename)

    filename = op.join(options.output_dir, 'results.h5')
    store = pd.HDFStore(filename, mode='w')
    store.put('df', df)
    store.put('mdf', mdf)
    store.put('par_keys', pd.Series(list(par_keys)))
    store.close()

    if options.call_plugins:
        if options.plugin_mod is not None:
            plugin_mod = import_file(options.plugin_mod)

        else:
            plugin_mod = scoop_mod

        if hasattr(plugin_mod, 'get_plugin_info'):
            plugin_info = plugin_mod.get_plugin_info()
            output('available plugins:', [fun.__name__ for fun in plugin_info])

            if options.use_plugins is not None:
                plugin_info = [fun for fun in plugin_info
                               if fun.__name__ in options.use_plugins]

            elif options.omit_plugins is not None:
                plugin_info = [fun for fun in plugin_info
                               if fun.__name__ not in options.omit_plugins]

            data = run_plugins(plugin_info, df, options.output_dir, par_keys,
                               plugin_args=options.plugin_args)
            output('plugin data keys:')
            output(data.keys())

        else:
            output('no get_plugin_info() in {}'.format(plugin_mod.__name__))

    if options.shell:
        from soops.base import shell; shell()

def main():
    options = parse_args()
    return scoop_outputs(options)

if __name__ == '__main__':
    sys.exit(main())
