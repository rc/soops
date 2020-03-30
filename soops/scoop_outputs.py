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
from soops.parsing import parse_as_list
from soops.ioutils import load_options, locate_files, ensure_path

def load_array(filename, key='array', load_kwargs={}, rdata=None):
    arr = np.loadtxt(filename, **load_kwargs)
    return {key : arr}

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
        return pd.DataFrame({}), pd.DataFrame({})

    data = []
    metadata = []
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
            for filename, fun in info:
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

            rdata['time'] = datetime.utcnow()

            data.append(pd.Series(rdata))

    df = pd.DataFrame(data)
    mdf = pd.DataFrame(metadata)
    return df, mdf

def get_parametric_columns(df):
    par_cols = []
    omit = ('rdir', 'time')
    for ic, col in enumerate(df.columns):
        try:
            num = df[col].nunique()

        except TypeError:
            num = len(np.unique([str(ii) for ii in df[col]]))

        if num > 1 and col not in omit:
            par_cols.append(col)

    return par_cols

def get_uniques(df, columns):
    uniques = {}
    for col in sorted(columns):
        try:
            vals = df[col].unique()

        except TypeError:
            vals = np.unique([str(ii) for ii in df[col]])

        uniques[col] = sorted(vals)

    return uniques

def get_parametric_uniques(df, omit=None):
    if omit is None: omit = {}

    par_cols = get_parametric_columns(df)
    uniques = get_uniques(df, [col for col in par_cols if col not in omit])

    return uniques

def run_plugins(info, df, output_dir):
    if not len(info):
        return

    output('run plugins:')
    par_cols = get_parametric_columns(df)
    data = Struct(par_cols=par_cols, output_dir=output_dir)
    for fun in info:
        output('running {}()...'.format(fun.__name__))
        data = fun(df, data=data)
        output('...done')

    return data

helps = {
    'sort' : 'column keys for sorting of DataFrame rows',
    'results' : 'reuse previously scooped results file',
    'no_plugins' : 'do not call post-processing plugins',
    'use_plugins' : 'use only the named plugins (no effect with --no-plugins)',
    'omit_plugins' : 'omit the named plugins (no effect with --no-plugins)',
    'shell' : 'run ipython shell after all computations',
    'output_dir' : 'output directory [default: %(default)s]',
    'script' : 'the script that was run to generate the results',
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
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('-o', '--output-dir', metavar='path',
                        action='store', dest='output_dir',
                        default='.', help=helps['output_dir'])
    parser.add_argument('script', help=helps['script'])
    parser.add_argument('directories', nargs='+', help=helps['directories'])
    options = parser.parse_args(args=args)

    options.sort = parse_as_list(options.sort)

    if options.use_plugins is not None:
        options.use_plugins = parse_as_list(options.use_plugins)

    if options.omit_plugins is not None:
        options.omit_plugins = parse_as_list(options.omit_plugins)

    return options

def scoop_outputs(options):
    output.prefix = ''

    script_mod = import_file(options.script)

    if (options.results is None
        or not (op.exists(options.results) and op.isfile(options.results))):

        if hasattr(script_mod, 'get_scoop_info'):
            scoop_info = script_mod.get_scoop_info()

        else:
            output('no get_scoop_info() in {} script'.format(options.script))
            return

        df, mdf = apply_scoops(scoop_info, options.directories)

    else:
        df = pd.read_hdf(options.results, 'df')
        mdf = pd.read_hdf(options.results, 'mdf')

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
    store.close()

    if options.call_plugins:
        if hasattr(script_mod, 'get_plugin_info'):
            plugin_info = script_mod.get_plugin_info()
            output('available plugins:', [fun.__name__ for fun in plugin_info])

            if options.use_plugins is not None:
                plugin_info = [fun for fun in plugin_info
                               if fun.__name__ in options.use_plugins]

            elif options.omit_plugins is not None:
                plugin_info = [fun for fun in plugin_info
                               if fun.__name__ not in options.omit_plugins]

            run_plugins(plugin_info, df, options.output_dir)

        else:
            output('no get_plugin_info() in {} script'.format(options.script))

    if options.shell:
        from soops.base import shell; shell()

def main():
    options = parse_args()
    return scoop_outputs(options)

if __name__ == '__main__':
    sys.exit(main())
