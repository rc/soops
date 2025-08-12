#!/usr/bin/env python
"""
Scoop output files.
"""
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import os.path as op
import glob
from datetime import datetime
import warnings

import numpy as np
import pandas as pd

from soops.base import output, product, flatten_dict, import_file, Struct
from soops.parsing import parse_as_dict, parse_as_list
from soops.ioutils import load_options, locate_files, ensure_path

def load_array(filename, key='array', columns=None, load_kwargs={}, rdata=None):
    is_npy = filename.endswith('.npy')
    is_npz = filename.endswith('.npz')
    is_txt = not (is_npy or is_npz)
    if is_txt:
        arr = np.loadtxt(filename, **load_kwargs)

    else:
        arr = np.load(filename, **load_kwargs)

    if is_txt or is_npy:
        if columns is None:
            out = {key : arr}

        elif arr.shape[-1] == len(columns):
            out = {key : arr[..., ic] for ic, key in enumerate(columns)}

    else:
        if columns is None:
            out = {key : val for key, val in arr.items()}

        else:
            out = {key : arr[key] for key in columns}

        arr.close()

    return out

def load_csv(filename, orient='list', rdata=None):
    df = pd.read_csv(filename)
    return df.to_dict(orient=orient)

def load_soops_parameters(filename, orient='list', rdata=None):
    return load_csv(filename, orient='index')[0]

def split_options(options, split_keys, recur=False):
    if not isinstance(split_keys, dict):
        split_keys = {key : None for key in split_keys}

    new_options = options.copy()
    for okey, nkeys in split_keys.items():
        vals = new_options.pop(okey)
        if nkeys is None:
            if recur:
                new_options.update(flatten_dict(vals, prefix=okey + '__',
                                                sep='__'))

            else:
                new_options.update({okey + '__' + key: val
                                    for key, val in vals.items()})

        else:
            new_options.update({okey + '__' + nkeys[ii]: val
                                for ii, val in enumerate(vals)})

    return new_options

def load_split_options(filename, split_keys=None, recur=False, rdata=None):
    options = load_options(filename)
    if split_keys is not None:
        options = split_options(options, split_keys=split_keys, recur=recur)
    return options

def filter_dict(data, key_prefix, strip_prefix=True):
    """
    Filter a subset of `data` with keys starting with `key_prefix`.
    """
    out = Struct((key.replace(key_prefix, '', 1) if strip_prefix else key, val)
                 for key, val in data.items()
                 if key.startswith(key_prefix))
    return out

def apply_scoops(info, directories, debug_mode=False):
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

            rdata = {'rdir' : rdir.replace(home, '~'), 'rfiles' : []}
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
                if not op.exists(path):
                    paths = list(locate_files(path))
                    output('expanded:', [path.replace(rdir, '<rdir>')
                                         for path in paths])
                    if len(paths) == 0:
                        paths = None

                else:
                    paths = None

                try:
                    if paths is None:
                        out = fun(path, rdata=rdata)

                    else:
                        out = fun(paths, rdata=rdata)

                except KeyboardInterrupt:
                    raise

                except Exception as exc:
                    output('- failed with:')
                    output(exc)
                    if debug_mode: raise
                    continue

                else:
                    if out is None:
                        output('- nothing returned!')
                        out = {}

                    if paths is None:
                        paths = [path]

                    rdata['rfiles'].append(filename)
                    mtimes = []
                    for path in paths:
                        try:
                            mtime = datetime.fromtimestamp(op.getmtime(path))

                        except FileNotFoundError:
                            mtime = np.nan

                        mtimes.append(mtime)

                    rmetadata.update({
                        'data_row' : len(data),
                        'data_columns' : tuple(out.keys()),
                        'filename' : path,
                        'filenames' : paths,
                        'mtimes' : mtimes,
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
            vals = sorted(df[col].unique().tolist())

        except TypeError:
            _, ir = np.unique([str(ii) for ii in df[col]], return_index=True)
            vals = df.loc[df.index[ir], col].tolist() # np.unique() sorts.

        uniques[col] = vals

    return uniques

def iter_uniques(df, columns, uniques):
    pars = [uniques[col] for col in columns]
    for ii, vals in enumerate(product(*pars)):
        selection = Struct(dict(zip(columns, vals)))
        sdf = df.loc[(df[columns] == vals).all(axis=1)]

        if not len(sdf): continue

        yield ii, selection, sdf

def init_plugin_data(df, par_keys, output_dir, store_filename):
    par_uniques = get_uniques(df, par_keys)
    multi_par_keys = [key for key, vals in par_uniques.items()
                      if len(vals) > 1]
    multi_par_uniques = {key : par_uniques[key] for key in multi_par_keys}
    data = Struct(par_keys=par_keys,
                  multi_par_keys=multi_par_keys,
                  par_uniques=par_uniques,
                  multi_par_uniques=multi_par_uniques,
                  output_dir=output_dir,
                  store_filename=store_filename)
    return data

def run_plugins(info, df, output_dir, par_keys, store_filename,
                plugin_args=None):
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
    data = init_plugin_data(df, par_keys, output_dir, store_filename)
    for fun in info:
        output('running {}()...'.format(fun.__name__))
        wfun = wrap_fun(fun)
        _data = wfun(df, data=data)
        data = _data if _data is not None else data
        output('...done')

    return data

def write_results(results_filename, df, mdf, par_keys):
    with pd.HDFStore(results_filename, mode='w') as store:
        store.put('df', df)
        store.put('mdf', mdf)
        store.put('par_keys', pd.Series(list(par_keys)))

helps = {
    'sort' : 'column keys for sorting of DataFrame rows',
    'filter' : 'use only DataFrame rows with given files successfully scooped',
    'no_plugins' : 'do not call post-processing plugins',
    'use_plugins' : 'use only the named plugins (no effect with --no-plugins)',
    'omit_plugins' : 'omit the named plugins (no effect with --no-plugins)',
    'plugin_mod' :
    'if given, the module that has get_plugin_info() instead of scoop_mod',
    'plugin_args' :
    """optional arguments passed to plugins given as plugin_name={key1=val1,
       key2=val2, ...}, ...""",
    'results' : 'results file name [default: <output_dir>/results.h5]',
    'no_csv' : 'do not save results as CSV (use only HDF5)',
    'reuse' : 'reuse previously scooped results file',
    'write' : 'write results files even when results were loaded using '
    '--reuse option',
    'write_after_plugins' :
    """write the pandas HDF5 results file again after plugins were applied""",
    'shell' : 'run ipython shell after all computations',
    'debug' : 'automatically start debugger when an exception is raised',
    'output_dir' : 'output directory [default: %(default)s]',
    'scoop_mod' : 'the importable script/module with get_scoop_info()',
    'directories' :
    """results directories. On "Argument list too long" system error,
       enclose the directories matching pattern in "", it will be expanded
       using glob.glob().""",
}

def parse_args(args=None):
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-s', '--sort', metavar='column[,column,...]',
                        action='store', dest='sort',
                        default=None, help=helps['sort'])
    parser.add_argument('--filter', metavar='filename[,filename,...]',
                        action='store', dest='filter',
                        default=None, help=helps['filter'])
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
    parser.add_argument('--results', metavar='filename',
                        action='store', dest='results',
                        default=None, help=helps['results'])
    parser.add_argument('--no-csv',
                        action='store_false', dest='save_csv',
                        default=True, help=helps['no_csv'])
    parser.add_argument('-r', '--reuse',
                        action='store_true', dest='reuse',
                        default=False, help=helps['reuse'])
    parser.add_argument('--write',
                        action='store_true', dest='write',
                        default=False, help=helps['write'])
    parser.add_argument('--write-after-plugins',
                        action='store_true', dest='write_after_plugins',
                        default=False, help=helps['write_after_plugins'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('--debug',
                        action='store_true', dest='debug',
                        default=False, help=helps['debug'])
    parser.add_argument('-o', '--output-dir', metavar='path',
                        action='store', dest='output_dir',
                        default='.', help=helps['output_dir'])
    parser.add_argument('scoop_mod', help=helps['scoop_mod'])
    parser.add_argument('directories', nargs='+', help=helps['directories'])
    options = parser.parse_args(args=args)

    if options.debug:
        from soops import debug_on_error; debug_on_error()

    options.sort = parse_as_list(options.sort)

    if options.filter is not None:
        options.filter = set(parse_as_list(options.filter, free_word=True))

    if options.use_plugins is not None:
        options.use_plugins = parse_as_list(options.use_plugins)

    if options.omit_plugins is not None:
        options.omit_plugins = parse_as_list(options.omit_plugins)

    if options.plugin_args is not None:
        options.plugin_args = parse_as_dict(options.plugin_args)

    if options.results is None:
        options.results = op.join(options.output_dir, 'results.h5')

    directories = []
    for directory in options.directories:
        expanded = glob.glob(directory + op.sep)
        directories.extend(expanded)
    options.directories = directories

    return options

def scoop_outputs(options):
    output.prefix = ''

    scoop_mod = import_file(options.scoop_mod)

    if (not options.reuse
        or not (op.exists(options.results) and op.isfile(options.results))):
        new_results = True

        if hasattr(scoop_mod, 'get_scoop_info'):
            scoop_info = scoop_mod.get_scoop_info()

        else:
            output('no get_scoop_info() in {}, exiting'
                   .format(options.scoop_mod))
            return

        df, mdf, par_keys = apply_scoops(scoop_info, options.directories,
                                         options.debug)

        if options.filter is not None:
            idf = [ii for ii, rfiles in df['rfiles'].items()
                   if options.filter.intersection(rfiles)]
            df = df.iloc[idf]
            df.index = np.arange(len(df))

            imdf = [ii for ii, data_row in mdf['data_row'].items()
                    if data_row in idf]
            mdf = mdf.iloc[imdf]
            mdf.index = np.arange(len(mdf))

    else:
        new_results = False
        with pd.HDFStore(options.results, mode='r') as store:
            df = store.get('df')
            mdf = store.get('mdf')
            par_keys = set(store.get('par_keys').to_list())
            std_keys = ('/df', '/mdf', '/par_keys')
            user_keys = set(store.keys()).difference(std_keys)
            output('user data:')
            output(user_keys)

    output('data keys:')
    output(df.keys())
    output('metadata keys:')
    output(mdf.keys())

    if options.sort:
        df = df.sort_values(options.sort)
        df.index = np.arange(len(df))

    warnings.simplefilter(action='ignore',
                          category=pd.errors.PerformanceWarning)

    results_filename = options.results
    ensure_path(results_filename)
    if new_results or options.write:
        write_results(results_filename, df, mdf, par_keys)

        if options.save_csv:
            filename = op.join(options.output_dir, 'results.csv')
            df.to_csv(filename)

        filename = op.join(options.output_dir, 'results-meta.csv')
        mdf.to_csv(filename)

    if options.call_plugins:
        if options.plugin_mod is not None:
            plugin_mod = import_file(options.plugin_mod)

        else:
            plugin_mod = scoop_mod

        if hasattr(plugin_mod, 'get_plugin_info'):
            plugin_info = plugin_mod.get_plugin_info()
            fun_names = [fun.__name__ for fun in plugin_info]
            output('available plugins:', fun_names)

            if options.use_plugins is not None:
                aux = []
                for fun_name in options.use_plugins:
                    try:
                        ii = fun_names.index(fun_name)

                    except ValueError:
                        raise ValueError('unknown plugin! ({})'
                                         .format(fun_name))

                    aux.append(plugin_info[ii])

                plugin_info = aux

            elif options.omit_plugins is not None:
                plugin_info = [fun for fun in plugin_info
                               if fun.__name__ not in options.omit_plugins]

            data = run_plugins(plugin_info, df, options.output_dir, par_keys,
                               results_filename,
                               plugin_args=options.plugin_args)
            output('plugin data keys:')
            output(data.keys())

        else:
            output('no get_plugin_info() in {}'.format(plugin_mod.__name__))

        if options.write_after_plugins:
            write_results(results_filename, df, mdf, par_keys)

    if options.shell:
        from soops.base import shell; shell()

def main():
    options = parse_args()
    return scoop_outputs(options)

if __name__ == '__main__':
    sys.exit(main())
