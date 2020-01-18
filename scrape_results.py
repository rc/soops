#!/usr/bin/env python
"""
Scrape results files.
"""
from __future__ import absolute_import
from __future__ import print_function
import os.path as op
from argparse import ArgumentParser, RawDescriptionHelpFormatter

import numpy as np
import pandas as pd

from sfepy.base.base import output, import_file, Struct
from sfepy.base.conf import dict_from_string
from sfepy.base.ioutils import locate_files, ensure_path

def load_array(filename, key='array', load_kwargs={}, rdata=None):
    arr = np.loadtxt(filename, **load_kwargs)
    return {key : arr}

def load_options(filename):
    with open(filename, 'r') as fd:
        data = fd.readlines()

    raw_options = [ii.strip() for ii in data[8:]]
    options = {}
    for opt in raw_options:
        aux = opt.split(':')
        key = aux[0].strip()
        sval = ':'.join([ii.strip() for ii in aux[1:]])
        try:
            val = dict_from_string(sval)
        except:
            try:
                val = eval(sval)
            except:
                val = sval

        options[key] = val

    return options

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

def load_split_options(filename, split_keys, rdata=None):
    _options = load_options(filename)
    options = split_options(_options, split_keys)
    return options

def parse_log_info(log, info, key, rdata=None):
    plog = {}
    offset = 0
    for group, val in info.items():
        pinfo = Struct(xlabel=val[0], ylabel=val[1], yscale=val[2],
                       names=val[3], plot_kwargs=val[4], data={})
        for name in pinfo.names:
            if name in log:
                xs, ys, vlines = log[name]

            else:
                xs, ys, vlines = log[pinfo.names.index(name) + offset]

            pinfo.data[name] = (xs, ys)

        plog[group] = pinfo
        offset += len(pinfo.names)

    return {key : plog}

def scrape_results(script, directories):
    script_mod = import_file(script)

    info = script_mod.get_scrape_info()
    if not len(info):
        return pd.DataFrame({})

    data = []
    for idir, directory in enumerate(directories):
        output(idir, directory)
        name0 = info[0][0]
        filenames = locate_files(name0, directory)
        for ir, filename in enumerate(filenames):
            rdir = op.dirname(filename)
            output(ir, rdir)

            rdata = {'rdir' : rdir}
            for filename, fun in info:
                output(filename)
                try:
                    out = fun(op.join(rdir, filename), rdata=rdata)

                except KeyboardInterrupt:
                    raise

                except Exception as exc:
                    output('- failed with:')
                    output(exc)
                    continue

                finally:
                    rdata.update(out)

            data.append(pd.Series(rdata))

    df = pd.DataFrame(data)
    return df

helps = {
    'sort' : 'column keys for sorting of DataFrame rows',
    'results' : 'reuse previously scraped results file',
    'shell' : 'run ipython shell after all computations',
    'output_dir' : 'output directory [default: %(default)s]',
    'script' : 'the script that was run to generate the results',
    'directories' : 'results directories',
}

def main():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-s', '--sort', metavar='column[,columns,...]',
                        action='store', dest='sort',
                        default=None, help=helps['sort'])
    parser.add_argument('-r', '--results', metavar='filename',
                        action='store', dest='results',
                        default=None, help=helps['results'])
    parser.add_argument('--shell',
                        action='store_true', dest='shell',
                        default=False, help=helps['shell'])
    parser.add_argument('-o', '--output-dir', metavar='path',
                        action='store', dest='output_dir',
                        default='.', help=helps['output_dir'])
    parser.add_argument('script', help=helps['script'])
    parser.add_argument('directories', nargs='+', help=helps['directories'])
    options = parser.parse_args()

    options.sort = options.sort.split(',') if options.sort is not None else []

    output.prefix = ''

    if (options.results is None
        or not (op.exists(options.results) and op.isfile(options.results))):
        df = scrape_results(options.script, options.directories)

    else:
        df = pd.read_hdf(options.results, 'results')

    if options.sort:
        df = df.sort_values(options.sort)
        df.index = np.arange(len(df))

    output(df)

    filename = op.join(options.output_dir, 'results.csv')
    ensure_path(filename)
    df.to_csv(filename)
    filename = op.join(options.output_dir, 'results.h5')
    df.to_hdf(filename, 'results')

    if options.shell:
        from sfepy.base.base import shell; shell()

if __name__ == '__main__':
    main()
