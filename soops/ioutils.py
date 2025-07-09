import sys
import os
import fnmatch
import tempfile
import shutil
import glob
import subprocess
from collections.abc import Iterable

from soops.base import output, ordered_iteritems
from soops.parsing import parse_as_dict

def ensure_path(filename):
    """
    Check if path to `filename` exists and if not, create the necessary
    intermediate directories.
    """
    dirname = os.path.dirname(filename)
    if dirname:
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        if not os.path.isdir(dirname):
            raise IOError('cannot ensure path for "%s"!' % filename)

def fix_path(path):
    """
    Expand user directory and make the path absolute.
    """
    return os.path.abspath(os.path.expanduser(path))

def edit_filename(filename, prefix='', suffix='', new_dir=None, new_ext=None):
    """
    Edit a file name by adding a prefix, by inserting a suffix in front of the
    extension or by replacing the extension.

    Parameters
    ----------
    filename : str
        The file name.
    prefix : str
        The prefix to be added.
    suffix : str
        The suffix to be inserted.
    new_dir : str, optional
        If not None, it replaces the path to the original file.
    new_ext : str, optional
        If not None, it replaces the original file name extension.

    Returns
    -------
    new_filename : str
        The new file name.
    """
    path, filename = os.path.split(filename)
    base, ext = os.path.splitext(filename)

    if new_ext is None:
        new_filename = prefix + base + suffix + ext

    else:
        new_filename = prefix + base + suffix + new_ext

    if new_dir is None:
        new_filename = os.path.join(path, new_filename)

    else:
        new_filename = os.path.join(new_dir, new_filename)

    return new_filename

def locate_files(pattern, root_dir=os.curdir, **kwargs):
    """
    Locate all files matching fiven filename pattern in and below
    supplied root directory.

    The `**kwargs` arguments are passed to ``os.walk()``.
    """
    dirname, pattern = os.path.split(pattern)
    if dirname:
        root_dir = os.path.join(root_dir, dirname)

    for dirpath, dirnames, filenames in os.walk(os.path.abspath(root_dir),
                                                **kwargs):
        for filename in fnmatch.filter(filenames, pattern):
            yield os.path.join(dirpath, filename)

def remove_files(root_dir, **kwargs):
    """
    Remove all files and directories in supplied root directory.

    The `**kwargs` arguments are passed to ``os.walk()``.
    """
    for dirpath, dirnames, filenames in os.walk(os.path.abspath(root_dir),
                                                **kwargs):
        for filename in filenames:
            os.remove(os.path.join(root_dir, filename))

        for dirname in dirnames:
            shutil.rmtree(os.path.join(root_dir, dirname))

def remove_files_patterns(root_dir, patterns, ignores=None,
                          verbose=False):
    """
    Remove files with names satisfying the given glob patterns in a supplied
    root directory. Files with patterns in `ignores` are omitted.
    """
    from itertools import chain

    if ignores is None: ignores = []
    for _f in chain(*[glob.glob(os.path.join(root_dir, pattern))
                      for pattern in patterns]):
        can_remove = True
        for ignore in ignores:
            if fnmatch.fnmatch(_f, os.path.join(root_dir, ignore)):
                can_remove = False
                break

        if can_remove:
            output('removing "%s"' % _f, verbose=verbose)
            os.remove(_f)

def save_options(filename, options_groups, save_command_line=True,
                 quote_command_line=False):
    """
    Save groups of options/parameters into a file.

    Each option group has to be a sequence with two items: the group name and
    the options in ``{key : value}`` form.
    """
    with open(filename, 'w') as fd:
        if save_command_line:
            fd.write('command line\n')
            fd.write('------------\n\n')
            if quote_command_line:
                fd.write(' '.join('"%s"' % ii for ii in sys.argv) + '\n')

            else:
                fd.write(' '.join(sys.argv) + '\n')

        for options_group in options_groups:
            name, options = options_group
            fd.write('\n%s\n' % name)
            fd.write(('-' * len(name)) + '\n\n')
            for key, val in ordered_iteritems(options):
                fd.write('%s: %s\n' % (key, val))

def load_options(filename):
    with open(filename, 'r') as fd:
        data = [line.strip() for line in fd.readlines()]

    for ii, line in enumerate(data):
        if (line == 'options') and data[ii+1] == '-------':
            ii += 3
            break

    raw_options = data[ii:]
    options = {}
    for opt in raw_options:
        aux = opt.split(':')
        key = aux[0].strip()
        sval = ':'.join([ii.strip() for ii in aux[1:]])
        try:
            val = parse_as_dict(sval)
        except:
            try:
                val = eval(sval)
            except:
                val = sval

        options[key] = val

    return options

def skip_lines(fd, num):
    for ii in range(num):
        line = next(fd)
    return line

def skip_lines_to(fd, key):
    while 1:
        try:
            line = next(fd)

        except StopIteration:
            return ''

        if key in dec(line):
            return line

def dec(val):
    if isinstance(val, bytes):
        return val.decode('utf-8')

    else:
        return val

def is_in_store(filename, keys):
    from pandas import HDFStore

    if not isinstance(keys, Iterable):
        keys = [keys]

    with HDFStore(filename, mode='r') as store:
        return all(key in store for key in keys)

def put_to_store(filename, key, val):
    from pandas import HDFStore

    with HDFStore(filename, mode='r+') as store:
        store.put(key, val)

def get_from_store(filename, key, default=None):
    from pandas import HDFStore

    try:
        store = HDFStore(filename, mode='r')

    except OSError:
        return default

    else:
        out = store.get(key) if key in store else default
        store.close()

    return out

def delete_from_store(filename, key):
    from pandas import HDFStore

    with HDFStore(filename, mode='r+') as store:
        store.remove(key)

def repack_store(filename):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'repack.h5')

        cmd = ('ptrepack --chunkshape=auto --propindexes {} {}'
               .format(filename, path)
               .split())

        if subprocess.call(cmd) == 0:
            shutil.move(path, filename)

        else:
            raise IOError('repack failed!')
