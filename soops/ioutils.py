import sys
import os
import fnmatch

from soops.base import ordered_iteritems
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

def locate_files(pattern, root_dir=os.curdir, **kwargs):
    """
    Locate all files matching fiven filename pattern in and below
    supplied root directory.

    The `**kwargs` arguments are passed to ``os.walk()``.
    """
    for dirpath, dirnames, filenames in os.walk(os.path.abspath(root_dir),
                                                **kwargs):
        for filename in fnmatch.filter(filenames, pattern):
            yield os.path.join(dirpath, filename)

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
        data = fd.readlines()

    raw_options = [ii.strip() for ii in data[8:]]
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
