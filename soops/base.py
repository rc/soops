import os
import sys
import subprocess
import itertools

class AttrDict(dict):
    """
    A dict with the attribute access to its items.
    """

    def __getattr__(self, name):
        try:
            return self[name]

        except KeyError:
            raise AttributeError(name)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __str__(self):
        if self.keys():
            return '\n'.join(
                [self.__class__.__name__ + ':'] +
                self.format_items()
            )

        else:
            return self.__class__.__name__

    def __repr__(self):
        return self.__class__.__name__

    def __dir__(self):
        return list(self.keys())

    def copy(self):
        return type(self)(self)

    def format_items(self):
        num = max(map(len, list(self.keys()))) + 1
        return [f'{key.rjust(num)}: {val}'
                for key, val in sorted(self.items())]

class Struct(AttrDict):

    def format_items(self):
        num = max(map(len, list(self.keys()))) + 1
        return [f'{key.rjust(num)}: {val}' if not isinstance(val, Struct)
                else f'{key.rjust(num)}: {repr(val)}'
                for key, val in sorted(self.items())]

class Output(Struct):
    """
    A class that provides output (print) functions.
    """

    def __init__(self, prefix, filename=None, quiet=False, combined=False,
                 append=False, **kwargs):
        Struct.__init__(self, filename=filename, output_dir=None, **kwargs)
        self.prefix = prefix

        self.set_output(filename=filename, quiet=quiet,
                        combined=combined, append=append)

    def __call__(self, *argc, **argv):
        """
        Call `self.output_function()`.

        Parameters
        ----------
        argc : positional arguments
            The values to print.
        argv : keyword arguments
            The arguments to control the output behaviour. Supported keywords
            are listed below.
        verbose : bool (in **argv)
            No output if False.
        """
        verbose = argv.get('verbose', True)
        if verbose:
            self.output_function(*argc, **argv)

    def get_prefix(self):
        if len(self.prefix):
            return self.prefix + ' ' + ('  ' * self.level)

        else:
            return self.prefix + ('  ' * self.level)

    def set_output(self, filename=None, quiet=False, combined=False,
                   append=False):
        """
        Set the output mode.

        If `quiet` is `True`, no messages are printed to screen. If
        simultaneously `filename` is not `None`, the messages are logged
        into the specified file.

        If `quiet` is `False`, more combinations are possible. If
        `filename` is `None`, output is to screen only, otherwise it is
        to the specified file. Moreover, if `combined` is `True`, both
        the ways are used.

        Parameters
        ----------
        filename : str or file object
            Print messages into the specified file.
        quiet : bool
            Do not print anything to screen.
        combined : bool
            Print both on screen and into the specified file.
        append : bool
            Append to an existing file instead of overwriting it. Use with
            `filename`.
        """
        if not isinstance(filename, str):
            # filename is a file descriptor.
            append = True

        self.level = 0

        def output_none(*argc, **argv):
            pass

        def output_screen(*argc, **argv):
            format = '%s' + ' %s' * (len(argc) - 1)
            msg = format % argc

            if msg.startswith('...'):
                self.level -= 1

            print(self.get_prefix() + msg)

            if msg.endswith('...'):
                self.level += 1

        def print_to_file(filename, msg):
            if isinstance(filename, str):
                fd = open(filename, 'a')

            else:
                fd = filename

            print(self.get_prefix() + msg, file=fd)

            if isinstance(filename, str):
                fd.close()

            else:
                fd.flush()

        def output_file(*argc, **argv):
            format = '%s' + ' %s' * (len(argc) - 1)
            msg = format % argc

            if msg.startswith('...'):
                self.level -= 1

            print_to_file(filename, msg)

            if msg.endswith('...'):
                self.level += 1

        def output_combined(*argc, **argv):
            format = '%s' + ' %s' * (len(argc) - 1)
            msg = format % argc

            if msg.startswith('...'):
                self.level -= 1

            print(self.get_prefix() + msg)

            print_to_file(filename, msg)

            if msg.endswith('...'):
                self.level += 1

        def reset_file(filename):
            if isinstance(filename, str):
                self.output_dir = os.path.dirname(filename)
                if self.output_dir and not os.path.exists(self.output_dir):
                    os.makedirs(self.output_dir)

                fd = open(filename, 'w')
                fd.close()

            else:
                raise ValueError('cannot reset a file object!')

        if quiet is True:
            if filename is not None:
                if not append:
                    reset_file(filename)

                self.output_function = output_file

            else:
                self.output_function = output_none

        else:
            if filename is None:
                self.output_function = output_screen

            else:
                if not append:
                    reset_file(filename)

                if combined:
                    self.output_function = output_combined

                else:
                    self.output_function = output_file

    def get_output_function(self):
        return self.output_function

output = Output('soops:')

def product(*seqs, contracts=None):
    """
    Like `itertools.product()`, but loops in contracts vary in lockstep.
    """
    if contracts is None:
        yield from itertools.product(*seqs)
        return

    ifollowing = [[ii for ii in contract[1:]] for contract in contracts]

    aux = set(sum(ifollowing, []))
    pindices, pseqs = zip(*itertools.filterfalse(
        lambda x: x[0] in aux,
        [(ip, enumerate(seq)) for ip, seq in enumerate(seqs)]
    ))

    for pout in itertools.product(*pseqs):
        out = [0] * len(seqs)
        for ip, ii in enumerate(pindices):
            out[ii] = pout[ip][1]

        for ic, contract in enumerate(contracts):
            ii = pout[pindices.index(contract[0])][0]
            for ik in ifollowing[ic]:
                val = seqs[ik][ii]
                out[ik] = val

        yield out

def get_default(arg, default, msg_if_none=None):
    out = arg if arg is not None else default

    if (out is None) and (msg_if_none is not None):
        raise ValueError(msg_if_none)

    return out

def ordered_iteritems(adict):
    keys = sorted(adict.keys())
    for key in keys:
        yield key, adict[key]

def flatten_dict(adict, prefix='', sep='__'):
    out = {}
    for key, val in adict.items():
        new_key = prefix + key
        if isinstance(val, dict):
            out.update(flatten_dict(val, prefix=new_key + sep))

        else:
            out[new_key] = val

    return out

def import_file(filename, package_name=None, can_reload=True):
    """
    Import a file as a module. The module is explicitly reloaded to
    prevent undesirable interactions.
    """
    base_dir = os.path.dirname(os.path.normpath(os.path.realpath(__file__)))
    top_dir = os.path.normpath(os.path.join(base_dir, '..'))
    path = os.path.dirname(os.path.normpath(os.path.realpath(filename)))

    if (package_name is None) and (top_dir == path[:len(top_dir)]):
        package_name = path[len(top_dir) + 1:].replace(os.sep, '.')
        path = top_dir

    if not path in sys.path:
        sys.path.append(path)
        remove_path = True

    else:
        remove_path = False

    name = os.path.splitext(os.path.basename(filename))[0]

    if package_name:
        mod = __import__('.'.join((package_name, name)), fromlist=[name])

    else:
        mod = __import__(name)

    if (name in sys.modules) and can_reload:
        import importlib
        importlib.reload(mod)

    if remove_path:
        sys.path.remove(path)

    return mod

def is_derived_class(cls, parent):
    return issubclass(cls, parent) and (cls is not parent)

def find_subclasses(context, classes, omit_unnamed=False, name_attr='name'):
    """
    Find subclasses of the given classes in the given context.
    """
    table = {}
    for key, var in context.items():
        try:
            for cls in classes:
                if is_derived_class(var, cls):
                    if hasattr(var, name_attr):
                        key = getattr(var, name_attr)
                        if omit_unnamed and not key:
                            continue

                    elif omit_unnamed:
                        continue

                    else:
                        key = var.__class__.__name__

                    table[key] = var
                    break

        except TypeError:
            pass

    return table

def load_classes(filenames, classes, package_name=None, ignore_errors=False,
                 name_attr='name'):
    """
    For each filename in filenames, load all subclasses of classes listed.
    """
    table = {}
    for filename in filenames:
        if not ignore_errors:
            mod = import_file(filename, package_name=package_name,
                              can_reload=False)

        else:
            try:
                mod = import_file(filename, package_name=package_name,
                                  can_reload=False)

            except:
                output('WARNING: module %s cannot be imported!' % filename)
                output('reason:\n', sys.exc_info()[1])
                continue

        table.update(find_subclasses(vars(mod), classes, omit_unnamed=True,
                                     name_attr=name_attr))

    return table

def python_shell(frame=0):
    import code
    frame = sys._getframe(frame+1)
    code.interact(local=frame.f_locals)

def ipython_shell(frame=0, magics=None):
    from IPython.terminal.embed import InteractiveShellEmbed
    from IPython.paths import get_ipython_dir
    from traitlets.config import Config
    from traitlets.config.loader import PyFileConfigLoader

    # Locate the IPython configuration directory.
    ipython_dir = get_ipython_dir()
    config_file = os.path.join(ipython_dir, 'profile_default',
                               'ipython_config.py')

    # Load the user configuration if it exists.
    config = Config()
    if os.path.exists(config_file):
        loader = PyFileConfigLoader(config_file)
        config.update(loader.load_config())

    # Start the embedded IPython shell with the configuration.
    ipshell = InteractiveShellEmbed(config=config)

    if magics is not None:
        # Run line magic functions if any.
        for magic in magics:
            if isinstance(magic, str):
                magic = (magic, '')

            ipshell.run_line_magic(*magic)

    ipshell(stack_depth=frame+1)

def shell(frame=0, magics=('matplotlib',)):
    """
    Embed an IPython (if available) or regular Python shell in the given frame.
    """
    try:
        ipython_shell(frame=frame+2, magics=magics)

    except ImportError:
        python_shell(frame=frame+1)

def debug(frame=None, frames_back=1):
    """
    Start debugger on line where it is called, roughly equivalent to::

        import pdb; pdb.set_trace()

    First, this function tries to start an `IPython`-enabled
    debugger using the `IPython` API.

    When this fails, the plain old `pdb` is used instead.

    With IPython, one can say in what frame the debugger can stop.
    """
    try:
        import IPython

    except ImportError:
        import pdb
        pdb.set_trace()

    else:
        old_excepthook = sys.excepthook

        if IPython.__version__ >= '0.11':
            from IPython.core.debugger import Pdb

            try:
                ip = get_ipython()

            except NameError:
                from IPython.terminal.embed import InteractiveShellEmbed
                ip = InteractiveShellEmbed()

            colors = ip.colors

        else:
            from IPython.Debugger import Pdb
            from IPython.Shell import IPShell
            from IPython import ipapi

            ip = ipapi.get()
            if ip is None:
                IPShell(argv=[''])
                ip = ipapi.get()

            colors = ip.options.colors

        sys.excepthook = old_excepthook

        if frame is None:
            frame = sys._getframe(frames_back)

        Pdb(colors).set_trace(frame)

def debug_on_error():
    """
    Start debugger at the line where an exception was raised.
    """
    try:
        from IPython.core import ultratb

        except_hook = ultratb.FormattedTB(mode='Verbose',
                                          color_scheme='Linux', call_pdb=1)

    except ImportError:
        def except_hook(etype, value, tb):
            if hasattr(sys, 'ps1') or not sys.stderr.isatty():
                # We are in interactive mode or we don't have a tty-like
                # device, so we call the default hook.
                sys.__excepthook__(etype, value, tb)

            else:
                import traceback, pdb
                # We are NOT in interactive mode, print the exception...
                traceback.print_exception(etype, value, tb)
                print()
                # ...then start the debugger in post-mortem mode.
                pdb.post_mortem(tb)

    sys.excepthook = except_hook

def run_command(command, filename=None, repeat=1, silent=False):
    """
    Run `command` with `filename` positional argument in the directory of the
    `filename`. If `filename` is not given, run only the command.
    """
    if filename is not None:
        fdir = os.path.dirname(os.path.abspath(filename))
        fname = os.path.basename(filename)

        cmd = command + ' ' + fname

    else:
        fdir = None
        cmd = command

    status = 0
    for ii in range(repeat):
        if silent:
            with open(os.devnull, 'w') as devnull:
                st = subprocess.call(cmd.split(), cwd=fdir,
                                     stdout=devnull, stderr=devnull)

        else:
            st = subprocess.call(cmd.split(), cwd=fdir)

        status = status or st

    return status
