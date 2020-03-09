import os
import sys

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
        return [key.rjust(num) + ': ' + repr(val)
                for key, val in sorted(self.items())]

class Struct(AttrDict):
    pass

class Output(Struct):
    """
    A class that provides output (print) functions.
    """

    def __init__(self, prefix, filename=None, quiet=False, combined=False,
                 append=False, **kwargs):
        Struct.__init__(self, filename=filename, output_dir=None, **kwargs)
        if isinstance(filename, str):
            self.output_dir = os.path.dirname(filename)

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

def ordered_iteritems(adict):
    keys = sorted(adict.keys())
    for key in keys:
        yield key, adict[key]

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

def python_shell(frame=0):
    import code
    frame = sys._getframe(frame+1)
    code.interact(local=frame.f_locals)

def ipython_shell(frame=0):
    from IPython.terminal.embed import InteractiveShellEmbed
    ipshell = InteractiveShellEmbed()

    ipshell(stack_depth=frame+1)

def shell(frame=0):
    """
    Embed an IPython (if available) or regular Python shell in the given frame.
    """
    try:
        ipython_shell(frame=frame+2)

    except ImportError:
        python_shell(frame=frame+1)
