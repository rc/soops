from .base import (output, ordered_iteritems, import_file, shell, debug,
                   debug_on_error, run_command, Struct)
from .cliargs import build_arg_parser
from .ioutils import ensure_path, fix_path, locate_files, save_options
from .parsing import parse_as_list, parse_as_dict
from .timing import get_timestamp, Timer
from .version import __version__

def test(*args):
    """
    Run all the package tests.

    Equivalent to running ``pytest soops/tests/`` in the base directory of
    soops. Allows an installed version of soops to be tested.

    To test an installed version of soops use

    .. code-block:: bash

       $ python -c "import soops; soops.test()"

    Parameters
    ----------
    *args : positional arguments
        Arguments passed to pytest.
    """
    import os
    import pytest  # pylint: disable=import-outside-toplevel

    path = os.path.join(os.path.split(__file__)[0], 'tests')
    return pytest.main(args=[path] + list(args))
