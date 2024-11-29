def build_arg_parser(parser, arg_conf, is_help=True):
    """
    Build an argument parser according to `arg_conf` using argparse.

    Items of `arg_conf` have the form ``key: (value, 'help message')``.

    The key is prefixed with '--' and '_' -> '-' to create the option name.

    The values can be:
    - False -> the action is 'store_true'
    - True -> the action is 'store_false', the option is prefixed with 'no-'
    - value -> the action is 'store', the type is inferred
    - tuple of values -> the type is inferred from the first item, choices are
      created
    - [value0, value1] -> the type is inferred from value1, the default is
      value0, a typical example is [None, 0.0]
    """
    helps = {}

    dhelp = ' [default: %(default)s]'
    for key, (val, msg) in arg_conf.items():
        helps[key] = msg
        action = 'store'
        vtype = type(val)
        choices = None
        option = key
        if val is True:
            action = 'store_false'
            option = 'no_' + key

        elif val is False:
            action = 'store_true'

        elif isinstance(val, tuple):
            choices = val
            vtype = type(val[0])
            val = val[0]

        elif isinstance(val, list):
            vtype = type(val[1])
            val = val[0]

        if action == 'store':
            helps[key] += dhelp
            parser.add_argument('--' + option.replace('_', '-'),
                                type=vtype,
                                action=action, dest=key, choices=choices,
                                default=val, help=helps[key])
        else:
            parser.add_argument('--' + option.replace('_', '-'),
                                action=action, dest=key,
                                default=val, help=helps[key])

def build_opt_args(arg_conf,
                   omit=('--output-dir', '--plot-rc-params',
                         '--show', '--silent', '--shell', '--debug'),
                   add_to_omit=None):
    """
    Build a list of optional arguments for `soops-run`. See
    :func:`build_arg_parser()` for `arg_conf` explanation, though only its keys
    are used here. Arguments in `omit` and `add_to_omit` are skipped.
    """
    if add_to_omit is not None:
        omit = omit + tuple(add_to_omit)

    out = []
    for key in arg_conf.keys():
        opt = '--' + key.replace('_', '-')
        if opt not in omit:
            out.append(f'{opt}={{{opt}}}')

    return out
