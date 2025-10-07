def _transform_arg_conf(arg_conf):
    targ_conf = {}
    for key, conf in arg_conf.items():
        if len(conf) == 2:
            (val, msg) = conf
            extra = {}

        else:
            (val, msg, extra) = conf

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

        elif val is None: # Positional command line argument.
            action = None

        targ_conf[key] = (action, vtype, option, choices, val, msg, extra)

    return targ_conf

def build_arg_parser(parser, arg_conf, is_help=True, alternatives=None):
    """
    Build an argument parser according to `arg_conf` using argparse.

    Items of `arg_conf` have the form ``key: (value, 'help message')`` or
    ``key: (value, 'help message', dict)``, where `dict` contains additional
    arguments of `parser.add_argument()` such as `nargs` or `metavar`.

    The key is prefixed with '--' and '_' -> '-' to create the option name.

    The values can be:
    - False -> the action is 'store_true'
    - True -> the action is 'store_false', the option is prefixed with 'no-'
    - value -> the action is 'store', the type is inferred
    - tuple of values -> the type is inferred from the first item, choices are
      created
    - [value0, value1] -> the type is inferred from value1, the default is
      value0, a typical example is [None, 0.0]
    - None -> positional command line argument
    """
    dhelp = ' [default: %(default)s]'
    if alternatives is None:
        alternatives = {}

    targ_conf = _transform_arg_conf(arg_conf)
    for key, (action, vtype, option, choices,
              val, msg, extra) in targ_conf.items():
        if action is not None:
            opt_arg_strs = ['--' + option.replace('_', '-')]
            alt = alternatives.get(key, [])
            if isinstance(alt, list):
                opt_arg_strs = alt + opt_arg_strs

            elif isinstance(alt, str):
                opt_arg_strs = [alt] + opt_arg_strs

            else:
                raise ValueError('alternatives value can be either a list of'
                                 f' strings or a string! (is {alt})')

        if action == 'store':
            parser.add_argument(*opt_arg_strs,
                                type=vtype,
                                action=action, dest=key, choices=choices,
                                default=val, help=msg + dhelp, **extra)
        elif action is not None:
            parser.add_argument(*opt_arg_strs,
                                action=action, dest=key,
                                default=val, help=msg, **extra)

        else:
            parser.add_argument(option, help=msg, **extra)

def _get_opt_from_key(key):
    return '--' + key.replace('_', '-')

def build_opt_args(arg_conf,
                   omit=('--output-dir', '--plot-rc-params',
                         '--show', '--silent', '--shell', '--debug'),
                   add_to_omit=None,
                   return_defaults=False):
    """
    Build a list of optional arguments for `soops-run`. See
    :func:`build_arg_parser()` for `arg_conf` explanation. Arguments in `omit`
    and `add_to_omit` are skipped.

    Returns
    -------
    out : list or (list, dict)
        The list of optional arguments and, if `return_defaults` is True, also
        default values for options whose action is not 'store_false' or
        'store_true'.
    """
    if add_to_omit is not None:
        omit = omit + tuple(add_to_omit)

    out = []
    for key, val in arg_conf.items():
        opt = _get_opt_from_key(key)
        if (opt not in omit) and (val[0] is not None):
            if (val[0] is True) or (val[0] is False):
                out.append(f'{opt}')

            else:
                out.append(f'{opt}={{{opt}}}')

    if return_defaults:
        targ_conf = _transform_arg_conf(arg_conf)
        defaults = {_get_opt_from_key(key) : (val[4] if val[4] is not None
                                              else '@undefined')
                    for key, val in targ_conf.items()
                    if val[0] not in ('store_false', 'store_true', None)}

        out = (out, defaults)

    return out
