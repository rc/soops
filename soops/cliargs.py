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
