def format_float(val, prec, replace_dot=True):
    fmt = '{{:.{}e}}'.format(prec)
    aux = fmt.format(val)
    if replace_dot:
        return aux.replace('.', ':') # Make LaTeX happy.

    else:
        return aux

def format_float_latex(val, prec):
    if val == 0.0:
        return '0'

    else:
        fmt = '{{:.{}e}}'.format(prec)
        sval = fmt.format(val)
        iexp = list(map(float, sval.split('e')))
        iexp[1] = int(iexp[1])
        return r'${} \cdot 10^{{{}}}$'.format(*iexp)
