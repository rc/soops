from functools import partial
from math import isfinite

import numpy as np

from soops.base import output, run_command

def format_float(val, prec, replace_dot=True):
    if isinstance(prec, int):
        fmt = '{{:.{}e}}'.format(prec)

    else:
        fmt = '{{:{}}}'.format(prec)

    aux = fmt.format(val)
    if replace_dot:
        return aux.replace('.', ':') # Make LaTeX happy.

    else:
        return aux

def format_float_latex(val, prec, in_math=False):
    if val == 0.0:
        return '0'

    elif isfinite(val):
        if isinstance(prec, int):
            fmt = '{{:.{}e}}'.format(prec)
            sval = fmt.format(val)
            iexp = list(map(float, sval.split('e')))
            if prec == 0:
                iexp[0] = int(iexp[0])
            iexp[1] = int(iexp[1])
            out = r'{} \cdot 10^{{{}}}'.format(*iexp)

        else:
            aux = r'{{:{}}}'.format(prec).format(val).replace(' ', '\enspace ')
            out = r'{}'.format(aux)

        if not in_math:
            out = '$' + out + '$'

        return out

    else:
        return str(val)

def format_array_latex(arr, prec=2, env='bmatrix'):
    arr = np.asarray(arr)
    arr = arr.reshape((arr.shape[0], -1))

    fmt = partial(format_float_latex, prec=prec, in_math=True)

    rows = [' & '.join(map(fmt, row)) for row in arr]
    body = ' \\\\\n'.join(rows)

    out = fragments['env'].format(env=env, text=body)

    return out

def escape_latex(txt):
    out = (txt.replace('\\', '\\textbackslash ')
           .replace('_', '\\_')
           .replace('%', '\\%')
           .replace('$', '\\$')
           .replace('#', '\\#')
           .replace('{', '\\{')
           .replace('}', '\\}')
           .replace('~', '\\textasciitilde ')
           .replace('^', '\\textasciicircum ')
           .replace('&', '\\&')
           if (txt and txt != '{}')
           else '{}')
    return out

def build_pdf(filename):
    status = run_command('pdflatex -interaction=nonstopmode', filename,
                         repeat=3, silent=True)
    if status:
        output('build_pdf() failed with status {}!'.format(status))
