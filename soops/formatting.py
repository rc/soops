import os
import subprocess
from math import isfinite

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

    elif isfinite(val):
        fmt = '{{:.{}e}}'.format(prec)
        sval = fmt.format(val)
        iexp = list(map(float, sval.split('e')))
        iexp[1] = int(iexp[1])
        return r'${} \cdot 10^{{{}}}$'.format(*iexp)

    else:
        return str(val)

def build_pdf(filename):
    fdir = os.path.dirname(os.path.abspath(filename))
    fname = os.path.basename(filename)
    build_cmd = 'pdflatex -interaction=nonstopmode %s' % fname
    with open(os.devnull, 'w') as devnull:
        for ii in range(3):
            subprocess.call(build_cmd.split(), cwd=fdir,
                            stdout=devnull, stderr=devnull)
