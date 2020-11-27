from math import isfinite

from soops.base import output, run_command

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
