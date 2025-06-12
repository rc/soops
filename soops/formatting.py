import os.path as op
from functools import partial
from math import isfinite

import numpy as np
import pandas as pd

from soops.base import output, run_command
from soops.ioutils import fix_path

fragments = {
    #
    'begin-document' : r"""
\documentclass[{options}]{{article}}

\usepackage{{siunitx}}
\usepackage{{booktabs}}
\usepackage{{graphicx}}
\usepackage{{amsmath}}
\usepackage{{a4wide}}
\usepackage{{multirow}}

\begin{{document}}
""",
    #
    'end-document' : r"""
\end{document}
    """,
    #
    'section' : r"""
\{level}section{{{name}}}
\label{{{label}}}
    """,
    #
    'center' : r"""
\begin{{center}}
{text}
\end{{center}}
""",
    #
    'env' : r"""\begin{{{env}}}
{text}
\end{{{env}}}""",
    #
    'figure' : r"""
\begin{{figure}}[htp!]
  \centering
    \includegraphics[width={width}\linewidth]{{{path}}}
  \caption{{{caption}}}
  \label{{{label}}}
\end{{figure}}
    """,
    #
    'newline' : r"""
\\
""",
    #
    'newpage' : r"""
\clearpage
""",
    #
    'input' : r"""
\input {filename}
    """,
}

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

def format_float_latex(val, prec, in_math=False, mul_one=True):
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
            if (iexp[0] == 1.0) and not mul_one:
                out = r'10^{{{}}}'.format(iexp[1])

            else:
                out = r'{} \cdot 10^{{{}}}'.format(*iexp)

        else:
            aux = r'{{:{}}}'.format(prec).format(val).replace(' ', '\enspace ')
            out = r'{}'.format(aux)

        if not in_math:
            out = '$' + out + '$'

        return out

    else:
        return str(val)

def format_array_latex(arr, prec=2, rel_zero=0.0, env='bmatrix'):
    arr = np.asarray(arr)
    if arr.ndim == 0:
        return format_float_latex(arr, prec, in_math=True)

    arr = arr.reshape((arr.shape[0], -1))

    aarr = np.abs(arr)
    vmax = aarr.max()
    arr = np.where(aarr < vmax * rel_zero, 0.0, arr)

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

def itemize_latex(items):
    """
    Turn the `items` list into the itemize environment.
    """
    out = fragments['env'].format(
        env='itemize',
        text='\n'.join('\item ' + item for item in items)
    )
    return out

def make_tabular_latex(rows, **kwargs):
    """
    Put the `rows` dict or list into the tabular environment.
    """
    if isinstance(rows, dict):
        pdf = pd.Series(rows).map(str)

    else:
        pdf = pd.DataFrame(rows)

    pd.set_option('display.max_colwidth', None)
    out = pdf.to_latex(**kwargs)
    pd.reset_option('display.max_colwidth')

    return out

def setup_figures_latex(output_dir, figdir='figures'):
    infdir = partial(op.join, fix_path(output_dir), figdir)
    infigdir = partial(op.join, figdir)

    def make_figure_latex(figname, width=1.0, caption='', label=None):
        """
        Make the figure environment with the `figname` image file.
        """
        if label is None:
            label = 'fig:' + op.basename(figname)

        out = fragments['figure'].format(
            width=width,
            path=infigdir(op.basename(figname)),
            caption=caption,
            label=label,
        )

        return out

    return infdir, make_figure_latex

def build_pdf(filename):
    status = run_command('pdflatex -interaction=nonstopmode', filename,
                         repeat=3, silent=True)
    if status:
        output('build_pdf() failed with status {}!'.format(status))

def format_next(text, new_text, pos, can_newline, width, ispaces):
    new_len = len(new_text)

    if (pos + new_len > width) and can_newline:
        text += '\n' + ispaces + new_text
        pos = new_len
        can_newline = False

    else:
        if pos > 0:
            text += ' ' + new_text
            pos += new_len + 1

        else:
            text += ispaces + new_text
            pos += len(ispaces) + new_len

        can_newline = True

    return text, pos, can_newline

def typeset_to_indent(txt, indent, width):
    if not len(txt): return txt

    txt_lines = txt.strip().split('\n')
    ispaces = ' ' * indent

    can_newline = False
    pos = 0
    text = ''
    for line in txt_lines:
        for word in line.split():
            text, pos, can_newline = format_next(text, word, pos, can_newline,
                                                 width, ispaces)

    return text
