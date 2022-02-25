import itertools

import numpy as np
import matplotlib.pyplot as plt

from soops.base import output
from soops.parsing import parse_as_dict

def select_by_keys(df, keys):
    selected = {key : sorted(df[key].unique()) for key in keys}
    return selected

def normalize_selected(selected):
    nselected = selected.copy()
    for key, svals in selected.items():
        if not isinstance(svals, list):
            nselected[key] = [svals]

    return nselected

def setup_plot_styles(selected, raw_styles):
    styles = raw_styles.copy()
    for key, style in raw_styles.items():
        if not key in selected:
            continue

        for skey, svals in style.items():
            if skey == 'color' and isinstance(svals, str):
                cmap_name, *modifiers = svals.split(':')
                mods = parse_as_dict(modifiers[0]) if len(modifiers) else {}
                cmap = getattr(plt.cm, cmap_name)

                ncolors = max(len(selected[key]), 2)
                if (hasattr(cmap, 'colors') and
                    (mods.get('kind') == 'qualitative')):
                    acc = np.asarray(cmap.colors)
                    i0 = mods.get('min', 0)
                    i1 = mods.get('max', ncolors)
                    cc = acc[i0:i1]

                else:
                    t0 = mods.get('min', 0.0)
                    t1 = mods.get('max', 1.0)
                    cc = cmap(np.linspace(t0, t1, ncolors))

                styles[key][skey] = cc

            elif skey == 'alpha' and isinstance(svals, str):
                mi, ma = map(float, svals.split(','))
                styles[key][skey] = np.linspace(mi, ma, len(selected[key]))

            elif np.isscalar(svals):
                styles[key][skey] = [svals]

    return styles

def get_cat_style(selected, key, styles, skey):
    cdict = {cat : val for cat, val in zip(selected[key], styles[key][skey])}
    return cdict

def select_cat_style(cdict, cats):
    return [cdict[cat] for cat in cats]

def get_indices_in_selected(selected, row, compares):
    _cmp = lambda a, b: a == b
    indices = {}
    for key, svals in selected.items():
        cmp = compares.get(key, _cmp)
        dval = row[key]
        for ii, sval in enumerate(svals):
            if cmp(sval, dval):
                indices[key] = ii
                break

        else:
            return None

    return indices

def get_plot_style(indices, styles):
    style_kwargs = {}
    for key, key_styles in styles.items():
        for skey, style_vals in key_styles.items():
            if skey in style_kwargs:
                output('style key "{}" of "{}" already in use!'
                       .format(skey, key))

            if key in indices:
                style_kwargs[skey] = style_vals[indices[key] % len(style_vals)]

    return style_kwargs

def get_row_style(row, selected, compares, styles, **plot_kwargs):
    indices = get_indices_in_selected(selected, row, compares)
    if indices is None: return None, None

    style_kwargs = get_plot_style(indices, styles)
    style_kwargs.update(plot_kwargs)

    return style_kwargs, indices

def update_used(used, indices):
    if used is None:
        used = {}

    for key, indx in indices.items():
        used.setdefault(key, set()).add(indices[key])

    return used

def get_row_style_used(row, selected, compares, styles, used, **plot_kwargs):
    """
    Combines :func:`get_row_style()` and :func:`update_used()` into a single
    call.
    """
    style_kwargs, indices = get_row_style(
        row, selected, compares, styles, **plot_kwargs
    )
    if indices is not None:
        used = update_used(used, indices)

    return style_kwargs, indices, used

def get_legend_items(selected, styles, used=None, format_labels=None):
    if format_labels is None:
        format_labels = lambda key, iv, val: '{}: {}'.format(key, val)

    lines = []
    labels = []
    for key, svals in selected.items():
        key_styles = styles[key]
        if not len(key_styles): continue

        plines = []
        plabels = []
        for iv, val in enumerate(svals):
            if (used is not None) and (iv not in used[key]): continue

            kw = {}
            for skey, style_vals in key_styles.items():
                kw[skey] = style_vals[iv % len(style_vals)]
                if ((skey in ('fillstyle', 'markersize', 'markeredgecolor',
                              'markeredgewidth', 'markerfacecolor',
                              'markerfacecoloralt', 'markevery')) and
                    ('marker' not in key_styles)):
                    kw['marker'] = 'o'

            if not len(kw):
                continue

            if 'color' not in kw:
                kw['color'] = (0.5, 0.5, 0.5)

            line = plt.Line2D((0,1), (0,0), **kw)

            if ((line.get_linestyle() == 'None') and
                (line.get_marker() == 'None')):
                line.set_linestyle('-')

            plines.append(line)
            plabels.append(format_labels(key, iv, val))

        lines.append(plines)
        labels.append(plabels)

    return lines, labels

def add_legend(ax, selected, styles, used, format_labels=None,
               per_parameter=False, loc='best', fontsize=None, frame_alpha=0.5,
               **kwargs):

    lines, labels = get_legend_items(selected, styles, used=used,
                                     format_labels=format_labels)
    if per_parameter:
        if not isinstance(loc, list):
            ploc = [loc] * len(plines)

        legs = []
        for plines, plabels, ploc in zip(lines, labels, loc):
            leg = ax.legend(plines, plabels, loc=ploc, fontsize=fontsize,
                            **kwargs)
            legs.append(leg)

        for leg in legs[:-1]:
            ax.add_artist(leg)

    else:
        lines = itertools.chain(*lines)
        labels = itertools.chain(*labels)
        leg = ax.legend(lines, labels, loc=loc, fontsize=fontsize, **kwargs)

    if leg is not None:
        leg.get_frame().set_alpha(frame_alpha)

def plot_selected(ax, df, column, selected, compares, styles,
                  format_labels=None, xaxis=None, **plot_kwargs):
    if ax is None:
        _, ax = plt.subplots()

    used = None
    for ir in range(len(df)):
        style_kwargs, indices, used = get_row_style_used(
            df.iloc[ir], selected, compares, styles, used, **plot_kwargs
        )
        if style_kwargs is None: continue
        if xaxis is None:
            ax.plot(df.loc[df.index[ir], column], **style_kwargs)

        else:
            ax.plot(df.loc[df.index[ir], xaxis],
                    df.loc[df.index[ir], column], **style_kwargs)

    add_legend(ax, selected, styles, used, format_labels=format_labels)

    return ax
