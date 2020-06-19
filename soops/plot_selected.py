import numpy as np
import matplotlib.pyplot as plt

from soops.base import output

def normalize_selected(selected):
    nselected = selected.copy()
    for key, svals in selected.items():
        if not isinstance(svals, list):
            nselected[key] = [svals]

    return nselected

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

def setup_plot_styles(selected, raw_styles):
    styles = raw_styles.copy()
    for key, style in raw_styles.items():
        if not key in selected:
            continue

        for skey, svals in style.items():
            if skey == 'color' and isinstance(svals, str):
                cmap = getattr(plt.cm, svals)

                ncolors = max(len(selected[key]), 2)
                if hasattr(cmap, 'colors'):
                    acc = np.asarray(cmap.colors)
                    icc = np.linspace(0, len(acc) - 1,
                                      ncolors).astype(int)
                    cc = acc[icc]

                else:
                    cc = cmap(np.linspace(0.0, 1.0, ncolors))

                styles[key][skey] = cc

            elif skey == 'alpha' and isinstance(svals, str):
                mi, ma = map(float, svals.split(','))
                styles[key][skey] = np.linspace(mi, ma, len(selected[key]))

            elif np.isscalar(svals):
                styles[key][skey] = [svals]

    return styles

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

def get_row_style(df, ir, selected, compares, styles, **plot_kwargs):
    indices = get_indices_in_selected(selected, df.iloc[ir], compares)
    if indices is None: return None, None

    style_kwargs = get_plot_style(indices, styles)
    style_kwargs.update(plot_kwargs)

    return style_kwargs, indices

def get_legend_items(selected, styles, used=None, format_labels=None):
    if format_labels is None:
        format_labels = lambda key, iv, val: '{}: {}'.format(key, val)

    lines = []
    labels = []
    for key, svals in selected.items():
        key_styles = styles[key]
        if not len(key_styles): continue

        for iv, val in enumerate(svals):
            if (used is not None) and (iv not in used[key]): continue

            kw = {}
            for skey, style_vals in key_styles.items():
                kw[skey] = style_vals[iv % len(style_vals)]
                if skey in ('fillstyle', 'markersize'):
                    kw['marker'] = 'o'

            if not len(kw):
                continue

            if 'color' not in kw:
                kw['color'] = (0.5, 0.5, 0.5)

            line = plt.Line2D((0,1), (0,0), **kw)

            if ((line.get_linestyle() == 'None') and
                (line.get_marker() == 'None')):
                line.set_linestyle('-')

            lines.append(line)
            labels.append(format_labels(key, iv, val))

    return lines, labels

def update_used(used, indices):
    if used is None:
        used = {}

    for key, indx in indices.items():
        used.setdefault(key, set()).add(indices[key])

    return used

def add_legend(ax, selected, styles, used, format_labels=None):
    lines, labels = get_legend_items(selected, styles, used=used,
                                     format_labels=format_labels)

    leg = ax.legend(lines, labels, loc='best')
    if leg is not None:
        leg.get_frame().set_alpha(0.5)

def plot_selected(ax, df, column, selected, compares, styles,
                  format_labels=None, **plot_kwargs):
    if ax is None:
        _, ax = plt.subplots()

    used = None
    for ir in range(len(df)):
        style_kwargs, indices = get_row_style(
            df, ir, selected, compares, styles, **plot_kwargs
        )
        if indices is None: continue
        used = update_used(used, indices)

        ax.plot(df.loc[ir, column], **style_kwargs)

    add_legend(ax, selected, styles, used, format_labels=format_labels)

    return ax
