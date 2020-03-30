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

def get_legend_items(selected, styles, used=None):
    used_selected = selected
    if used is not None:
        used_selected = {key : [val for ii, val in enumerate(vals)
                              if ii in used[key]]
                       for key, vals in selected.items()}

    lines = []
    labels = []
    for key, svals in used_selected.items():
        key_styles = styles[key]
        if not len(key_styles): continue

        for iv, val in enumerate(svals):
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
            lines.append(line)
            labels.append(key + ': {}'.format(val))

    return lines, labels

def plot_selected(ax, df, column, selected, compares, styles, **plot_kwargs):
    if ax is None:
        _, ax = plt.subplots()

    used = {key : set() for key in selected.keys()}
    for ir in range(len(df)):
        indices = get_indices_in_selected(selected, df.iloc[ir], compares)
        if indices is None: continue

        style_kwargs = get_plot_style(indices, styles)
        style_kwargs.update(plot_kwargs)
        ax.plot(df.loc[ir, column], **style_kwargs)

        for key, indx in indices.items():
            used[key].add(indices[key])

    lines, labels = get_legend_items(selected, styles, used=used)
    leg = ax.legend(lines, labels, loc='best')
    if leg is not None:
        leg.get_frame().set_alpha(0.5)

    return ax
