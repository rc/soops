#!/usr/bin/env python
"""
The Monty Hall problem simulator parameterized with soops.

https://en.wikipedia.org/wiki/Monty_Hall_problem

Examples
--------

- Direct runs::

  python soops/examples/monty_hall.py -h
  python soops/examples/monty_hall.py output
  python soops/examples/monty_hall.py --switch output
  python soops/examples/monty_hall.py --num=10000 output

- A parametric study::

  soops-run -r 1 -n 3 -c='--switch + --seed' -o output "python='python3', output_dir='output/study/%s', --num=[100,1000,10000], --repeat=[10,20], --switch=['@undefined', '@defined', '@undefined', '@defined'], --seed=['@undefined', '@undefined', 12345, 12345], --host=['random', 'first'], --silent=@defined, --no-show=@defined" soops/examples/monty_hall.py

  soops-info soops/examples/monty_hall.py -e output/study/00*

  soops-scoop soops/examples/monty_hall.py output/study/ -s rdir -o output/study

  soops-scoop soops/examples/monty_hall.py output/study/ -s rdir -o output/study -r --plugin-args=plot_win_rates={colormap_name='plasma'}

  soops-scoop soops/examples/monty_hall.py output/study/ -o output/study -r --no-plugins --shell

- The same parametric study as above, but the parameters are given by a study
  configuration file::

  soops-run -r 1 -n 3 -c='--switch + --seed' --study=study -o output soops/examples/studies.cfg soops/examples/monty_hall.py

- Use --generate-pars instead of listing values of --seed and --switch::

  soops-run -r 1 -n 3 -c='--switch + --seed' -o output/study-g "python='python3', output_dir='output/study-g/%s', --num=[100,1000,10000], --repeat=[10,20], --switch=@generate, --seed=@generate, --host=['random', 'first'], --silent=@defined, --no-show=@defined" --generate-pars="function=generate_seed_switch, seeds=['@undefined', 12345], switches=['@undefined', '@defined']" soops/examples/monty_hall.py

  soops-scoop soops/examples/monty_hall.py output/study-g/0* -s rdir -o output/study-g

- The same parametric study as above, but the parameters are given by a study
  configuration file::

  soops-run -r 1 -n 3 -c='--switch + --seed' --generate-pars=study-g.@generate --study=study-g -o output soops/examples/studies.cfg soops/examples/monty_hall.py

- Explore parameters of a study::

  soops-find output/study

  soops-find output/study -q "num==1000 & repeat==20 & seed==12345"
"""
import os
from functools import partial
from itertools import product

import numpy as np
import matplotlib.pyplot as plt

import soops as so
import soops.scoop_outputs as sc
from soops import output

opts = so.Struct(
    output_dir=(None, 'output directory'),
    switch=(False, 'if given, the contestant always switches the door,'
            ' otherwise never switches'),
    host=(('random', 'first'), 'the host strategy for opening doors'),
    num=(100, 'the number of rounds in a single simulation'),
    repeat=(5, 'the number of simulations'),
    seed=([None, 42], 'if given, the random seed is fixed to the given value'),
    plot_opts=('linewidth=3,alpha=0.5', 'matplotlib plot() options'),
    show=(True, 'do not call matplotlib show()', ),
    silent=(False, 'do not print messages to screen'),
)

def get_run_info():
    # script_dir is added by soops-run, it is the normalized path to
    # this script.
    run_cmd = """
    {python} {script_dir}/monty_hall.py {output_dir}
    """
    run_cmd = ' '.join(run_cmd.split())
    opt_args = so.build_opt_args(opts, omit=['--plot-opts'],
                                 return_defaults=True)
    output_dir_key = 'output_dir'
    is_finished_basename = 'wins.png'

    return run_cmd, opt_args, output_dir_key, is_finished_basename

def generate_seed_switch(args, gkeys, dconf, options):
    """
    Parameters
    ----------
    args : Struct
        The arguments passed from the command line.
    gkeys : list
        The list of option keys to generate.
    dconf : dict
        The parsed parameters of the parametric study.
    options : Namespace
        The soops-run command line options.
    """
    seeds, switches = zip(*product(args.seeds, args.switches))
    gconf = {'--seed' : list(seeds), '--switch' : list(switches)}
    return gconf

def get_scoop_info():
    info = [
        ('options.txt', partial(
            sc.load_split_options,
            split_keys=None,
        ), True),
        ('output_log.txt', scrape_output),
    ]

    return info

def scrape_output(filename, rdata=None):
    out = {}
    with open(filename, 'r') as fd:
        repeat = rdata['repeat']
        for ii in range(4):
            next(fd)

        elapsed = []
        win_rate = []
        for ii in range(repeat):
            line = next(fd).split()
            elapsed.append(float(line[-1]))
            line = next(fd).split()
            win_rate.append(float(line[-1]))

        out['elapsed'] = np.array(elapsed)
        out['win_rate'] = np.array(win_rate)

    return out

def get_plugin_info():
    from soops.plugins import show_figures

    info = [plot_win_rates, show_figures]

    return info

def plot_win_rates(df, data=None, colormap_name='viridis'):
    import soops.plot_selected as sps

    df = df.copy()
    df['seed'] = df['seed'].where(df['seed'].notnull(), -1)

    uniques = sc.get_uniques(df, [key for key in data.multi_par_keys
                                  if key not in ['output_dir']])
    output('parameterization:')
    for key, val in uniques.items():
        output(key, val)

    selected = sps.normalize_selected(uniques)

    styles = {key : {} for key in selected.keys()}
    styles['seed'] = {'alpha' : [0.9, 0.1]}
    styles['num'] = {'color' : colormap_name}
    styles['repeat'] = {'lw' : np.linspace(3, 2,
                                           len(selected.get('repeat', [1])))}
    styles['host'] = {'ls' : ['-', ':']}
    styles['switch'] = {'marker' : ['x', 'o'], 'mfc' : 'None', 'ms' : 10}

    styles = sps.setup_plot_styles(selected, styles)

    fig, ax = plt.subplots(figsize=(8, 8))
    sps.plot_selected(ax, df, 'win_rate', selected, {}, styles)
    ax.set_xlabel('simulation number')
    ax.set_ylabel('win rate')
    fig.tight_layout()
    fig.savefig(os.path.join(data.output_dir, 'win_rates.png'))

    return data

def parse_args(args=None):
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    so.build_arg_parser(parser, opts, aliases=dict(show='-n'))
    options = parser.parse_args(args=args)
    options.plot_opts = so.parse_as_dict(options.plot_opts)

    return options

def main():
    options = parse_args()

    output_dir = options.output_dir

    output.prefix = 'monty_hall:'
    filename = os.path.join(output_dir, 'output_log.txt')
    so.ensure_path(filename)
    output.set_output(filename=filename, combined=not options.silent)

    options.plot_opts = so.parse_as_dict(options.plot_opts)
    filename = os.path.join(output_dir, 'options.txt')
    so.save_options(filename, [('options', vars(options))],
                 quote_command_line=True)

    output('num:', options.num)
    output('repeat:', options.repeat)
    output('switch:', options.switch)
    output('host strategy:', options.host)

    switch = options.switch
    host_strategy = options.host
    histories = []
    for ir in range(options.repeat):
        if options.seed is not None:
            np.random.seed(options.seed)

        timer = so.Timer().start()
        history = []
        choices = {0, 1, 2}
        for ii in range(options.num):
            doors = [False] * 3
            car = np.random.randint(0, 3)
            doors[car] = True
            choice1 = np.random.randint(0, 3)
            can_host = sorted(choices.difference([car, choice1]))
            if len(can_host) == 2: # choice1 is correct.
                if host_strategy == 'random':
                    host = can_host[np.random.randint(0, 2)]

                else:
                    host = can_host[0]

            else:
                host = can_host[0]

            if switch:
                choice2 = choices.difference([choice1, host]).pop()

            else:
                choice2 = choice1

            win = doors[choice2]
            history.append(win)

        output('elapsed:', timer.stop())
        output('win rate:', sum(history) / options.num)

        histories.append(history)

    wins = np.cumsum(histories, axis=1)

    fig, ax = plt.subplots()
    colors = plt.cm.viridis(np.linspace(0, 1, options.repeat))
    ax.set_prop_cycle(
        plt.cycler('color', colors)
    )

    ax.plot(wins.T, **options.plot_opts)
    ax.set_xlabel('round')
    ax.set_ylabel('wins')
    ax.set_title('switch: {}, host strategy: {}, num: {}, repeat: {}, seed: {}'
                 .format(options.switch, options.host, options.num,
                         options.repeat, options.seed))
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, 'wins.png'), bbox_inches='tight')

    if options.show:
        plt.show()

if __name__ == '__main__':
    main()
