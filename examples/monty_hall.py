#!/usr/bin/env python
"""
The Monty Hall problem simulator parameterizable with soops.

https://en.wikipedia.org/wiki/Monty_Hall_problem

soops-run -r 1 -n 3 -o output "python='python3', output_dir='output/%s', --num=[100,1000,10000], --repeat=[5,20], --switch=['@undefined', '@defined'], --host=['random', 'first'], --silent=@defined, --no-show=@defined" examples/monty_hall.py
"""
from argparse import ArgumentParser
import os
import time

import numpy as np
import matplotlib.pyplot as plt

import soops as so
from soops import output

def get_run_info():
    run_cmd = """
    {python} examples/monty_hall.py --num={--num} --repeat={--repeat}
    {output_dir}
    """
    run_cmd = ' '.join(run_cmd.split())

    # Arguments allowed to be missing in soops-run calls.
    opt_args = {
        '--switch' : ' --switch',
        '--host' : ' --host={--host}',
        '--seed' : ' --seed={--seed}',
        '--plot-opts' : ' --plot-opts={--plot-opts}',
        '--no-show' : ' --no-show',
        '--silent' : ' --silent',
    }

    is_finished_basename = 'wins.png'
    output_dir_key = 'output_dir'

    return run_cmd, opt_args, output_dir_key, is_finished_basename

helps = {
    'output_dir'
    : 'output directory',
    'switch'
    : ('if given, the contestant always switches the door, otherwise never'
       ' switches'),
    'host'
    : 'the host strategy for opening doors',
    'num'
    : 'the number of rounds in a single simulation [default: %(default)s]',
    'repeat'
    : 'the number of simulations [default: %(default)s]',
    'seed'
    : 'if given, the random seed is fixed to the given value',
    'plot_opts'
    : 'matplotlib plot() options [default: "{}"]',
    'no_show'
    : 'do not call matplotlib show()',
    'silent'
    : 'do not print messages to screen',
}

def main():
    default_plot_opts = ("linewidth=3,alpha=0.5")
    helps['plot_opts'] = helps['plot_opts'].format(default_plot_opts)

    parser = ArgumentParser(description=__doc__.rstrip())
    parser.add_argument('output_dir', help=helps['output_dir'])
    parser.add_argument('--switch',
                        action='store_true', dest='switch',
                        default=False, help=helps['switch'])
    parser.add_argument('--host', action='store', dest='host',
                        choices=['random', 'first'],
                        default='random', help=helps['host'])
    parser.add_argument('--num', metavar='int', type=int,
                        action='store', dest='num',
                        default=100, help=helps['num'])
    parser.add_argument('--repeat', metavar='int', type=int,
                        action='store', dest='repeat',
                        default=5, help=helps['repeat'])
    parser.add_argument('--seed', metavar='int', type=int,
                        action='store', dest='seed',
                        default=None, help=helps['seed'])
    parser.add_argument('--plot-opts', metavar='dict-like',
                        action='store', dest='plot_opts',
                        default=default_plot_opts, help=helps['plot_opts'])
    parser.add_argument('-n', '--no-show',
                        action='store_false', dest='show',
                        default=True, help=helps['no_show'])
    parser.add_argument('--silent',
                        action='store_true', dest='silent',
                        default=False, help=helps['silent'])
    options = parser.parse_args()

    output_dir = options.output_dir

    output.prefix = 'monty_hall:'
    filename = os.path.join(output_dir, 'output_log.txt')
    so.ensure_path(filename)
    output.set_output(filename=filename, combined=options.silent == False)

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

        tt = time.perf_counter()
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

        output('elapsed:', time.perf_counter() - tt)
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
