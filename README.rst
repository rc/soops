soops
=====

soops = scoop output of parametric studies

Utilities to run parametric studies in parallel using dask, and to scoop
the output files produced by the studies into a pandas dataframe.

Installation
------------

The latest release::

  pip install soops

The source code of the development version in git::

  git clone https://github.com/rc/soops.git
  cd soops
  pip install .

or the development version via pip::

  pip install git+https://github.com/rc/soops.git

Testing
-------

Install pytest::

  pip install pytest

Install `soops` from sources (in the current directory)::

  pip install .

Run the tests::

  pytest .

Example
-------

Before we begin - TL;DR:

- Run a script in parallel with many combinations of parameters.
- Scoop all the results in many output directories into a big ``DataFrame``.
- Work with the ``DataFrame``.

A Script
''''''''

Suppose we have a script that takes a number of command line arguments. The
actual arguments are not so important, neither what the script does.
Nevertheless, to have something to work with, let us simulate the `Monty Hall
problem <https://en.wikipedia.org/wiki/Monty_Hall_problem>`_ in Python.

For the first reading of the example below, it is advisable not to delve in
details of the script outputs and code listings and just read the text to get
an overall idea. After understanding the idea, return to the details, or just
have a look at the `complete example script <examples/monty_hall.py>`_.

This is our script and its arguments::

  $ python ./examples/monty_hall.py -h
  usage: monty_hall.py [-h] [--switch] [--host {random,first}] [--num int]
                       [--repeat int] [--seed int] [--plot-opts dict-like] [-n]
                       [--silent]
                       output_dir

  The Monty Hall problem simulator parameterizable with soops.

  https://en.wikipedia.org/wiki/Monty_Hall_problem

  <snip>

  positional arguments:
    output_dir            output directory

  optional arguments:
    -h, --help            show this help message and exit
    --switch              if given, the contestant always switches the door,
                          otherwise never switches
    --host {random,first}
                          the host strategy for opening doors
    --num int             the number of rounds in a single simulation [default:
                          100]
    --repeat int          the number of simulations [default: 5]
    --seed int            if given, the random seed is fixed to the given value
    --plot-opts dict-like
                          matplotlib plot() options [default:
                          "linewidth=3,alpha=0.5"]
    -n, --no-show         do not call matplotlib show()
    --silent              do not print messages to screen

Basic Run
'''''''''

A run with the default parameters::

  $ python examples/monty_hall.py output
  monty_hall: num: 100
  monty_hall: repeat: 5
  monty_hall: switch: False
  monty_hall: host strategy: random
  monty_hall: elapsed: 0.004662119084969163
  monty_hall: win rate: 0.25
  monty_hall: elapsed: 0.0042096920078620315
  monty_hall: win rate: 0.3
  monty_hall: elapsed: 0.003894180990755558
  monty_hall: win rate: 0.31
  monty_hall: elapsed: 0.003928505931980908
  monty_hall: win rate: 0.35
  monty_hall: elapsed: 0.0035342529881745577
  monty_hall: win rate: 0.31

produces some results:

.. image:: doc/readme/wins.png
   :alt: wins.png

Parameterization
''''''''''''''''

Now we would like to run it for various combinations of arguments and their
values, for example:

- `--num=[100,1000,10000]`
- `--repeat=[10,20]`
- `--switch` either given or not
- `--seed` either given or not, changing together with `--seed`
- `--host=['random', 'first']`

and then collect and analyze the all results. Doing this manually is quite
tedious, but `soops` can help.

In order to run a parametric study, first we have to define a function
describing the arguments of our script:

.. code:: python

   def get_run_info():
       run_cmd = """
       {python} {script_dir}/monty_hall.py
       --num={--num} --repeat={--repeat}
       {output_dir}
       """
       run_cmd = ' '.join(run_cmd.split())

       # Arguments allowed to be missing in soops-run calls.
       opt_args = {
           '--switch' : '--switch',
           '--host' : '--host={--host}',
           '--seed' : '--seed={--seed}',
           '--plot-opts' : '--plot-opts={--plot-opts}',
           '--no-show' : '--no-show',
           '--silent' : '--silent',
       }

       output_dir_key = 'output_dir'
       is_finished_basename = 'wins.png'

       return run_cmd, opt_args, output_dir_key, is_finished_basename

The `get_run_info()` functions should provide four items:

#. A command to run given as a string, with the non-optional arguments and
   their values (if any) given as ``str.format()`` keys.

#. A dictionary of optional arguments and their values (if any) given as
   ``str.format()`` keys.

#. A special format key, that denotes the output directory argument of the
   command. Note that the script must have an argument allowing an output
   directory specification.

#. A function ``is_finished()`` taking the output directory argument that
   returns True, if the results are already present in that directory. Instead
   of a function, a file name can be given, as in `get_run_info()` above. Then
   the existence of a file with the specified name means that the results are
   present in the directory.

Run Parametric Study
''''''''''''''''''''

Putting `get_run_info()` into our script allows running a parametric study using
`soops-run`::

  $ soops-run -h
  usage: soops-run [-h] [-r {0,1,2}] [-c key1+key2+..., ...] [-n int]
                   [--create-output-dirs] [--silent] [--shell] [-o path]
                   conf run_mod

  Run parametric studies.

  positional arguments:
    conf                  a dict-like parametric study configuration
    run_mod               the importable script/module with get_run_info()

  optional arguments:
    -h, --help            show this help message and exit
    -r {0,1,2}, --recompute {0,1,2}
                          recomputation strategy: 0: do not recompute, 1:
                          recompute only if is_finished() returns False, 2:
                          always recompute [default: 1]
    -c key1+key2+..., ..., --contract key1+key2+..., ...
                          list of option keys that should be contracted to vary
                          in lockstep
    -n int, --n-workers int
                          the number of dask workers [default: 2]
    --create-output-dirs  create parametric output directories if necessary
    --silent              do not print messages to screen
    --shell               run ipython shell after all computations
    -o path, --output-dir path
                          output directory [default: output]

In our case (the arguments with no value (flags) can be specified either as
``'@defined'`` or ``'@undefined'``)::

  soops-run -r 1 -n 3 -c='--switch + --seed' -o output "python='python3', output_dir='output/study/%s', --num=[100,1000,10000], --repeat=[10,20], --switch=['@undefined', '@defined', '@undefined', '@defined'], --seed=['@undefined', '@undefined', 12345, 12345], --host=['random', 'first'], --silent=@defined, --no-show=@defined" examples/monty_hall.py

This command runs our script using three dask workers (``-n 3`` option) and
produces a directory for each parameter set::

  $ ls output/study/
  0_0_0_0_0_0_0_0_0/  0_0_1_0_1_0_0_0_0/  1_0_0_0_0_0_0_0_0/  1_0_1_0_1_0_0_0_0/
  0_0_0_0_0_1_0_1_0/  0_0_1_0_1_1_0_1_0/  1_0_0_0_0_1_0_1_0/  1_0_1_0_1_1_0_1_0/
  0_0_0_0_0_2_0_2_0/  0_0_1_0_1_2_0_2_0/  1_0_0_0_0_2_0_2_0/  1_0_1_0_1_2_0_2_0/
  0_0_0_0_0_3_0_3_0/  0_0_1_0_1_3_0_3_0/  1_0_0_0_0_3_0_3_0/  1_0_1_0_1_3_0_3_0/
  0_0_0_0_1_0_0_0_0/  0_0_2_0_0_0_0_0_0/  1_0_0_0_1_0_0_0_0/  1_0_2_0_0_0_0_0_0/
  0_0_0_0_1_1_0_1_0/  0_0_2_0_0_1_0_1_0/  1_0_0_0_1_1_0_1_0/  1_0_2_0_0_1_0_1_0/
  0_0_0_0_1_2_0_2_0/  0_0_2_0_0_2_0_2_0/  1_0_0_0_1_2_0_2_0/  1_0_2_0_0_2_0_2_0/
  0_0_0_0_1_3_0_3_0/  0_0_2_0_0_3_0_3_0/  1_0_0_0_1_3_0_3_0/  1_0_2_0_0_3_0_3_0/
  0_0_1_0_0_0_0_0_0/  0_0_2_0_1_0_0_0_0/  1_0_1_0_0_0_0_0_0/  1_0_2_0_1_0_0_0_0/
  0_0_1_0_0_1_0_1_0/  0_0_2_0_1_1_0_1_0/  1_0_1_0_0_1_0_1_0/  1_0_2_0_1_1_0_1_0/
  0_0_1_0_0_2_0_2_0/  0_0_2_0_1_2_0_2_0/  1_0_1_0_0_2_0_2_0/  1_0_2_0_1_2_0_2_0/
  0_0_1_0_0_3_0_3_0/  0_0_2_0_1_3_0_3_0/  1_0_1_0_0_3_0_3_0/  1_0_2_0_1_3_0_3_0/

In each directory, there are three files::

  $ ls output/study/0_0_0_0_0_0_0_0_0/
  options.txt  output_log.txt  wins.png

just like in the basic run above. Our example script stores the values of
command line arguments in ``options.txt`` for possible re-runs and inspection::

  $ cat output/study/0_0_0_0_0_0_0_0_0/options.txt

  command line
  ------------

  "examples/monty_hall.py" "--num=100" "--repeat=10" "output/study/0_0_0_0_0_0_0_0_0" "--host=random" "--no-show" "--silent"

  options
  -------

  host: random
  num: 100
  output_dir: output/study/0_0_0_0_0_0_0_0_0
  plot_opts: {'linewidth': 3, 'alpha': 0.5}
  repeat: 10
  seed: None
  show: False
  silent: True
  switch: False

Explain Output Directory Names
''''''''''''''''''''''''''''''

Use ``soops-info`` to explain the output directory names::

  $ soops-info -h
  usage: soops-info [-h] [-e output directory] [--shell] run_mod

  Get parametric study configuration information.

  positional arguments:
    run_mod               the importable script/module with get_run_info()

  optional arguments:
    -h, --help            show this help message and exit
    -e output directory, --explain output directory
                          explain the given directory name
    --shell               run ipython shell after all computations

::

  $ soops-info -e output/study/1_0_2_0_1_3_0_3_0/ examples/monty_hall.py
  info:   0:   1 <- --host
  info:   1:   0 <- --no-show
  info:   2:   2 <- --num
  info:   3:   0 <- --plot-opts
  info:   4:   1 <- --repeat
  info:   5:   3 <- --seed
  info:   6:   0 <- --silent
  info:   7:   3 <- --switch
  info:   8:   0 <- python

Scoop Outputs of the Parametric Study
'''''''''''''''''''''''''''''''''''''

In order to use ``soops-scoop`` to scoop/collect outputs of our parametric
study, a new function needs to be defined:

.. code:: python

   import soops.scoop_outputs as sc

   def get_scoop_info():
       info = [
           ('options.txt', partial(
               sc.load_split_options,
               split_keys=None,
           ), True),
           ('output_log.txt', scrape_output),
       ]

       return info

The function for loading the ``'options.txt'`` files is already in `soops`. The
third item in the tuple, if present and True, denotes that the output contains
input parameters that were used for the parameterization. This allows getting
the parameterization in post-processing plugins, see below
the ``plot_win_rates()`` function.

The function to get useful information from ``'output_log.txt'`` needs to be
provided:

.. code:: python

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

Then we are ready to run ``soops-scoop``::

  $ soops-scoop -h
  usage: soops-scoop [-h] [-s column[,columns,...]] [-r filename] [--no-plugins]
                     [--use-plugins name[,name,...] | --omit-plugins
                     name[,name,...]] [-p module] [--plugin-args dict-like]
                     [--shell] [-o path]
                     scoop_mod directories [directories ...]

  Scoop output files.

  positional arguments:
    scoop_mod             the importable script/module with get_scoop_info()
    directories           results directories

  optional arguments:
    -h, --help            show this help message and exit
    -s column[,columns,...], --sort column[,columns,...]
                          column keys for sorting of DataFrame rows
    -r filename, --results filename
                          reuse previously scooped results file
    --no-plugins          do not call post-processing plugins
    --use-plugins name[,name,...]
                          use only the named plugins (no effect with --no-
                          plugins)
    --omit-plugins name[,name,...]
                          omit the named plugins (no effect with --no-plugins)
    -p module, --plugin-mod module
                          if given, the module that has get_plugin_info()
                          instead of scoop_mod
    --plugin-args dict-like
                          optional arguments passed to plugins given as
                          plugin_name={key1=val1, key2=val2, ...}, ...
    --shell               run ipython shell after all computations
    -o path, --output-dir path
                          output directory [default: .]

as follows::

  $ soops-scoop examples/monty_hall.py output/study/ -s rdir -o output/study --no-plugins --shell

  <snip>

  Python 3.7.3 | packaged by conda-forge | (default, Jul  1 2019, 21:52:21)
  Type 'copyright', 'credits' or 'license' for more information
  IPython 7.13.0 -- An enhanced Interactive Python. Type '?' for help.

  In [1]: df.keys()
  Out[1]:
  Index(['rdir', 'host', 'num', 'output_dir', 'plot_opts', 'repeat', 'seed',
         'show', 'silent', 'switch', 'elapsed', 'win_rate', 'time'],
        dtype='object')

  In [2]: df.win_rate.head()
  Out[2]:
  0    [0.35, 0.28, 0.26, 0.41, 0.32, 0.37, 0.29, 0.3...
  1    [0.59, 0.65, 0.67, 0.73, 0.72, 0.74, 0.69, 0.6...
  2    [0.32, 0.32, 0.32, 0.32, 0.32, 0.32, 0.32, 0.3...
  3    [0.68, 0.68, 0.68, 0.68, 0.68, 0.68, 0.68, 0.6...
  4    [0.34, 0.35, 0.31, 0.32, 0.38, 0.31, 0.42, 0.3...
  Name: win_rate, dtype: object

  In [3]: df.iloc[0]
  Out[3]:
  rdir            ~/projects/soops/output/study/0_0_0_0_0_0_0_0_0
  host                                                     random
  num                                                         100
  output_dir                       output/study/0_0_0_0_0_0_0_0_0
  plot_opts                        {'linewidth': 3, 'alpha': 0.5}
  repeat                                                       10
  seed                                                        NaN
  show                                                      False
  silent                                                     True
  switch                                                    False
  elapsed       [0.004276808933354914, 0.003945986973121762, 0...
  win_rate      [0.35, 0.28, 0.26, 0.41, 0.32, 0.37, 0.29, 0.3...
  time                                 2020-04-01 19:04:34.712128
  Name: 0, dtype: object

The ``DataFrame`` with the all results is saved in ``output/study/results.h5``
for reuse.

Post-processing Plugins
'''''''''''''''''''''''

It is also possible to define simple plugins that act on the resulting
``DataFrame``. First, define a function that will register the plugins:

.. code:: python

   def get_plugin_info():
       from soops.plugins import show_figures

       info = [plot_win_rates, show_figures]

       return info

The ``show_figures()`` plugin is defined in `soops`. The ``plot_win_rates()``
plugin allows plotting the all results combined:

.. code:: python

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

Then, running::

  soops-scoop examples/monty_hall.py output/study/ -s rdir -o output/study -r output/study/results.h5

reuses the ``results.h5`` file and plots the combined results:

.. image:: doc/readme/win_rates.png
   :alt: win_rates.png

It is possible to pass arguments to plugins using ``--plugin-args`` option, as
follows::

  soops-scoop examples/monty_hall.py output/study/ -s rdir -o output/study -r output/study/results.h5 --plugin-args=plot_win_rates={colormap_name='plasma'}

Notes
'''''

- The `get_run_info()`, `get_scoop_info()` and `get_plugin_info()` info
  function can be in different modules.
- The script that is being parameterized need not be a Python module - any
  executable which can be run from a command line can be used.
