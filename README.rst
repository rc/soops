soops
=====

soops = scoop output of parametric studies

Utilities to run parametric studies in parallel, and to scoop the output files
produced by the studies into a pandas dataframe.

.. contents::

Installation
------------

The latest release::

  pip install soops

Optionally, `dask.distributed` can be used to run the studies instead of the
default concurrent.futures::

  pip install soops[dask]

The source code of the development version in git::

  git clone https://github.com/rc/soops.git
  cd soops
  pip install .
  # or
  pip install .[dask]

or the development version via pip::

  pip install git+https://github.com/rc/soops.git

Testing
-------

Install pytest::

  pip install pytest

Install `soops` from sources (in the current directory)::

  pip install .

Run the tests (in any directory)::

  python -c "import soops; soops.test()"

Run tests in the source directory without installing soops::

  export PYTHONPATH=.
  python -c "import soops; soops.test()"
  # or
  pytest soops/tests

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
have a look at the `complete example script <soops/examples/monty_hall.py>`_.

This is our script and its arguments::

  $ python soops/examples/monty_hall.py -h
  usage: monty_hall.py [-h] [--switch] [--host {random,first}] [--num INT]
                       [--repeat INT] [--seed INT] [--plot-opts STR] [-n]
                       [--silent]
                       output_dir

  The Monty Hall problem simulator parameterized with soops.

  https://en.wikipedia.org/wiki/Monty_Hall_problem

  <snip>

  positional arguments:
    output_dir            output directory

  options:
    -h, --help            show this help message and exit
    --switch              if given, the contestant always switches the door,
                          otherwise never switches
    --host {random,first}
                          the host strategy for opening doors [default: random]
    --num INT             the number of rounds in a single simulation [default:
                          100]
    --repeat INT          the number of simulations [default: 5]
    --seed INT            if given, the random seed is fixed to the given value
                          [default: None]
    --plot-opts STR       matplotlib plot() options [default:
                          linewidth=3,alpha=0.5]
    -n, --no-show         do not call matplotlib show()
    --silent              do not print messages to screen

Basic Run
'''''''''

A run with the default parameters::

  $ python soops/examples/monty_hall.py output
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

The `get_run_info()` functions should provide four items:

#. A command to run given as a string, with the non-optional arguments and
   their values (if any) given as ``str.format()`` keys.

#. A dictionary of optional arguments constructed using `so.build_opt_args()`
   from `opts`. Note that `opts` data are used also to build automatically the
   command line options in `parse_args()`, see `the example script
   <soops/examples/monty_hall.py>`_.

#. A special format key, that denotes the output directory argument of the
   command. Note that the script must have an argument allowing an output
   directory specification.

#. A function ``is_finished(pars, options)``, where `pars` is the dictionary of
   the actual values of the script arguments and `options` are `soops-run`
   options, see below. The dictionary contains the output directory argument of
   the script and the function should return True, whenever the results are
   already present in the given output directory. Instead of a function, a file
   name can be given, as in `get_run_info()` above. Then the existence of a
   file with the specified name means that the results are present in the
   output directory.

Run Parametric Study
''''''''''''''''''''

Putting `get_run_info()` into our script allows running a parametric study using
`soops-run`::

  $ soops-run -h
  usage: soops-run [-h] [--dry-run] [-r {0,1,2}] [-n int]
                   [--run-function {subprocess.run,psutil.Popen,os.system}]
                   [-t float]
                   [--generate-pars dict-like: function=function_name,par0=val0,... or str]
                   [-c key1+key2+..., ...]
                   [--compute-pars dict-like: class=class_name,par0=val0,...]
                   [-s str] [--silent] [--shell] [-o path]
                   conf run_mod

  Run parametric studies.

  positional arguments:
    conf                  a dict-like parametric study configuration or a study
                          configuration file name
    run_mod               the importable script/module with get_run_info()

  options:
    -h, --help            show this help message and exit
    --dry-run             perform a trial run with no commands executed
    -r {0,1,2}, --recompute {0,1,2}
                          recomputation strategy: 0: do not recompute, 1:
                          recompute only if is_finished() returns False, 2:
                          always recompute [default: 1]
    -n int, --n-workers int
                          the number of dask workers [default: 2]
    --run-function {subprocess.run,psutil.Popen,os.system}
                          function for running the parameterized command
                          [default: subprocess.run]
    -t float, --timeout float
                          if given, the timeout in seconds; requires setting
                          --run-function=psutil.Popen
    --generate-pars dict-like: function=function_name,par0=val0,... or str
                          if given, generate values of parameters using the
                          specified function; the generated parameters must be
                          set to @generate in the parametric study
                          configuration. Alternatively, a section key in a study
                          configuration file.
    -c key1+key2+..., ..., --contract key1+key2+..., ...
                          list of option keys that should be contracted to vary
                          in lockstep
    --compute-pars dict-like: class=class_name,par0=val0,...
                          if given, compute additional parameters using the
                          specified class
    -s str, --study str   study key when parameter sets are given by a study
                          configuration file
    --silent              do not print messages to screen
    --shell               run ipython shell after all computations
    -o path, --output-dir path
                          output directory [default: output]

In our case (the arguments with no value (flags) can be specified either as
``'@defined'`` or ``'@undefined'``)::

  soops-run -r 1 -n 3 -c='--switch + --seed' -o output "python='python3', output_dir='output/study/%s', --num=[100,1000,10000], --repeat=[10,20], --switch=['@undefined', '@defined', '@undefined', '@defined'], --seed=['@undefined', '@undefined', 12345, 12345], --host=['random', 'first'], --silent=@defined, --no-show=@defined" soops/examples/monty_hall.py

This command runs our script using three dask workers (``-n 3`` option) and
produces a directory for each parameter set::

  $ ls output/study/
  000-a96da9ba9ee27be055166a3f64f641d2  024-04c03c243eea170aaf2bcb6ad27b3553
  001-683dca8ed500db306362a9aed10876f6  025-59e26f89a5c4ff98b7e6bb3ab23369f8
  002-ed389f6f0b8f8656891dd2785f1eca9d  026-2217691056bf0ab11e64e6125abefb3a
  003-79cacb84c4fb358ba7431c60ee0bd6b9  027-a6d738c5a732b6c4cb130fa5a36cc525
  004-b9eec14c504e6a2d2b23448e7e4073e5  028-b6e0b030fda9d3d2eff55b6e29b3438b
  005-18248bf1c448246a741e8c0e9099934e  029-b4754c22669dbcad1e2a66f3bc94ced1
  006-b4c8cd0dec50799eb77dbfff487f2608  030-2af86aa5329a5b69c9b5feaf989e9652
  007-c3ce48559a177812a9934f76cf45bd72  031-30ee10ccca40840a52c574defe622c51
  008-e15c5fdad6d3c6a4ef09e97d3d43ff40  032-03d1cf2e9d1a7b3c1692d52003669099
  009-889ac21d981a4aaee5ecb338d3845bea  033-d92c598ca65e423b71b4c52b0c99ca45
  010-8fcac713a518bfac737f72efd9c7a72f  034-b2bd1faf276b8072f97a6ed6ed8a4a3a
  011-20daf27250fd56ecbccc3cd674229ad6  035-2f676e229524da9f68b07024d2db9463
  012-7acbff8c9aebc0e51dc401eb1166fe41  036-7c97e794bfd442cf4062291755220fd8
  013-90701b1a193e4bcb3302348a0a89f26d  037-d8e72d98da5130bbab551f1fc6e6ee18
  014-c25408ecfd430a8550962fe2aab6be7e  038-d0759f20f4f5029d2a7dd872bda4d330
  015-94ef45703ab2c96dd05add8a78b9d24f  039-372d0de1c0aee25861e60d96f13700ac
  016-1a3a94fbad26651b1cce1f031b414841  040-6c390a413d099e4e41d00bbc79e36e23
  017-7f8d639a85f5e61685d0c919e4053270  041-ebff32cd18b128a5e9eb790b2d61682e
  018-2e38d5032b32c9e8c6c1f34c54ad5f07  042-fceb9e65b951a4b44000589cd7cd5fb1
  019-bbf0acb5c529bbb75dad3b9e0814af1d  043-ff77579f63caa2569105f2c1d0edd157
  020-e4df1cc5ad45307f39c6a03235b3857d  044-f16c4a6c32cb350a004a9618254d79a4
  021-2f3ecfddc555d626a31bf812982bc877  045-cae7631f8cb0bbb257bf11d8d38b2611
  022-9d90968a994089d8cacfd3a092b083f9  046-97299823f49756637785d68a432f81d3
  023-8ffc3ecfdc3cd5cedc47d06ad1f61c46  047-82e6500591a10f967a7be1035407151a

The directory names consist of an integer allowing an easy location and a MD5
hash of the run parameters. In each directory, there are five files::

  $ ls output/study/000-*
  options.txt  output_log.txt  run.py  soops-parameters.csv  wins.png

three just like in the basic run above, `soops-parameters.csv`, where the run
parameters (mostly command line arguments) are stored by `soops-run` and
`run.py` script that can be used to manually rerun the run. For convenience,
parameters of all runs are collected in `all_parameters.csv` in the `soops-run`
output directory (`output` by default), using the data in all
`soops-parameters.csv` files found.

Our example script also stores the values of command line arguments in
``options.txt`` for possible re-runs and inspection::

  $ cat output/study/000-a96da9ba9ee27be055166a3f64f641d2/options.txt

  command line
  ------------

  "soops/examples/monty_hall.py" "output/study/000-a96da9ba9ee27be055166a3f64f641d2" "--host=random" "--num=100" "--repeat=10" "--no-show" "--silent"

  options
  -------

  host: random
  num: 100
  output_dir: output/study/000-a96da9ba9ee27be055166a3f64f641d2
  plot_opts: {'linewidth': 3, 'alpha': 0.5}
  repeat: 10
  seed: None
  show: False
  silent: True
  switch: False

Using Parametric Study Configuration Files
''''''''''''''''''''''''''''''''''''''''''

Instead of providing the parameter sets on the command line, a study
configuration file can be used. Then the same parametric study as above
can be run using::

  soops-run -r 1 -n 3 -c='--switch + --seed' --study=study -o output soops/examples/studies.cfg soops/examples/monty_hall.py

where ``soops/examples/studies.cfg`` contains::

  [study]
  python='python3'
  output_dir='output/study/%s'
  --num=[100,1000,10000]
  --repeat=[10,20]
  --switch=['@undefined', '@defined', '@undefined', '@defined']
  --seed=['@undefined', '@undefined', 12345, 12345]
  --host=['random', 'first']
  --silent=@defined
  --no-show=@defined

Several studies can be stored in a single file, see `soops/examples/studies.cfg
<soops/examples/studies.cfg>`_. See also the docstring of
`soops/examples/monty_hall.py <soops/examples/monty_hall.py>`_ for more
examples.

Show Parameters Used in Each Output Directory
'''''''''''''''''''''''''''''''''''''''''''''

Use ``soops-info`` to explain which parameters were used in the given output
directories::

  $ soops-info -h
  usage: soops-info [-h] [-e dirname [dirname ...]] [--shell] run_mod

  Get parametric study configuration information.

  positional arguments:
    run_mod               the importable script/module with get_run_info()

  optional arguments:
    -h, --help            show this help message and exit
    -e dirname [dirname ...], --explain dirname [dirname ...]
                          explain parameters used in the given output
                          directory/directories
    --shell               run ipython shell after all computations

::

  $ soops-info soops/examples/monty_hall.py -e output/study/000-*
  info: output/study/000-a96da9ba9ee27be055166a3f64f641d2/
  info:     finished: True
  info:         iset: 0
  info: *     --host: random
  info: *  --no-show: @defined
  info: *      --num: 100
  info: *   --repeat: 10
  info: *     --seed: @undefined
  info: *   --silent: @defined
  info: *   --switch: @undefined
  info: *     python: python3
  info:   output_dir: output/study/000-a96da9ba9ee27be055166a3f64f641d2
  info:   script_dir: soops/examples

A `*` denotes a parameter used in the parameterization of the example script,
other parameters are employed by `soops-run`.

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
  usage: soops-scoop [-h] [-s column[,column,...]]
                     [--filter filename[,filename,...]] [--no-plugins]
                     [--use-plugins name[,name,...] | --omit-plugins
                     name[,name,...]] [-p module] [--plugin-args dict-like]
                     [--results filename] [--no-csv] [-r | -u] [--write]
                     [--write-after-plugins] [--shell] [--debug] [-o path]
                     scoop_mod directories [directories ...]

  Scoop output files.

  positional arguments:
    scoop_mod             the importable script/module with get_scoop_info()
    directories           results directories. On "Argument list too long"
                          system error, enclose the directories matching pattern
                          in "", it will be expanded using glob.glob().

  options:
    -h, --help            show this help message and exit
    -s column[,column,...], --sort column[,column,...]
                          column keys for sorting of DataFrame rows
    --filter filename[,filename,...]
                          use only DataFrame rows with given files successfully
                          scooped
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
    --results filename    results file name [default: <output_dir>/results.h5]
    --no-csv              do not save results as CSV (use only HDF5)
    -r, --reuse           reuse previously scooped results file
    -u, --update          update previously scooped results file with results in
                          new directories. Results in previously existing
                          directories are reused (as with -r) without any
                          contents checking.
    --write               write results files even when results were loaded
                          using --reuse option
    --write-after-plugins
                          write the pandas HDF5 results file again after plugins
                          were applied
    --shell               run ipython shell after all computations
    --debug               automatically start debugger when an exception is
                          raised
    -o path, --output-dir path
                          output directory [default: .]

as follows::

  $ soops-scoop soops/examples/monty_hall.py output/study/ -s rdir -o output/study --no-plugins --shell

  <snip>

  Python 3.10.12 (main, Aug 31 2026, 10:18:17) [GCC 11.4.0]
  Type 'copyright', 'credits' or 'license' for more information
  IPython 8.39.0 -- An enhanced Interactive Python. Type '?' for help.

  In [1]: df.keys()
  Out[1]:
  Index(['rdir', 'rfiles', 'host', 'num', 'output_dir', 'plot_opts', 'repeat',
         'seed', 'show', 'silent', 'switch', 'elapsed', 'win_rate', 'time'],
        dtype='object')

  In [2]: df.win_rate.head()
  Out[2]:
  0    [0.32, 0.4, 0.38, 0.27, 0.31, 0.39, 0.25, 0.33...
  1    [0.64, 0.67, 0.68, 0.67, 0.73, 0.62, 0.66, 0.7...
  2    [0.32, 0.32, 0.32, 0.32, 0.32, 0.32, 0.32, 0.3...
  3    [0.68, 0.68, 0.68, 0.68, 0.68, 0.68, 0.68, 0.6...
  4    [0.28, 0.28, 0.35, 0.32, 0.29, 0.33, 0.29, 0.3...
  Name: win_rate, dtype: object

  In [3]: df.iloc[0]
  Out[3]:
  rdir          ~/projects/soops/output/study/000-a96da9ba9ee2...
  rfiles                            [options.txt, output_log.txt]
  host                                                     random
  num                                                         100
  output_dir    output/study/000-a96da9ba9ee27be055166a3f64f641d2
  plot_opts                        {'linewidth': 3, 'alpha': 0.5}
  repeat                                                       10
  seed                                                        NaN
  show                                                      False
  silent                                                     True
  switch                                                    False
  elapsed       [0.0031552709988318384, 0.0032349379907827824,...
  win_rate      [0.32, 0.4, 0.38, 0.27, 0.31, 0.39, 0.25, 0.33...
  time                                 2026-10-07 14:05:33.537072
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

  soops-scoop soops/examples/monty_hall.py output/study/ -s rdir -o output/study -r

reuses the ``output/study/results.h5`` file and plots the combined results:

.. image:: doc/readme/win_rates.png
   :alt: win_rates.png

It is possible to pass arguments to plugins using ``--plugin-args`` option, as
follows::

  soops-scoop soops/examples/monty_hall.py output/study/ -s rdir -o output/study -r --plugin-args=plot_win_rates={colormap_name='plasma'}

Notes
'''''

- The `get_run_info()`, `get_scoop_info()` and `get_plugin_info()` info
  function can be in different modules.
- The script that is being parameterized need not be a Python module - any
  executable which can be run from a command line can be used.

Special Argument Values
'''''''''''''''''''''''

- ``'@defined'`` denotes that a value-less argument is present.
- ``'@undefined'`` denotes that a value-less argument is not present.
- ``'@arange([start,] stop[, step,], dtype=None)'`` denotes values obtained by
  calling ``numpy.arange()`` with the given arguments.
- ``'@linspace(start, stop, num=50, endpoint=True, dtype=None, axis=0)'``
  denotes values obtained by calling ``numpy.linspace()`` with the given
  arguments.
- ``'@generate'`` denotes an argument whose values are generated, in connection
  with ``--generate-pars`` option, see below.

Generated Arguments
'''''''''''''''''''

Argument sequences can be generated using a function with the help of
``--generate-pars`` option. For example, the same results as above can be
achieved by defining a function that generates ``--switch`` and ``--seed``
arguments values:

.. code:: python

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

and then calling `soops-run` as follows::

  soops-run -r 1 -n 3 -c='--switch + --seed' -o output/study2 "python='python3', output_dir='output/study2/%s', --num=[100,1000,10000], --repeat=[10,20], --switch=@generate, --seed=@generate, --host=['random', 'first'], --silent=@defined, --no-show=@defined" --generate-pars="function=generate_seed_switch, seeds=['@undefined', 12345], switches=['@undefined', '@defined']" soops/examples/monty_hall.py

Notice the special ``@generate`` values of ``--switch`` and ``--seed``, and the
use of ``--generate-pars``: all key-value pairs, except the function name, are
passed into :func:``generate_seed_switch()`` in the ``args`` dict-like
argument.

The combined results can again be plotted using::

  soops-scoop soops/examples/monty_hall.py output/study2/0* -s rdir -o output/study2/

Computed Arguments
''''''''''''''''''

By using ``--compute-pars`` option it is possible to define arguments depending
on other arguments values in a more general way than with ``--contract``.
A callable class needs to be provided with the following structure:

.. code:: python

   class ComputePars:

       def __init__(self, args, par_seqs, key_order, options):
           """
           Called prior to the parametric study to pre-compute reusable data.
           """
           pass

       def __call__(self, all_pars):
           """
           Called for each parameter set of the study.
           """
           out = {}
           return out

Find Runs with Given Parameters
'''''''''''''''''''''''''''''''

For very large parametric studies, it might be impractical to view
`all_parameters.csv` directly when searching a directory of a run with given
parameters. The `soops-find` script can be used instead::

  $ soops-find -h
  usage: soops-find [-h] [-q pandas-query-expression]
                    [--engine {numexpr,python}] [-m {truncated,full,single}]
                    [-k KEY] [--shell]
                    directories [directories ...]

  Find parametric studies with parameters satisfying a given query.

  Option-like parameters are transformed to valid Python attribute names removing
  initial dashes and replacing other dashes by underscores. For example
  '--output-dir' becomes 'output_dir'.

  positional arguments:
    directories           one or more root directories with sub-directories
                          containing parametric study results

  options:
    -h, --help            show this help message and exit
    -q pandas-query-expression, --query pandas-query-expression
                          pandas query expression applied to collected
                          parameters
    --engine {numexpr,python}
                          pandas query evaluation engine [default: numexpr]
    -m {truncated,full,single}, --mode {truncated,full,single}
                          output mode [default: truncated]
    -k KEY, --key KEY     column key. If given, forces "single" output mode
                          [default: output_dir]
    --shell               run ipython shell after all computations

Without options, it loads all parameter sets found in given directories into
a DataFrame and launches the ipython shell::

  $ soops-find output/study
  find: 48 parameter sets stored in `apdf` DataFrame
  find: column names:
  Index(['finished', 'iset', 'host', 'no_show', 'num', 'repeat', 'seed',
         'silent', 'switch', 'python', 'output_dir', 'script_dir'],
        dtype='object')
  Using matplotlib backend: gtk3agg
  Python 3.10.12 (main, Aug 31 2026, 10:18:17) [GCC 11.4.0]
  Type 'copyright', 'credits' or 'license' for more information

  In [1]:

The ``--query`` option can be used to limit the search, for example::

  $ soops-find output/study -q "num==1000 & repeat==20 & seed==12345"

Additional Utilities
--------------------

- dict-like classes with attribute access, various utilities: `soops.base
  <https://github.com/rc/soops/blob/HEAD/soops/base.py>`_
- Command line options made simple: `soops.cliargs
  <https://github.com/rc/soops/blob/HEAD/soops/cliargs.py>`_
- LaTeX report generation utilities: `soops.formatting
  <https://github.com/rc/soops/blob/HEAD/soops/formatting.py>`_
- Timer class: `soops.timing
  <https://github.com/rc/soops/blob/HEAD/soops/timing.py>`_

See Also
--------

- `automan <https://github.com/pypr/automan>`_
