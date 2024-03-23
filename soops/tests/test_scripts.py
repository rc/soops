import os
import pytest

cmd_run0 = r"""-r 1 -n 3 -c=--switch+--seed -o {output_dir} python='python3',output_dir='{output_dir}/study0/%s',--num='@linspace(100,1000,2,dtype=np.int32)',--repeat=5,--switch=['@undefined','@defined'],--seed=['@undefined',12345],--silent=@defined,--no-show=@defined {soops_dir}/examples/monty_hall.py"""

cmd_run1 = r"""-r 1 -n 3 -c=--switch+--seed --study=study-test -o {output_dir} {output_dir}/studies.cfg {soops_dir}/examples/monty_hall.py"""

study_cfg = r"""
[study-test]
  python='python3'
  output_dir='{output_dir}/study1/%s'
  --num='@linspace(100,1000,2,dtype=np.int32)'
  --repeat=5
  --switch=['@undefined', '@defined']
  --seed=['@undefined', 12345]
  --silent=@defined
  --no-show=@defined
"""

cmd_scoop0 = r"""{soops_dir}/examples/monty_hall.py {output_dir}/study0/ -s rdir -o {output_dir}/study0 --omit-plugins=show_figures --plugin-args=plot_win_rates={{colormap_name='tab10:kind=qualitative'}}"""

cmd_scoop1 = r"""{soops_dir}/examples/monty_hall.py {output_dir}/study1/ -s rdir -o {output_dir}/study1 --omit-plugins=show_figures --plugin-args=plot_win_rates={{colormap_name='tab10:kind=qualitative'}}"""

cmd_info = r"""{soops_dir}/examples/monty_hall.py -e {output_dir}/study0/000-5adf4124d4e3e519e6eb49f2f0992ee1"""

cmd_find = r"""--query=num==1000&repeat==20&seed==12345 {output_dir}/study0"""

cmd_jobs = r"""-v"""

@pytest.fixture(scope='session')
def soops_dir():
    return os.path.normpath(os.path.join(os.path.dirname(__file__), '../'))

@pytest.fixture(scope='session')
def output_dir(tmpdir_factory):
    return tmpdir_factory.mktemp('output')

def test_run_parametric(soops_dir, output_dir):
    import soops.run_parametric as rp
    from soops import locate_files

    print(soops_dir)
    print(output_dir)
    options = rp.parse_args(args=cmd_run0
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    rp.run_parametric(options)

    results = list(locate_files('wins.png', os.path.join(output_dir, 'study0')))
    assert len(results) == 4

def test_run_parametric_cfg(soops_dir, output_dir):
    import soops.run_parametric as rp
    from soops import locate_files

    print(soops_dir)
    print(output_dir)

    with open('{output_dir}/studies.cfg'.format(output_dir=output_dir), 'w') as fd:
        fd.write(study_cfg.format(output_dir=output_dir))

    options = rp.parse_args(args=cmd_run1
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    rp.run_parametric(options)

    results = list(locate_files('wins.png', os.path.join(output_dir, 'study1')))
    assert len(results) == 4

def test_compare_runs(output_dir):
    from soops import locate_files

    dirs0 = sorted(os.path.basename(ii) for ii in
                   locate_files('wins.png', os.path.join(output_dir, 'study0')))
    dirs1 = sorted(os.path.basename(ii) for ii in
                   locate_files('wins.png', os.path.join(output_dir, 'study1')))
    assert dirs0 == dirs1

@pytest.mark.parametrize('command', [cmd_scoop0, cmd_scoop1])
def test_scoop_outputs(command, soops_dir, output_dir):
    import soops.scoop_outputs as so

    print(soops_dir)
    print(output_dir)
    options = so.parse_args(args=command
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    so.scoop_outputs(options)

    assert so.op.exists(so.op.join(output_dir, 'study0/win_rates.png'))

def test_print_info(soops_dir, output_dir):
    import soops.print_info as pi

    print(soops_dir)
    print(output_dir)
    options = pi.parse_args(args=cmd_info
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    pi.print_info(options)

def test_find_studies(soops_dir, output_dir):
    import soops.find_studies as fs

    print(soops_dir)
    print(output_dir)
    options = fs.parse_args(args=cmd_find
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    apdf = fs.find_studies(options)
    assert len(apdf) == 4

def test_show_jobs(soops_dir, output_dir):
    import soops.show_jobs as sj

    print(soops_dir)
    print(output_dir)
    options = sj.parse_args(args=cmd_jobs.split())
    jobs, infos = sj.show_jobs(options)
    assert isinstance(jobs, list)
    assert isinstance(infos, list)
