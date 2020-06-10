import os
import pytest

cmd_run = r"""-r 1 -n 3 -c=--switch+--seed -o {output_dir} python='python3',output_dir='{output_dir}/study/%s',--num=[100,1000],--repeat=5,--switch=['@undefined','@defined'],--seed=['@undefined',12345],--silent=@defined,--no-show=@defined {soops_dir}/examples/monty_hall.py"""

cmd_scoop = r"""{soops_dir}/examples/monty_hall.py {output_dir}/study/ -s rdir -o {output_dir}/study --omit-plugins=show_figures --plugin-args=plot_win_rates={{colormap_name='plasma'}}"""

cmd_info = r"""{soops_dir}/examples/monty_hall.py -e {output_dir}/study/0_0_1_0_0_1_0_1_0/"""

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
    options = rp.parse_args(args=cmd_run
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    rp.run_parametric(options)

    results = list(locate_files('wins.png', output_dir))
    assert len(results) == 4

def test_scoop_outputs(soops_dir, output_dir):
    import soops.scoop_outputs as so

    print(soops_dir)
    print(output_dir)
    options = so.parse_args(args=cmd_scoop
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    so.scoop_outputs(options)

    assert so.op.exists(so.op.join(output_dir, 'study/win_rates.png'))

def test_print_info(soops_dir, output_dir):
    import soops.print_info as pi

    print(soops_dir)
    print(output_dir)
    options = pi.parse_args(args=cmd_info
                            .format(soops_dir=soops_dir,
                                    output_dir=output_dir).split())
    pi.print_info(options)
