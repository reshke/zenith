import os

from fixtures.utils import mkdir_if_needed
from fixtures.zenith_fixtures import ZenithEnv, base_dir, pg_distrib_dir

pytest_plugins = ("fixtures.zenith_fixtures")


def test_isolation(zenith_simple_env: ZenithEnv, test_output_dir, pg_bin, capsys):
    env = zenith_simple_env

    env.zenith_cli.create_branch("test_isolation", "empty")
    # Connect to postgres and create a database called "regression".
    # isolation tests use prepared transactions, so enable them
    pg = env.postgres.create_start('test_isolation', config_lines=['max_prepared_transactions=100'])
    pg.safe_psql('CREATE DATABASE isolation_regression')

    # Create some local directories for pg_isolation_regress to run in.
    runpath = os.path.join(test_output_dir, 'regress')
    mkdir_if_needed(runpath)
    mkdir_if_needed(os.path.join(runpath, 'testtablespace'))

    # Compute all the file locations that pg_isolation_regress will need.
    build_path = os.path.join(pg_distrib_dir, 'build/src/test/isolation')
    src_path = os.path.join(base_dir, 'vendor/postgres/src/test/isolation')
    bindir = os.path.join(pg_distrib_dir, 'bin')
    schedule = os.path.join(src_path, 'isolation_schedule')
    pg_isolation_regress = os.path.join(build_path, 'pg_isolation_regress')

    pg_isolation_regress_command = [
        pg_isolation_regress,
        '--use-existing',
        '--bindir={}'.format(bindir),
        '--dlpath={}'.format(build_path),
        '--inputdir={}'.format(src_path),
        '--schedule={}'.format(schedule),
    ]

    env_vars = {
        'PGPORT': str(pg.port),
        'PGUSER': pg.username,
        'PGHOST': pg.host,
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_isolation_regress_command, env=env_vars, cwd=runpath)
