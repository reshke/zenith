from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder


@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_tenants_normal_work(zenith_env_builder: ZenithEnvBuilder, with_wal_acceptors: bool):
    if with_wal_acceptors:
        zenith_env_builder.num_safekeepers = 3

    env = zenith_env_builder.init()
    """Tests tenants with and without wal acceptors"""
    tenant_1 = env.create_tenant()
    tenant_2 = env.create_tenant()

    env.zenith_cli.create_branch(f"test_tenants_normal_work_with_wal_acceptors{with_wal_acceptors}",
                                 "main",
                                 tenant_id=tenant_1)
    env.zenith_cli.create_branch(f"test_tenants_normal_work_with_wal_acceptors{with_wal_acceptors}",
                                 "main",
                                 tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start(
        f"test_tenants_normal_work_with_wal_acceptors{with_wal_acceptors}",
        None,  # branch name, None means same as node name
        tenant_1,
    )
    pg_tenant2 = env.postgres.create_start(
        f"test_tenants_normal_work_with_wal_acceptors{with_wal_acceptors}",
        None,  # branch name, None means same as node name
        tenant_2,
    )

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000, )
