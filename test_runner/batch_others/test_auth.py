from contextlib import closing
from typing import Iterator
from uuid import UUID, uuid4
from fixtures.zenith_fixtures import ZenithEnvBuilder
from requests.exceptions import HTTPError
import pytest

pytest_plugins = ("fixtures.zenith_fixtures")


def test_pageserver_auth(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.pageserver_auth_enabled = True
    env = zenith_env_builder.init()

    ps = env.pageserver

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant.hex)
    tenant_http_client = env.pageserver.http_client(tenant_token)

    invalid_tenant_token = env.auth_keys.generate_tenant_token(uuid4().hex)
    invalid_tenant_http_client = env.pageserver.http_client(invalid_tenant_token)

    management_token = env.auth_keys.generate_management_token()
    management_http_client = env.pageserver.http_client(management_token)

    # this does not invoke auth check and only decodes jwt and checks it for validity
    # check both tokens
    ps.safe_psql("set FOO", password=tenant_token)
    ps.safe_psql("set FOO", password=management_token)

    # tenant can create branches
    tenant_http_client.timeline_create(timeline_id=uuid4(),
                                       tenant_id=env.initial_tenant,
                                       ancestor_timeline_id=env.initial_timeline)
    # console can create branches for tenant
    management_http_client.timeline_create(timeline_id=uuid4(),
                                           tenant_id=env.initial_tenant,
                                           ancestor_timeline_id=env.initial_timeline)

    # fail to create branch using token with different tenant_id
    with pytest.raises(HTTPError, match='403 Client Error: Forbidden for url: *.?'):
        invalid_tenant_http_client.timeline_create(timeline_id=uuid4(),
                                                   tenant_id=env.initial_tenant,
                                                   ancestor_timeline_id=env.initial_timeline)

    # create tenant using management token
    management_http_client.tenant_create(uuid4())

    # fail to create tenant using tenant token
    with pytest.raises(HTTPError, match='403 Client Error: Forbidden for url: *.?'):
        tenant_http_client.tenant_create(uuid4())


@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_compute_auth_to_pageserver(zenith_env_builder: ZenithEnvBuilder, with_wal_acceptors: bool):
    zenith_env_builder.pageserver_auth_enabled = True
    if with_wal_acceptors:
        zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    branch = f"test_compute_auth_to_pageserver{with_wal_acceptors}"
    new_timeline_id = env.zenith_cli.create_timeline()
    pg = env.postgres.create_start(branch, timeline=new_timeline_id)

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            assert cur.fetchone() == (5000050000, )
