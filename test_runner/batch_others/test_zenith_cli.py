import json
import uuid
import requests

from psycopg2.extensions import cursor as PgCursor
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder
from typing import cast

pytest_plugins = ("fixtures.zenith_fixtures")


def helper_compare_timeline_list(page_server_cur: PgCursor,
                                 env: ZenithEnv,
                                 initial_tenant: uuid.UUID):
    """
    Compare timelines list returned by CLI and directly via API.
    Filters out timelines created by other tests.
    """

    timelines_cli = env.zenith_cli.list_timelines()
    timelines_cli = [
        b for b in timelines_cli if b.startswith('test_cli_') or b in ('empty', 'main')
    ]

    timelines_cli_with_tenant_arg = env.zenith_cli.list_timelines(initial_tenant)
    timelines_cli_with_tenant_arg = [
        b for b in timelines_cli if b.startswith('test_cli_') or b in ('empty', 'main')
    ]

    assert timelines_cli == timelines_cli_with_tenant_arg


def test_cli_timeline_list(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    page_server_conn = env.pageserver.connect()
    page_server_cur = page_server_conn.cursor()

    # Initial sanity check
    helper_compare_timeline_list(page_server_cur, env, env.initial_tenant)

    # Create a branch for us
    main_timeline_id = env.zenith_cli.create_timeline()
    helper_compare_timeline_list(page_server_cur, env, env.initial_tenant)

    # Create a nested branch
    nested_timeline_id = env.zenith_cli.create_timeline(ancestor_timeline_id=main_timeline_id)
    helper_compare_timeline_list(page_server_cur, env, env.initial_tenant)

    # Check that all new branches are visible via CLI
    timelines_cli = env.zenith_cli.list_timelines()

    assert main_timeline_id.hex in timelines_cli
    assert nested_timeline_id.hex in timelines_cli


def helper_compare_tenant_list(page_server_cur: PgCursor, env: ZenithEnv):
    page_server_cur.execute(f'tenant_list')
    tenants_api = sorted(
        map(lambda t: cast(str, t['id']), json.loads(page_server_cur.fetchone()[0])))

    res = env.zenith_cli.list_tenants()
    tenants_cli = sorted(map(lambda t: t.split()[0], res.stdout.splitlines()))

    assert tenants_api == tenants_cli


def test_cli_tenant_list(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    page_server_conn = env.pageserver.connect()
    page_server_cur = page_server_conn.cursor()

    # Initial sanity check
    helper_compare_tenant_list(page_server_cur, env)

    # Create new tenant
    tenant1 = uuid.uuid4()
    env.zenith_cli.create_tenant(tenant_id=tenant1)

    # check tenant1 appeared
    helper_compare_tenant_list(page_server_cur, env)

    # Create new tenant
    tenant2 = uuid.uuid4()
    env.zenith_cli.create_tenant(tenant_id=tenant2)

    # check tenant2 appeared
    helper_compare_tenant_list(page_server_cur, env)

    res = env.zenith_cli.list_tenants()
    tenants = sorted(map(lambda t: t.split()[0], res.stdout.splitlines()))

    assert env.initial_tenant.hex in tenants
    assert tenant1.hex in tenants
    assert tenant2.hex in tenants


def test_cli_ipv4_listeners(zenith_env_builder: ZenithEnvBuilder):
    # Start with single sk
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()

    # Connect to sk port on v4 loopback
    res = requests.get(f'http://127.0.0.1:{env.safekeepers[0].port.http}/v1/status')
    assert res.ok

    # FIXME Test setup is using localhost:xx in ps config.
    # Perhaps consider switching test suite to v4 loopback.

    # Connect to ps port on v4 loopback
    # res = requests.get(f'http://127.0.0.1:{env.pageserver.service_port.http}/v1/status')
    # assert res.ok
