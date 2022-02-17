import json
from uuid import uuid4, UUID
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder, ZenithPageserverHttpClient
from typing import cast
import pytest, psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


def check_client(client: ZenithPageserverHttpClient, initial_tenant: UUID):
    client.check_status()

    # check initial tenant is there
    assert initial_tenant.hex in {t['id'] for t in client.tenant_list()}

    # create new tenant and check it is also there
    tenant_id = uuid4()
    client.tenant_create(tenant_id)
    assert tenant_id.hex in {t['id'] for t in client.tenant_list()}

    # check its timelines
    timelines = client.timeline_list(tenant_id)
    assert len(timelines) > 0
    for timeline in timelines:
        timeline_id_str = str(timeline['timeline_id'])
        timeline_details = client.timeline_detail(tenant_id=tenant_id,
                                                  timeline_id=UUID(timeline_id_str))
        assert timeline_details['type'] == 'Local'
        assert timeline_details['tenant_id'] == tenant_id.hex
        assert timeline_details['timeline_id'] == timeline_id_str

    # create timeline
    timeline_id = uuid4()
    client.timeline_create(tenant_id=tenant_id, timeline_id=timeline_id)

    # check it is there
    assert timeline_id.hex in {b['timeline_id'] for b in client.timeline_list(tenant_id)}


def test_pageserver_http_api_client(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    client = env.pageserver.http_client()
    check_client(client, env.initial_tenant)


def test_pageserver_http_api_client_auth_enabled(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.pageserver_auth_enabled = True
    env = zenith_env_builder.init()

    management_token = env.auth_keys.generate_management_token()

    client = env.pageserver.http_client(auth_token=management_token)
    check_client(client, env.initial_tenant)
