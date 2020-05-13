import prefect
import pytest

import prefect_server
from prefect_server.utilities.configuration import set_temporary_config


def test_leaves_prefect_config_alone():
    prefect_config = prefect.config.copy()

    with set_temporary_config({"services.host": "localhost probably"}):
        assert prefect_config == prefect.config


def test_changes_server_config():

    server_config = prefect_server.config.copy()

    with set_temporary_config({"services.host": "localhost probably"}):
        assert server_config != prefect_server.config
        assert prefect_server.config.services.host == "localhost probably"
        assert server_config.services.host != "localhost probably"

    assert prefect_server.config.services.host == server_config.services.host


def test_set_temporary_config_is_temporary():
    server_config = prefect_server.config.copy()

    with set_temporary_config({"services.host": "localhost probably"}):
        with set_temporary_config({"services.host": "definitely localhost"}):
            assert prefect_server.config.services.host == "definitely localhost"
        assert prefect_server.config.services.host == "localhost probably"

    assert prefect_server.config.services.host not in [
        "localhost probably",
        "definitely localhost",
    ]


def test_set_temporary_config_can_invent_new_settings():
    with set_temporary_config({"plugins.new_plugin.nested.val": "5"}):
        assert prefect_server.config.plugins.new_plugin.nested.val == "5"

    with pytest.raises(AttributeError):
        assert prefect.config.flows.nested.nested_again.val == "5"


def test_set_temporary_config_with_multiple_keys():
    with set_temporary_config({"x.y.z": 1, "a.b.c": 2}):
        assert prefect_server.config.x.y.z == 1
        assert prefect_server.config.a.b.c == 2
