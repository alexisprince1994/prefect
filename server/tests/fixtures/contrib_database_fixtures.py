import pytest

from prefect_server.contrib import api as contrib_api
from prefect_server.contrib.database import models as contrib_models


@pytest.fixture
async def flow_concurrency_limit() -> contrib_models.FlowConcurrencyLimit:

    concurrency_limit_id = await contrib_api.concurrency_limits.create_flow_concurrency_limit(
        "foo",
        description="A flow concurrency limit created from Prefect Server's test suite.",
        limit=1,
    )

    populated_concurrency_limit = await contrib_models.FlowConcurrencyLimit.where(
        id=concurrency_limit_id
    ).first({"id", "name", "description", "limit"})
    return populated_concurrency_limit


@pytest.fixture
async def flow_concurrency_limit_2() -> contrib_models.FlowConcurrencyLimit:
    concurrency_limit_id = await contrib_api.concurrency_limits.create_flow_concurrency_limit(
        "bar",
        description="A second flow concurrency limit created from Prefect Server's test suite",
        limit=1,
    )

    populated_concurrency_limit = await contrib_models.FlowConcurrencyLimit.where(
        id=concurrency_limit_id
    ).first({"id", "name", "description", "limit"})
    return populated_concurrency_limit
