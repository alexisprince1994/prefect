# Licensed under the Prefect Community License, available at
# https://www.prefect.io/legal/prefect-community-license

import asyncio
import uuid
from typing import List

import pendulum
import pytest
from asynctest import CoroutineMock
from prefect.engine.state import (
    Failed,
    Retrying,
    Running,
    Scheduled,
    State,
    Submitted,
    Success,
)

from prefect_server import api, config
from prefect_server.api import runs
from prefect_server.contrib import api as contrib_api
from prefect_server.contrib.database import models as contrib_models
from prefect_server.database import models, orm
from prefect_server.utilities.configuration import set_temporary_config


@pytest.fixture
def plugin_config():
    """
    Overrides the default config with the minimal configuration 
    required to make the plugin work.
    """
    cfg = {"contrib.flow_concurrency.enabled": True, "queued_runs_returned_limit": 25}
    with set_temporary_config(cfg):

        yield


class TestPluginFeatureFlagged:
    """
    This plugin requires changes to the "core" code in two places:
        - api.states.set_flow_run_state
        - api.runs.get_runs_in_queue

    These tests are focused around making sure that without enabling
    the plugin, the plugin's code does not run.
    """

    def test_disabled_by_default(self):
        assert config.contrib.flow_concurrency.enabled is False

    @pytest.mark.parametrize(
        "concurrency_limit",
        [{"foo": 1, "bar": 1}, {}, {"foo": 1}, {"foo": 0, "bar": 0}],
    )
    async def test_doesnt_check_concurrency_on_flow_state_change_if_disabled(
        self, labeled_flow_run_id: str, monkeypatch, concurrency_limit
    ):
        """
        This test should ensure that regardless of whether or not the
        flow's execution environment _would_ pass the concurrency checks,
        that if the plugin is disabled, we don't get far enough to check.
        """

        mock_concurrency_check = CoroutineMock(return_value=concurrency_limit)
        monkeypatch.setattr(
            "prefect_server.api.states.contrib_api.concurrency_limits.get_available_flow_concurrency",
            mock_concurrency_check,
        )
        await api.states.set_flow_run_state(labeled_flow_run_id, Running())
        mock_concurrency_check.assert_not_called()

    @pytest.mark.parametrize(
        "concurrency_limit",
        [{"foo": 1, "bar": 1}, {}, {"foo": 1}, {"foo": 0, "bar": 0}],
    )
    async def test_doesnt_check_concurrency_on_queued_runs_if_disabled(
        self, labeled_flow_id: str, monkeypatch, concurrency_limit: dict
    ):
        """
        Tests to make sure that whether or not the flow's execution environment
        _would_ pass the concurrency checks that if the plugin is disabled,
        we don't get far enough to check.
        """

        # Concurrency limits of 0 or 1 would only expect 0 or 1 flow runs to come back
        await asyncio.gather(
            *[api.runs.create_flow_run(labeled_flow_id) for _ in range(5)]
        )
        mock_concurrency_check = CoroutineMock(return_value=concurrency_limit)
        monkeypatch.setattr(
            "prefect_server.api.runs.contrib_api.concurrency_limits.get_available_flow_concurrency",
            mock_concurrency_check,
        )
        flow_run_ids = await api.runs.get_runs_in_queue(labels=["foo", "bar"])
        mock_concurrency_check.assert_not_called()
        assert len(flow_run_ids) == 5


class TestCreateFlowConcurrencyLimit:
    async def test_creates_flow_concurrency_limit(self):

        flow_concurrency_limit_name = uuid.uuid4().hex
        description = (
            "A flow concurrency limit created from Prefect Server's test suite."
        )
        limit = 5

        concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()

        concurrency_limit_id = await contrib_api.concurrency_limits.create_flow_concurrency_limit(
            flow_concurrency_limit_name, limit=limit, description=description
        )

        new_concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()
        assert (concurrency_limit_count + 1) == new_concurrency_limit_count

        concurrency_limit = await contrib_models.FlowConcurrencyLimit.where(
            where={"id": {"_eq": concurrency_limit_id}}
        ).first({"id", "name", "description", "limit"})
        assert concurrency_limit is not None
        assert concurrency_limit.id == concurrency_limit_id
        assert concurrency_limit.description == description
        assert concurrency_limit.limit == limit
        assert concurrency_limit.name == flow_concurrency_limit_name

    @pytest.mark.parametrize("limit", [0, -5])
    async def test_raises_error_on_bad_limit(self, limit: int):

        flow_concurrency_limit_name = "test concurrency limit"
        description = (
            "A flow concurrency limit created from Prefect Server's test suite."
        )

        concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()

        with pytest.raises(ValueError):
            concurrency_limit_id = await contrib_api.concurrency_limits.create_flow_concurrency_limit(
                flow_concurrency_limit_name, limit=limit, description=description
            )

        new_concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()
        # Making sure no create happened
        assert concurrency_limit_count == new_concurrency_limit_count

    async def test_conflicting_flow_concurrency_name_raises_error(self):

        flow_concurrency_limit_name = uuid.uuid4().hex
        description = (
            "A flow concurrency limit created from Prefect Server's test suite."
        )
        limit = 5

        concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()

        concurrency_limit_id = await contrib_api.concurrency_limits.create_flow_concurrency_limit(
            flow_concurrency_limit_name, limit=limit, description=description
        )

        new_concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()

        assert (concurrency_limit_count + 1) == new_concurrency_limit_count

        with pytest.raises(ValueError):
            concurrency_limit_id = await contrib_api.concurrency_limits.create_flow_concurrency_limit(
                flow_concurrency_limit_name, limit=limit, description=description
            )

        newest_concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()
        assert newest_concurrency_limit_count == new_concurrency_limit_count


class TestDeleteFlowConcurrencyLimit:
    async def test_delete_existing(
        self, flow_concurrency_limit: contrib_models.FlowConcurrencyLimit
    ):

        concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()
        deleted = await contrib_api.concurrency_limits.delete_flow_concurrency_limit(
            flow_concurrency_limit.id
        )
        assert deleted is True

        new_concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()

        assert concurrency_limit_count == (new_concurrency_limit_count + 1)

    async def test_delete_bad_id(
        self, flow_concurrency_limit: contrib_models.FlowConcurrencyLimit
    ):

        concurrency_limit = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()
        deleted = await contrib_api.concurrency_limits.delete_flow_concurrency_limit(
            uuid.uuid4().hex
        )
        assert deleted is False

        new_concurrency_limit_count = await orm.ModelQuery(
            model=contrib_models.FlowConcurrencyLimit
        ).count()

        assert concurrency_limit == new_concurrency_limit_count


class TestGetAvailableConcurrencyLimits:
    async def test_raises_error_without_params(
        self, flow_concurrency_limit: contrib_models.FlowConcurrencyLimit
    ):

        with pytest.raises(TypeError):
            available_concurrency_limits = (
                await contrib_api.concurrency_limits.get_available_flow_concurrency()
            )

    @pytest.mark.parametrize(
        "labels", [["foo"], ["bar"], ["foo", "bar"], ["bar", "foo"]]
    )
    async def test_contains_partial_matches_in(
        self, labeled_flow_id: str, labels: List[str]
    ):
        """
        This test doesn't necessarily test our code, but it does test
        that our understanding of how Hasura handles filtering a JSONB
        for the existance of one or more values that should return a match
        given the structure:
        {
            environment: {labels: [foo, bar, value,3]}
        }
        These find the match because they're either a subset or exact
        match. Order doesn't matter.
        """

        res = await models.Flow.where(
            {"environment": {"_contains": {"labels": labels}}}
        ).count()
        assert res == 1

    @pytest.mark.parametrize("labels", [["foo", "bar", "baz"], ["baz"], ["foo", "baz"]])
    async def test_contains_not_full_match(
        self, labeled_flow_id: str, labels: List[str]
    ):
        """
        This test doesn't necessarily test our code, but it does test
        that our understanding of how Hasura handles filtering a JSONB
        for the existance of one or more values that should not return 
        a match given the structure:
        {
            environment: {labels: [value, value2, value,3]}
        }
        These don't find a match because the _contains filter needs
        to be an exact match or subset.

        """
        res = await models.Flow.where(
            {"environment": {"_contains": {"labels": labels}}}
        ).count()
        assert res == 0

    async def test_only_includes_running_states(
        self,
        labeled_flow_id: str,
        labeled_flow_run_id: str,
        labeled_flow_run_id_2: str,
        flow_concurrency_limit: contrib_models.FlowConcurrencyLimit,
    ):
        """
        Tests to make sure that only running states count towards
        the concurrency limit's usage.
        """

        # Setting the limit higher so we can actually observe the changes
        await contrib_models.FlowConcurrencyLimit.where(
            id=flow_concurrency_limit.id
        ).update(set={"limit": 10})

        available_concurrency_limits = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name]
        )

        assert available_concurrency_limits[flow_concurrency_limit.name] == 9

        # Now that the flow is running, it should take up a spot
        await api.states.set_flow_run_state(labeled_flow_run_id, Running())

        new_available_concurrency_limits = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name]
        )
        # We should have 1 less spot due to the new flow run
        old = available_concurrency_limits[flow_concurrency_limit.name]
        new = new_available_concurrency_limits[flow_concurrency_limit.name]
        assert old == new + 1

    async def test_only_includes_labeled_runs(
        self,
        flow_run_id: str,
        labeled_flow_id: str,
        labeled_flow_run_id: str,
        flow_concurrency_limit: contrib_models.FlowConcurrencyLimit,
    ):
        """
        Tests to make sure that only flows using the environment that is tagged
        counts towards the concurrency limit's capacity.
        """

        # Setting the limit higher so we can actually observe the changes
        await contrib_models.FlowConcurrencyLimit.where(
            id=flow_concurrency_limit.id
        ).update(set={"limit": 10})

        available_concurrency_limits = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name]
        )

        assert available_concurrency_limits[flow_concurrency_limit.name] == 10

        # Marking the flow that _doesn't_ use the concurrency limit as running
        await api.states.set_flow_run_state(flow_run_id, Running())

        new_available_concurrency_limits = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name]
        )
        # No flow concurrency limit should be taken because it isn't tagged w/ the label
        assert available_concurrency_limits == new_available_concurrency_limits


@pytest.mark.usefixtures("plugin_config")
class TestFlowRunStatesConcurrency:
    """
    When promoted from contrib, to be placed in `api.test_states.TestFlowRunStates`
    """

    mock_location = "prefect_server.api.states.contrib_api.concurrency_limits.get_available_flow_concurrency"

    async def test_labeled_flow_run_has_expected_labels(self, labeled_flow_id: str):
        """
        Simple test to make sure that our `labeled_flow_id`'s environment labels
        haven't changed. If they change, the mocked tests below will likely
        become invalid.
        """
        flow = await models.Flow.where(id=labeled_flow_id).first({"id", "environment"})
        assert set(flow.environment["labels"]) == set(["foo", "bar"])

    @pytest.mark.parametrize(
        "state", [Failed(), Success(), Submitted(), Scheduled(), Retrying()]
    )
    async def test_doesnt_check_concurrency_on_not_running(
        self, labeled_flow_run_id: str, state: State, monkeypatch
    ):
        """
        This test should make sure that the flow run concurrency checks
        don't occur when being transitioned to any state other than
        `Running`.

        In order to test this properly, we do need to peek into the implementation
        of making sure the flow run _would_ normally trigger a concurrency check.
        """

        mock_concurrency_check = CoroutineMock(return_value={})
        monkeypatch.setattr(
            self.mock_location, mock_concurrency_check,
        )
        await api.states.set_flow_run_state(labeled_flow_run_id, state)
        mock_concurrency_check.assert_not_called()

        await api.states.set_flow_run_state(labeled_flow_run_id, Running())
        mock_concurrency_check.assert_called_once()

    async def test_raises_error_on_failing_concurrency_check(
        self, labeled_flow_run_id: str, monkeypatch
    ):
        """
        This test should check to make sure that if a flow's labels
        don't have concurrency slots available that we fail the transition
        into a `Running` state and raise an error.
        """
        mock_concurrency_check = CoroutineMock(return_value={"foo": 0, "bar": 0})
        monkeypatch.setattr(
            self.mock_location, mock_concurrency_check,
        )
        with pytest.raises(ValueError, match="concurrency limit"):
            await api.states.set_flow_run_state(labeled_flow_run_id, Running())

        mock_concurrency_check.assert_called_once()

    @pytest.mark.parametrize("limits", [{"foo": 0}, {"bar": 0}])
    async def test_full_concurrency_and_unlimited_labels(
        self, labeled_flow_run_id: str, monkeypatch, limits: dict
    ):
        """
        This should test whenever there is an environment with one or more
        limited labels and one or more unlimited labels that the concurrency
        gets limited strictly due to the limited label's capacity.
        """
        mock_concurrency_check = CoroutineMock(return_value=limits)
        monkeypatch.setattr(
            self.mock_location, mock_concurrency_check,
        )
        with pytest.raises(ValueError, match="concurrency limit"):
            await api.states.set_flow_run_state(labeled_flow_run_id, Running())

        mock_concurrency_check.assert_called_once()

    @pytest.mark.parametrize("limits", [{"foo": 1}, {"bar": 1}, {}])
    async def test_ignores_unlimited_labels(
        self, labeled_flow_run_id: str, monkeypatch, limits: dict
    ):
        """
        This should test that if a flow's execution environment has labels
        that aren't explicitely limited, the limit is treated as "unlimited",
        and does not cause a failure when setting to `Running`.
        """
        mock_concurrency_check = CoroutineMock(return_value=limits)
        monkeypatch.setattr(
            self.mock_location, mock_concurrency_check,
        )

        await api.states.set_flow_run_state(labeled_flow_run_id, Running())

        mock_concurrency_check.assert_called_once()

    async def test_unlabeled_environment_not_concurrency_checked(
        self, flow_run_id: str, monkeypatch
    ):
        """
        This code is attempting to be 100% opt in, so we need to make sure
        that if users don't mark their flow environments, they will not
        have their flows throttled. While this is implicitly done in other
        tests, it should be explicitely marked here due to the importance
        of being opt in.
        """
        mock_concurrency_check = CoroutineMock()
        monkeypatch.setattr(
            self.mock_location, mock_concurrency_check,
        )

        await api.states.set_flow_run_state(flow_run_id, Running())

        mock_concurrency_check.assert_not_called()


@pytest.mark.usefixtures("plugin_config")
class TestGetRunsInQueueConcurrency:
    """
    When promoted from contrib, to be placed in `api.test_runs.TestGetRunsInQueue`
    """

    async def test_concurrency_limited_flows_are_not_retrieved(
        self,
        labeled_flow_id: str,
        labeled_flow_id_2: str,
        flow_concurrency_limit: models.FlowConcurrencyLimit,
        flow_concurrency_limit_2: models.FlowConcurrencyLimit,
    ):
        """
        Tests to make sure that even when there are constrained flows
        that don't pass their constraint check, we're able to find
        flows that aren't concurrency constraint and return
        them from the queue.
        """

        agent_labels = ["foo", "bar", "baz"]
        num_unconstrained_flows = 5
        await asyncio.gather(
            *(
                [
                    runs.create_flow_run(flow_id=labeled_flow_id)
                    for _ in range(2 * config.queued_runs_returned_limit)
                ]
                + [
                    runs.create_flow_run(flow_id=labeled_flow_id_2)
                    for _ in range(num_unconstrained_flows)
                ]
            )
        )

        queued_runs = len(await runs.get_runs_in_queue(labels=agent_labels))
        available_constraint_slots = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name, flow_concurrency_limit_2.name]
        )
        expected_number_of_runs = (
            min(available_constraint_slots.values()) + num_unconstrained_flows
        )
        assert queued_runs == expected_number_of_runs

    async def test_finds_concurrency_limited_and_other_flows(
        self,
        labeled_flow_id_2: str,
        labeled_flow_id: str,
        flow_concurrency_limit: models.FlowConcurrencyLimit,
        flow_concurrency_limit_2: models.FlowConcurrencyLimit,
    ):
        """
        This test should be making sure that if both a concurrency limited
        flow and non concurrency limited flows have eligible runs, that
        we will at most find the concurrency limit's available amount of 
        limited flow runs (but not necessarily that amount), and 
        however many others are available (still not violating the total
        number of returned flow runs as specified by the config)
        """
        # create more runs than concurrency allows
        num_unconstrained_flow_runs = 3
        agent_labels = ["foo", "bar", "baz"]

        to_be_created = []
        to_be_created.extend(
            [
                runs.create_flow_run(flow_id=labeled_flow_id_2)
                for _ in range(num_unconstrained_flow_runs)
            ]
        )
        to_be_created.extend(
            [runs.create_flow_run(flow_id=labeled_flow_id) for _ in range(10)]
        )
        await asyncio.gather(*to_be_created)
        queued_runs = await runs.get_runs_in_queue(labels=agent_labels)

        flow_ids = await models.FlowRun.where({"id": {"_in": queued_runs}}).get(
            {"id", "flow_id"}
        )
        limited_flow_runs = [row for row in flow_ids if row.flow_id == labeled_flow_id]
        unconstrained_flow_runs = [
            row for row in flow_ids if row.flow_id == labeled_flow_id_2
        ]

        # There is already a running flow run, so this shouldn't bring back
        # any queued runs.

        available_slots = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name, flow_concurrency_limit_2.name]
        )

        assert len(queued_runs) == num_unconstrained_flow_runs + min(
            available_slots.values()
        )

        await models.FlowConcurrencyLimit.where(id=flow_concurrency_limit.id).update(
            set={"limit": 5}
        )

        queued_runs = await runs.get_runs_in_queue(labels=agent_labels)

        flow_ids = await models.FlowRun.where({"id": {"_in": queued_runs}}).get(
            {"id", "flow_id"}
        )
        limited_flow_runs = [row for row in flow_ids if row.flow_id == labeled_flow_id]
        unconstrained_flow_runs = [
            row for row in flow_ids if row.flow_id == labeled_flow_id_2
        ]

        assert len(queued_runs) == len(limited_flow_runs) + len(unconstrained_flow_runs)
        capacity = await contrib_api.concurrency_limits.get_available_flow_concurrency(
            [flow_concurrency_limit.name, flow_concurrency_limit_2.name]
        )
        assert len(limited_flow_runs) <= min(
            [
                capacity[flow_concurrency_limit.name],
                capacity[flow_concurrency_limit_2.name],
            ]
        )

    async def test_requires_all_concurrency_slots_for_eligibility(
        self,
        labeled_flow_id: str,
        flow_concurrency_limit: models.FlowConcurrencyLimit,
        flow_concurrency_limit_2: models.FlowConcurrencyLimit,
    ):
        """
        This should test that if a flow has more than one resource
        required for getting scheduled, the run only goes through
        if all concurrency slots required are available.
        """
        await models.FlowRun.where().delete()

        await asyncio.gather(
            *[runs.create_flow_run(flow_id=labeled_flow_id) for _ in range(5)]
        )

        assert await models.FlowRun.where().count() == 5

        # Both fixtures have a default limit of 1, so we need to set 1
        # higher for this test to actually test it.
        await models.FlowConcurrencyLimit.where(id=flow_concurrency_limit.id).update(
            {"limit": 4}
        )
        flow_concurrency_limit = await models.FlowConcurrencyLimit.where(
            id=flow_concurrency_limit.id
        ).first({"id", "limit", "name"})

        queued_runs = await runs.get_runs_in_queue(labels=["foo", "bar"])

        assert len(queued_runs) == min(
            [flow_concurrency_limit.limit, flow_concurrency_limit_2.limit]
        )
