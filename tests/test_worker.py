import sys
from unittest.mock import AsyncMock

import pytest
from prefect import flow
from prefect.states import Cancelling, Completed, Running, Scheduled

from prefect_uv.worker import UvJobConfiguration, UvWorker


@pytest.fixture
def mock_subprocess_exec(monkeypatch):
    mock_process = AsyncMock()
    mock_process.returncode = 0
    mock_process.communicate.return_value = (
        b"",
        b"",
    )  # Simulate empty stdout and stderr

    mock_create = AsyncMock(return_value=mock_process)
    monkeypatch.setattr("asyncio.create_subprocess_exec", mock_create)
    return mock_create


@pytest.fixture
def test_flow():
    @flow
    def test_flow():
        pass

    return test_flow


async def test_uv_worker_run_flow_run(mock_subprocess_exec, prefect_client, test_flow):
    flow_run = await prefect_client.create_flow_run(
        flow=test_flow,
        state=Scheduled(),
    )

    async with UvWorker(work_pool_name="test-pool") as worker:
        result = await worker.run(
            flow_run,
            configuration=UvJobConfiguration(),
        )

    assert result.status_code == 0
    mock_subprocess_exec.assert_called_once()
    args = mock_subprocess_exec.call_args.args
    assert args[:2] == ("uv", "run")
    assert args[-3:] == (sys.executable, "-m", "prefect.engine")


async def test_uv_worker_run_with_packages(
    mock_subprocess_exec, prefect_client, test_flow
):
    flow_run = await prefect_client.create_flow_run(
        flow=test_flow,
        state=Scheduled(),
    )

    async with UvWorker(work_pool_name="test-pool") as worker:
        result = await worker.run(
            flow_run,
            configuration=UvJobConfiguration(packages=["requests", "pandas"]),
        )

    assert result.status_code == 0
    mock_subprocess_exec.assert_called_once()
    args = mock_subprocess_exec.call_args.args
    assert args[:2] == ("uv", "run")
    assert "--with" in args
    assert "requests" in args
    assert "pandas" in args
    assert args[-3:] == (sys.executable, "-m", "prefect.engine")


@pytest.mark.parametrize(
    "state",
    [
        Scheduled(),
        Running(),
        Completed(),
    ],
)
async def test_uv_worker_does_not_cancel_non_cancelling_runs(
    prefect_client, state, test_flow
):
    flow_run = await prefect_client.create_flow_run(
        flow=test_flow,
        state=state,
    )

    async with UvWorker(work_pool_name="test-pool") as worker:
        worker.cancel_run = AsyncMock()
        await worker.check_for_cancelled_flow_runs()

    worker.cancel_run.assert_not_called()


async def test_uv_worker_cancels_cancelling_run(prefect_client, test_flow):
    flow_run = await prefect_client.create_flow_run(
        flow=test_flow,
        state=Cancelling(),
    )

    async with UvWorker(work_pool_name="test-pool") as worker:
        worker.cancel_run = AsyncMock()
        await worker.check_for_cancelled_flow_runs()

    worker.cancel_run.assert_awaited_once_with(flow_run)


async def test_uv_worker_handles_subprocess_error(
    mock_subprocess_exec, prefect_client, test_flow
):
    flow_run = await prefect_client.create_flow_run(
        flow=test_flow,
        state=Scheduled(),
    )

    mock_subprocess_exec.return_value.returncode = 1  # Simulate an error

    async with UvWorker(work_pool_name="test-pool") as worker:
        result = await worker.run(
            flow_run,
            configuration=UvJobConfiguration(),
        )

    assert result.status_code == 1
