import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional, Self, Type

from anyio.abc import TaskStatus
from prefect.client.schemas.objects import Flow, FlowRun
from prefect.client.schemas.responses import DeploymentResponse
from prefect.utilities.collections import listrepr
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)

logger = logging.getLogger("prefect.worker")


class UvJobConfiguration(BaseJobConfiguration):
    pip_packages: Optional[list[str]] = None
    stream_output: bool = True

    def prepare_for_flow_run(
        self,
        flow_run: FlowRun,
        deployment: Optional[DeploymentResponse] = None,
        flow: Optional[Flow] = None,
    ):
        super().prepare_for_flow_run(flow_run, deployment, flow)
        self.env = {**os.environ, **self.env}
        if not self.command:
            self.command = self._base_flow_run_command()

    def _base_flow_run_command(self) -> str:
        return "python -m prefect.engine"


class UvVariables(BaseVariables):
    pip_packages: Optional[list[str]] = None
    stream_output: bool = True


class UvWorkerResult(BaseWorkerResult):
    pass


class UvWorker(BaseWorker):
    type: str = "uv"
    job_configuration: Type[BaseJobConfiguration] = UvJobConfiguration
    job_configuration_variables: Type[BaseVariables] = UvVariables

    _documentation_url: str = "https://github.com/zzstoatzz/prefect-uv"
    _logo_url: str = "https://avatars.githubusercontent.com/u/115962839?s=200&v=4"

    async def run(
        self: Self,
        flow_run: FlowRun,
        configuration: UvJobConfiguration,
        task_status: Optional[TaskStatus] = None,
    ):
        flow_run_logger = self.get_flow_run_logger(flow_run)
        flow_run_logger.info("Opening process...")

        if configuration.pip_packages:
            flow_run_logger.info(
                f"Running flow with additional packages: {listrepr(configuration.pip_packages)}"
            )

        job = await self._create_and_start_job(configuration)

        if task_status:
            task_status.started(_infrastructure_pid_from_process(job["process"]))

        result = await self._watch_job(job, configuration)

        return result

    async def _create_and_start_job(
        self, configuration: UvJobConfiguration
    ) -> Dict[str, Any]:
        cmd = ["uv", "run"]

        if configuration.pip_packages:
            for package in configuration.pip_packages:
                cmd.extend(["--with", package])

        cmd.extend(configuration.command.split())

        logger.debug(f"Running command: {' '.join(cmd)}")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            env=configuration.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        return {"process": process, "cmd": cmd}

    async def _watch_job(self, job: Dict[str, Any], configuration: UvJobConfiguration):
        process = job["process"]
        stdout, stderr = await process.communicate()

        if configuration.stream_output:
            sys.stdout.write(stdout.decode())
            sys.stderr.write(stderr.decode())

        display_name = f" {process.pid}"

        if process.returncode:
            logger.error(
                f"Process{display_name} exited with status code: {process.returncode}"
            )
        else:
            logger.info(f"Process{display_name} exited cleanly.")

        return UvWorkerResult(
            status_code=process.returncode,
            identifier=str(process.pid),
        )

    async def _run_uv_command(self, args: list[str]):
        process = await asyncio.create_subprocess_exec(
            "uv",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"UV command failed: {stderr.decode()}")


def _infrastructure_pid_from_process(process: asyncio.subprocess.Process) -> str:
    return f"{os.uname().nodename}:{process.pid}"
