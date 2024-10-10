import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional, Self, Type

from anyio.abc import TaskStatus
from prefect.client.schemas.objects import Flow, FlowRun
from prefect.client.schemas.responses import DeploymentResponse
from prefect.types import NonEmptyishName
from prefect.utilities.collections import listrepr
from prefect.utilities.urls import url_for
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field

logger = logging.getLogger("prefect.worker")


class UvJobConfiguration(BaseJobConfiguration):
    pip_packages: list[NonEmptyishName] = Field(
        default_factory=list,
        description="Any packages will be passed as a `--with` argument to `uv run`.",
    )
    stream_output: bool = True
    show_flow_run_url: bool = True

    @property
    def ensured_command(self) -> str:
        if not self.command:
            raise ValueError("No command provided")
        return self.command

    def prepare_for_flow_run(
        self,
        flow_run: FlowRun,
        deployment: Optional[DeploymentResponse] = None,
        flow: Optional[Flow] = None,
    ):
        super().prepare_for_flow_run(flow_run, deployment, flow)
        self.env = {**os.environ, **self.env}

        # Prepare the command here
        cmd = ["uv", "run"]
        if self.pip_packages:
            for package in self.pip_packages:
                cmd.extend(["--with", package])
        cmd.extend(
            self.command.split() if self.command else [self._base_flow_run_command()]
        )

        self.command = " ".join(cmd)

    def _base_flow_run_command(self) -> str:
        return "python -m prefect.engine"


class UvVariables(BaseVariables):
    pip_packages: Optional[list[str]] = None
    stream_output: bool = True
    show_flow_run_url: bool = True


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

        if configuration.show_flow_run_url:
            logger.info(f"See the run in the UI: {url_for(flow_run)}")

        if pip_extra := configuration.pip_packages:
            flow_run_logger.info(f"Running with dependencies: {listrepr(pip_extra)}")

        job = await self._create_and_start_job(configuration)

        if task_status:
            task_status.started(self._infrastructure_pid_from_process(job["process"]))

        result = await self._watch_job(job, configuration)

        return result

    async def _create_and_start_job(
        self, configuration: UvJobConfiguration
    ) -> Dict[str, Any]:
        cmd = configuration.ensured_command.split()

        logger.info(f"Running command: {configuration.command}")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            env={k: v for k, v in configuration.env.items() if v is not None},
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

    @staticmethod
    def _infrastructure_pid_from_process(process: asyncio.subprocess.Process) -> str:
        return f"{os.uname().nodename}:{process.pid}"
