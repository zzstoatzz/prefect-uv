from pathlib import Path

from prefect import flow
from prefect.events import DeploymentEventTrigger
from pydantic_core import from_json


@flow(log_prints=True)
def fly_off_the_handle(payload_str: str):
    payload: dict = from_json(payload_str)
    print(payload)


if __name__ == "__main__":
    current_file = Path(__file__)
    flow.from_source(
        source=str(current_file.parent.resolve()),
        entrypoint=f"{current_file.name}:fly_off_the_handle",
    ).deploy(
        name="easy-2-trigger",
        work_pool_name="asdf",
        triggers=[
            DeploymentEventTrigger(  # type: ignore
                expect={"whatever.bro"},
                parameters={"payload_str": " {{ event.payload_str }}"},
            )
        ],
    )
