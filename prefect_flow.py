# <project_root>/register_prefect_flow.py
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

import click
import logging

from prefect import Client, Flow, Task
from prefect.utilities.exceptions import ClientError

from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession, get_current_session
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline.node import Node
from kedro.runner import run_node


class KedroInitTask(Task):
    def __init__(
        self,
        project_path: Union[Path, str] = None,
        package_name: str = None,
        env: str = None,
        extra_params: Dict[str, Any] = None,
        *args,
        **kwargs,
    ):
        self.package_name = package_name
        self.project_path = Path(project_path or Path.cwd()).resolve()
        self.extra_params = extra_params
        self.env = env
        super().__init__(name=package_name, *args, **kwargs)
        pass

    def run(self):
        metadata = bootstrap_project(self.project_path)
        session = KedroSession.create(
            project_path=self.project_path, env=self.env, extra_params=self.extra_params
        )
        session.close()
        return session.session_id


class KedroTask(Task):
    def __init__(
        self,
        node_name: str,
        # session_id: str,
        pipeline_name: str = "__default__",
        project_path: Union[Path, str] = None,
        *args,
        **kwargs,
    ):
        self.project_path = project_path
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        super().__init__(name=node_name, *args, **kwargs)

    def run(self, session_id):
        metadata = bootstrap_project(self.project_path)
        logging.info(f"Running kedro session for id {session_id}")
        with KedroSession(
            session_id=session_id, project_path=self.project_path
        ).__enter__() as session:
            session.run(self.pipeline_name, node_names=[self.node_name])


@click.command()
@click.option("-p", "--pipeline", "pipeline_name", default=None)
@click.option("--env", "-e", type=str, default=None)
def build_and_register_flow(pipeline_name, env):
    """Register a Kedro pipeline as a Prefect flow."""
    project_path = Path.cwd()
    metadata = bootstrap_project(project_path)

    package_name = "sf_prefect"
    pipeline_name = pipeline_name or "__default__"
    pipeline = pipelines.get(pipeline_name)

    init_task = KedroInitTask(project_path, package_name=package_name, env=env)
    tasks = {}

    for node, parent_nodes in pipeline.node_dependencies.items():
        if node._unique_key not in tasks:
            node_task = KedroTask(
                node_name=node.name,
                pipeline_name=pipeline_name,
                project_path=project_path,
            )
            tasks[node._unique_key] = {}
            tasks[node._unique_key]["task"] = node_task
        else:
            node_task = tasks[node._unique_key]["task"]

        parent_tasks = []

        for parent in parent_nodes:
            if parent._unique_key not in tasks:
                parent_task = KedroTask(
                    node_name=parent.name,
                    # session_id=session_id,
                    pipeline_name=pipeline_name,
                    project_path=project_path,
                )
                tasks[parent._unique_key] = {}
                tasks[parent._unique_key]["task"] = parent_task
            else:
                parent_task = tasks[parent._unique_key]["task"]

            parent_tasks.append(parent_task)

        tasks[node._unique_key]["parent_tasks"] = parent_tasks
        node_task = tasks[node._unique_key]["task"]

    with Flow(metadata.project_name) as flow:
        session_id = init_task()
        for task in tasks.values():
            node_task: Task = task["task"]
            parent_tasks = task["parent_tasks"]
            node_task.set_dependencies(
                upstream_tasks=parent_tasks, keyword_tasks={"session_id": session_id}
            )

        client = Client()
        try:
            client.create_project(project_name=metadata.project_name)
        except ClientError:
            # `metadata.project_name` project already exists
            pass

    # Register the flow with the server
    flow.register(project_name=metadata.project_name)

    # Start a local agent that can communicate between the server
    # and your flow code
    flow.run_agent()


if __name__ == "__main__":
    build_and_register_flow()
