import click
import google.auth
from googleapiclient.discovery import build, Resource
from multiprocessing.dummy import Pool
import time
from typing import Dict, List

from laminar.utils.yaml import YAMLUtility


class DataflowUtility:
    def __init__(self, project_id: str, region: str) -> None:
        self.project_id: str = project_id
        self.region: str = region
        self.credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.dataflow_service: Resource = build("dataflow", "v1b3", credentials=self.credentials)

    def __wait_job(self, parameter: dict) -> None:
        job: dict = parameter["job"]
        project_id: str = parameter["project_id"]
        region: str = parameter["region"]
        expected_states: list = parameter["expected_states"]
        unexpected_states: list = parameter["unexpected_states"]
        wait_check_interval_seconds: float = parameter["wait_check_interval_seconds"]
        wait_max_seconds: float = parameter["wait_max_seconds"]

        print(f"Waiting for Dataflow job named {job['name']} ({job['id']}) to enter state {expected_states}")

        stopped: bool = False
        wait_start_time: float = time.time()
        latest_state: str = None
        while not stopped:
            wait_elapsed_time: float = time.time() - wait_start_time
            if wait_elapsed_time > wait_max_seconds:
                raise Exception(
                    "Maximum time to wait for job to stop has been reached:"
                    f"Job: {job['name']} ({job['id']})"
                    f"Elapsed time (seconds): {wait_elapsed_time}"
                    f"Maximum time to wait (seconds): {wait_max_seconds}"
                )
            response: Dict[str, dict] = self.dataflow_service.projects().locations().jobs().get(
                projectId=project_id,
                location=region,
                jobId=job["id"]
            ).execute()
            job_current_state: str = response["currentState"]

            if job_current_state in expected_states:
                stopped = True
                print(f"The Dataflow job named {job['name']} ({job['id']}) has finally entered state {job_current_state}.")
            elif job_current_state in unexpected_states:
                stopped = True
                raise Exception(f"The Dataflow job named {job['name']} ({job['id']}) has unexpectedly entered state {job_current_state}.")
            else:
                if job_current_state != latest_state:
                    print(
                        f"The Dataflow job named {job['name']} ({job['id']}) is at state {job_current_state}. Still waiting..."
                    )
                    latest_state = job_current_state
                time.sleep(wait_check_interval_seconds)

    def wait_jobs(
        self,
        dataflow_jobs: List[dict],
        expected_states: list,
        unexpected_states: list,
        wait_check_interval_seconds: float,
        wait_max_seconds: float,
        wait_max_threads: int,
    ) -> None:
        parameters: List[dict] = [
            {
                "project_id": self.project_id,
                "region": self.region,
                "job": job,
                "expected_states": expected_states,
                "unexpected_states": unexpected_states,
                "wait_check_interval_seconds": wait_check_interval_seconds,
                "wait_max_seconds": wait_max_seconds
            } for job in dataflow_jobs
        ]
        wait_thread_num: int = len(dataflow_jobs) if len(dataflow_jobs) <= wait_max_threads else wait_max_threads
        wait_pool: Pool = Pool(wait_thread_num)
        wait_pool.map(self.__wait_job, parameters)
        wait_pool.close()

    def list_jobs(self, dataflow_job: str) -> List[dict]:
        response: Dict[str, dict] = self.dataflow_service.projects().locations().jobs().list(
            projectId=self.project_id,
            location=self.region,
            filter="ACTIVE"
        ).execute()
        if "jobs" in response:
            jobs: List[dict] = response["jobs"]
            matched_jobs: List[dict] = [job for job in jobs if job["name"] == dataflow_job]
            if len(matched_jobs) == 0:
                print(f"There are no Dataflow jobs named {dataflow_job}.")
            elif len(matched_jobs) > 1:
                print(f"There are {len(matched_jobs)} jobs with name of {dataflow_job}")
            else:
                print(f"There is exactly 1 job named {dataflow_job}")
            return matched_jobs
        else:
            print("There are no Dataflow jobs with filter ACTIVE.")
            return []
    
    def drain_jobs(self, dataflow_jobs: List[dict]) -> None:
        for job in dataflow_jobs:
            response: Dict[str, dict] = self.dataflow_service.projects().locations().jobs().update(
                projectId=self.project_id,
                location=self.region,
                jobId=job["id"],
                body={"requestedState": "JOB_STATE_DRAINED"}
            ).execute()
            print(
                f"The Dataflow job named {job['name']} ({job['id']}) has been requested to be drained."
                f"Response: {response}"
            )
    
@click.group()
def cli(): pass

@cli.command()
@click.option("--deployment_config_path", type=str, required=True)
def stop_dataflow_jobs(deployment_config_path: str) -> None:
    """
    Drain streaming Dataflow jobs.

    Args:
        deployment_config_path: Deployment configuration path.
    """
    deployment_config: dict = YAMLUtility.read_yaml_from_file(file_path=deployment_config_path)

    dataflow: DataflowUtility = DataflowUtility(
        project_id=deployment_config["dataflow_project_id"], 
        region=deployment_config["dataflow_region"]
    )

    matched_jobs: List[dict] = dataflow.list_jobs(dataflow_job=deployment_config["dataflow_job_name"])

    if len(matched_jobs) >= 1:
        dataflow.drain_jobs(dataflow_jobs=matched_jobs)

        dataflow.wait_jobs(
            dataflow_jobs=matched_jobs,
            expected_states=["JOB_STATE_DRAINED", "JOB_STATE_STOPPED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"],
            unexpected_states=[],
            wait_check_interval_seconds=deployment_config["dataflow_drain_wait_check_interval_seconds"],
            wait_max_seconds=deployment_config["dataflow_drain_wait_max_seconds"],
            wait_max_threads=deployment_config["dataflow_drain_wait_max_threads"]
        )

        print(f"Done waiting for all {len(matched_jobs)} jobs.")
    else:
        print(f"There are no matched jobs named {deployment_config['dataflow_job_name']}.")

@cli.command()
@click.option("--deployment_config_path", type=str, required=True)
def wait_dataflow_jobs(deployment_config_path: str) -> None:
    """
    Wait streaming Dataflow jobs to get deployed.

    Args:
        deployment_config_path: Deployment configuration path.
    """
    deployment_config: dict = YAMLUtility.read_yaml_from_file(file_path=deployment_config_path)

    dataflow: DataflowUtility = DataflowUtility(
        project_id=deployment_config["dataflow_project_id"], 
        region=deployment_config["dataflow_region"]
    )

    matched_jobs: List[dict] = dataflow.list_jobs(dataflow_job=deployment_config["dataflow_job_name"])

    if len(matched_jobs) >= 1:
        dataflow.wait_jobs(
            dataflow_jobs=matched_jobs,
            expected_states=["JOB_STATE_RUNNING"],
            unexpected_states=["JOB_STATE_FAILED"],
            wait_check_interval_seconds=deployment_config["dataflow_running_wait_check_interval_seconds"],
            wait_max_seconds=deployment_config["dataflow_running_wait_max_seconds"],
            wait_max_threads=deployment_config["dataflow_running_wait_max_threads"]
        )
        print(f"Done waiting for all {len(matched_jobs)} jobs.")
    else:
        print(f"There are no matched jobs named {deployment_config['dataflow_job_name']}.")

if __name__ == "__main__": cli()
