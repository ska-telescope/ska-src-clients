""" Calling the Global Execution API client """
from ska_src_clients.api.api import API
from ska_src_clients.common.exceptions import handle_client_exceptions


class GlobalExecutionAPI(API):
    """A client for the Global Execution API."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_client_exceptions
    def submit_job(self, container_image: str, script: str, parameters: str = "", system_parameters: dict = None):
        """Submit a job to the Global Execution API.
        :param str container_image: The Docker image to use for the job.
        :param script:
        :param parameters: Additional parameters for the script.
        :param system_parameters:
        :return: The response from the Global Execution API.
        """
        job_request = {
            "container_image": container_image,
            "script": script,
            "parameters": parameters,
            "system_parameters": system_parameters or {}
        }
        print(f"Submitting job with request: {job_request}")
        client = self.session.client_factory.get_client_from_service_name("global-execution-api", is_authenticated=True)
        return client.submit_job(job_request=job_request).json()

    @handle_client_exceptions
    def get_job_status(self, job_id):
        """Get the status of a job from the Global Execution API.

        :param str job_id: The ID of the job to check.
        :return: The status of the job.
        """
        client = self.session.client_factory.get_client_from_service_name("global-execution-api", is_authenticated=True)
        return client.get_job_status(job_id=job_id).json()
