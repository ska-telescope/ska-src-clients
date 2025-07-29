""" Calling the Compute API client """
from ska_src_clients.api.api import API
from ska_src_clients.common.exceptions import handle_client_exceptions


class ComputeAPI(API):
    """A client for the Compute API."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_client_exceptions
    def submit_job(self, container_image: str, script: str, parameters: dict = None):
        """Submit a job to the Compute API.
        :param parameters:
        :param script:
        :param str container_image: The Docker image to use for the job.
        :return: The response from the Compute API.
        """
        job_request = {
            "container_image": container_image,
            "script": script,
            "parameters": parameters or {}
        }
        print(f"Submitting job with request: {job_request}")
        client = self.session.client_factory.get_client_from_service_name("compute-api", is_authenticated=True)
        return client.submit_job(job_request=job_request).json()

    @handle_client_exceptions
    def get_job_status(self, job_id):
        """Get the status of a job from the Compute API.

        :param str job_id: The ID of the job to check.
        :return: The status of the job.
        """
        client = self.session.client_factory.get_client_from_service_name("compute-api", is_authenticated=True)
        return client.get_job_status(job_id=job_id).json()
