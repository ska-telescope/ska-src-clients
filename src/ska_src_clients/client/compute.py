"""An example client. This will be imported by dependent APIs."""
import json
import logging

import requests

from ska_src_clients.common.exceptions import handle_client_exceptions


class ComputeClient:
    """This API's client class."""

    logger = logging.getLogger("uvicorn")

    def __init__(self, api_url, session=None):
        self.api_url = api_url
        if session:
            self.session = session
        else:
            self.session = requests.Session()

    @handle_client_exceptions
    def submit_job(self, job_request):
        """Submit a job to the compute service.

        :param job_request: The job request data.
        :type job_request: dict
        :return: A submit job response.
        :rtype: requests.models.Response
        """
        submit_endpoint = "{api_url}/job/submit".format(api_url=self.api_url)
        self.logger.debug("Submitting job request: %s", job_request)
        resp = self.session.post(
            url=submit_endpoint, data=json.dumps(job_request), headers={"Content-Type": "application/json", "Accept": "application/json"}
        )
        resp.raise_for_status()
        return resp

    @handle_client_exceptions
    def get_job_status(self, job_id):
        """Get the status of a job.

        :param job_id: The ID of the job to retrieve.
        :type job_id: str
        :return: A get job response.
        :rtype: requests.models.Response
        """
        job_endpoint = "{api_url}/job/{job_id}/status".format(api_url=self.api_url, job_id=job_id)
        self.logger.debug("Getting job status for job ID: %s", job_id)
        resp = self.session.get(url=job_endpoint, headers={"Content-Type": "application/json", "Accept": "application/json"})
        resp.raise_for_status()
        return resp

    @handle_client_exceptions
    def health(self):
        """Get the service health.

        :return: A requests response.
        :rtype: requests.models.Response
        """
        health_endpoint = "{api_url}/health".format(api_url=self.api_url)
        self.logger.debug("Getting service health for API: %s", health_endpoint)
        resp = self.session.get(health_endpoint)
        resp.raise_for_status()
        return resp

    @handle_client_exceptions
    def ping(self):
        """Ping the service.

        :return: A requests response.
        :rtype: requests.models.Response
        """
        ping_endpoint = "{api_url}/ping".format(api_url=self.api_url)
        self.logger.debug("Ping for API: %s", ping_endpoint)
        resp = self.session.get(ping_endpoint)
        resp.raise_for_status()
        return resp
