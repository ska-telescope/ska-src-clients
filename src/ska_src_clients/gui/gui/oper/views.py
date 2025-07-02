import os
import requests
import yaml

from django.shortcuts import render
from pathlib import Path

from ska_src_clients.api.status import StatusAPI
from ska_src_clients.common.utility import load_config
from ska_src_clients.session.oidc import OIDCSession


def home(request):
    return render(request, 'oper/home.html', {})

def api_status(request):
    config = load_config()
    session = OIDCSession(config=config)
    session.load_tokens_from_disk()

    apis = sorted(config.get('apis', {}).keys())

    api_status_responses = []
    for api in apis:
        api_ping = StatusAPI(session=session).ping(service=api)
        api_health = StatusAPI(session=session).health(service=api)

        if api_health:
            api_health.pop("dependent_services")

        api_status_responses.append({
            'api_name': api,
            'ping': api_ping,
            'health': api_health
        })

    return render(request, 'oper/api_status.html', {'api_status_responses': api_status_responses})

def test_rse(request):
    import docker
    config = load_config()
    session = OIDCSession(config=config)
    session.load_tokens_from_disk()

    client = docker.from_env()
    a = client.containers.run(
        "registry.gitlab.com/ska-telescope/src/src-dm/ska-src-dm-da-rucio-client:release-35.6.0",
        command="/etc/profile.d/rucio_init.sh",
        tty=True,
        stdin_open=True
    )

    return render(request, 'oper/test_rse.html', {})
