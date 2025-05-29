import os
import requests
import yaml

from django.shortcuts import render
from pathlib import Path

from ska_src_clients.api.status import StatusAPI
from ska_src_clients.session.oidc import OIDCSession

config_paths = [
    'etc/cfg/oper.yml',
    os.path.join(Path.home(), '.local/etc/cfg/user.yml'),
    '/usr/local/etc/oper.yml'
]

def load_config(config_paths):
    for file_path in config_paths:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                try:
                    config = yaml.safe_load(f)
                    return config
                except yaml.YAMLError:
                    continue

def api_status(request):
    config = load_config(config_paths)
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

def home(request):
    return render(request, 'oper/home.html', {})