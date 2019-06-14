import json
import logging
import os
import time
from string import Template

from parsl.dataflow.error import ConfigurationError
from parsl.providers.aws.template import template_string
from parsl.providers.provider_base import ExecutionProvider
from parsl.providers.error import OptionalModuleMissing
from parsl.utils import RepresentationMixin
from parsl.launchers import SingleNodeLauncher

logger = logging.getLogger(__name__)

try:
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.network import NetworkManagementClient
    from azure.mgmt.compute import ComputeManagementClient
    from azure.mgmt.compute.models import DiskCreateOption

    from msrestazure.azure_exceptions import CloudError

except ImportError:
    _api_enabled = False
else:
    _api_enabled = True

translate_table = {
    'pending': 'PENDING',
    'running': 'RUNNING',
    'terminated': 'COMPLETED',
    'shutting-down': 'COMPLETED',  # (configuring),
    'stopping': 'COMPLETED',  # We shouldn't really see this state
    'stopped': 'COMPLETED',  # We shouldn't really see this state
}


class AzureProvider(ExecutionProvider, RepresentationMixin):
    """
    One of 2 methods are required to authenticate: keyfile, or environment
    variables. If  keyfile is not set, the following environment
    variables must be set: `AZURE_CLIENT_ID` (the access key for your azure account),
    `AZURE_CLIENT_SECRET` (the secret key for your azure account), and the
    `AZURE_TENANT_ID` (the session key for your azure account).
    """


    def __init__(self,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 nodes_per_block=1,
                 parallelism=1,

                 worker_init='',
                 instance_type_ref=None,

                 key_name=None,
                 key_file=None,
                 profile=None,
                 vnet_name="parsl.auto"

                 state_file=None,
                 walltime="01:00:00",
                 linger=False,
                 launcher=SingleNodeLauncher()):
        if not _api_enabled:
            raise OptionalModuleMissing(['azure'], "Azure Provider requires the azure-sdk-for-python module.")

        self._label = 'azure'
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.nodes_per_block = nodes_per_block
        self.max_nodes = max_blocks * nodes_per_block
        self.parallelism = parallelism

        self.worker_init = worker_init
        self.vm_reference = instance_type_ref
        self.region = region

        self.key_name = key_name
        self.key_file = key_file

        self.walltime = walltime
        self.launcher = launcher
        self.linger = linger
        self.resources = {}

        env_specified = os.getenv("AZURE_CLIENT_ID") is not None and os.getenv("AZURE_CLIENT_SECRET") is not None
        
        if key_file is None and not env_specified:
            raise ConfigurationError("Must specify either, 'key_file', or `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and `AZURE_TENANT_ID` environment variables.")

        if key_file is not None:
            self.clientid = os.getenv("AZURE_CLIENT_ID")
            self.clientsecret = os.getenv("AZURE_CLIENT_SECRET")
            self.tenantid = os.getenv("AZURE_TENANT_ID")
        else:
            with open(key_file) as fh:
                keys = json.load(fh)
                self.clientid = keys.get("AZURE_CLIENT_ID")
                self.clientsecret = keys.get("AZURE_CLIENT_SECRET")
                self.tenantid = keys.get("AZURE_TENANT_ID")





    def submit(self, command='sleep 1', blocksize=1, tasks_per_node=1, job_name="parsl.auto"):
        pass

    def status(self, job_ids):
        pass

    def cancel(self, job_ids):
        pass

    @property
    def scaling_enabled(self):
        return True

    @property
    def label(self):
        return self._label

    @property
    def current_capacity(self):
        """Returns the current blocksize."""
        return len(self.instances)



if __name__ == '__main__':
    vm_reference = {
        'publisher': 'Canonical',
        'offer': 'UbuntuServer',
        'sku': '16.04.0-LTS',
        'version': 'latest'
    }

    provider = AzureProvider(key_file="azure_keys.json", instance_type_ref=vm_reference)





