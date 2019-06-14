import json
import logging
import os
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

    _api_enabled = True

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
    variables must be set: `AZURE_CLIENT_ID` (the access key for
    your azure account),
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
                 location='westus',
                 group_name='parsl.auto',

                 key_name=None,
                 key_file=None,
                 profile=None,
                 vnet_name="parsl.auto",

                 state_file=None,
                 walltime="01:00:00",
                 linger=False,
                 launcher=SingleNodeLauncher()):
        if not _api_enabled:
            raise OptionalModuleMissing(
                ['azure'], "Azure Provider requires the azure module.")

        self._label = 'azure'
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.nodes_per_block = nodes_per_block
        self.max_nodes = max_blocks * nodes_per_block
        self.parallelism = parallelism

        self.worker_init = worker_init
        self.vm_reference = instance_type_ref
        self.region = location

        self.key_name = key_name
        self.key_file = key_file
        self.location = location
        self.group_name = group_name

        self.walltime = walltime
        self.launcher = launcher
        self.linger = linger
        self.resources = {}
        self.instances = []

        env_specified = os.getenv("AZURE_CLIENT_ID") is not None and os.getenv(
            "AZURE_CLIENT_SECRET") is not None

        if key_file is None and not env_specified:
            raise ConfigurationError(
                "Must specify either, 'key_file', or\
                 `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,\
                  and `AZURE_TENANT_ID` environment variables.")

        if key_file is None:
            self.clientid = os.getenv("AZURE_CLIENT_ID")
            self.clientsecret = os.getenv("AZURE_CLIENT_SECRET")
            self.tenantid = os.getenv("AZURE_TENANT_ID")
            self.subid = os.getenv("AZURE_SUBSCRIPTION_ID")
        else:
            with open(key_file) as fh:
                keys = json.load(fh)
                self.clientid = keys.get("AZURE_CLIENT_ID")
                self.clientsecret = keys.get("AZURE_CLIENT_SECRET")
                self.tenantid = keys.get("AZURE_TENANT_ID")
                self.subid = keys.get("AZURE_SUBSCRIPTION_ID")

        self.get_clients()

    def get_clients(self):
        credentials, subscription_id = self.get_credentials()
        self.resource_client = ResourceManagementClient(
            credentials, subscription_id)
        self.compute_client = ComputeManagementClient(
            credentials, subscription_id)
        self.network_client = NetworkManagementClient(
            credentials, subscription_id)

    def get_credentials(self):
        subscription_id = self.subid
        credentials = ServicePrincipalCredentials(
            client_id=self.clientid,
            secret=self.clientsecret,
            tenant=self.tenantid
        )
        return credentials, subscription_id

    def submit(self, command='sleep 1', blocksize=1,
               tasks_per_node=1, job_name="parsl.auto"):
        # Make sure group exists
        self.resource_client.resource_groups.create_or_update(
            self.group_name, {'location': self.location})
        self.resources.resources["group"] = self.group_name

        logger.info('\nCreating Linux Virtual Machine')
        nic = create_nic(network_client)

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

    def create_nic(network_client):
        """Create a Network Interface for a VM.
        """
        logger.info('\nCreating Vnet')
        async_vnet_creation = self.network_client.virtual_networks.create_or_update(
            self.group_name,
            self.vnet_name,
            {
                'location': self.location,
                'address_space': {
                    'address_prefixes': ['10.0.0.0/16']
                }
            }
        )
        async_vnet_creation.wait()

        # Create Subnet
        logger.info('\nCreate Subnet')
        async_subnet_creation = network_client.subnets.create_or_update(
            self.group_name,
            self.vnet_name,
            "{}.subnet".format(self.group_name),
            {'address_prefix': '10.0.0.0/24'}
        )
        subnet_info = async_subnet_creation.result()

        # Create NIC
        logger.info('\nCreate NIC')
        async_nic_creation = network_client.network_interfaces.create_or_update(
            self.group_name,
            "{}.nic".format(self.group_name),
            {
                'location': self.location,
                'ip_configurations': [{
                    'name': "{}.ip.config".format(self.group_name),
                    'subnet': {
                        'id': subnet_info.id
                    }
                }]
            }
        )

        return async_nic_creation.result()


if __name__ == '__main__':
    vm_reference = {
        'publisher': 'Canonical',
        'offer': 'UbuntuServer',
        'sku': '16.04.0-LTS',
        'version': 'latest'
    }

    provider = AzureProvider(
        key_file="azure_keys.json",
        instance_type_ref=vm_reference)
    provider.submit()
