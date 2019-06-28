import json
import logging
import os
import time
from string import Template

from parsl.dataflow.error import ConfigurationError
from template import template_string
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
        self.vm_disk_size = vm_reference["disk_size_gb"]
        self.vm_reference = instance_type_ref
        self.region = location
        self.vnet_name = vnet_name

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
            raise ConfigurationError("Must specify either, 'key_file', or\
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
        self.compute_client = ComputeManagementClient(credentials,
                                                      subscription_id)
        self.network_client = NetworkManagementClient(credentials,
                                                      subscription_id)

    def get_credentials(self):
        subscription_id = self.subid
        credentials = ServicePrincipalCredentials(client_id=self.clientid,
                                                  secret=self.clientsecret,
                                                  tenant=self.tenantid)
        return credentials, subscription_id

    def submit(self,
               command='sleep 1',
               blocksize=1,
               tasks_per_node=1,
               job_name="parsl.auto"):
        # Make sure group exists
        self.resource_client.resource_groups.create_or_update(
            self.group_name, {'location': self.location})
        self.resources["group"] = self.group_name

        logger.info('\nCreating NIC')
        nic = self.create_nic(self.network_client)

        logger.info('\nCreating Linux Virtual Machine')
        vm_parameters = self.create_vm_parameters(nic.id, self.vm_reference)

        # Uniqueness strategy from AWS provider
        job_name = "parsl.auto.{0}".format(time.time())

        wrapped_cmd = self.launcher(command, tasks_per_node,
                                    self.nodes_per_block)

        async_vm_creation = self.compute_client.\
            virtual_machines.create_or_update(
                self.vnet_name, job_name, vm_parameters)

        vm_info = async_vm_creation.result()
        self.instances.append(vm_info.id)

        disk, d_name = self.create_disk()

        logger.debug("Started instance_id: {0}".format(vm_info.id))

        # state = translate_table.get(instance.state['Name'], "PENDING")

        self.resources[vm_info.id] = {
            "job_id": vm_info.id,
            "instance": vm_info,
            "status": "Test State"
        }

        virtual_machine = async_vm_creation.result()

        virtual_machine.storage_profile.data_disks.append({
            'lun':
            12,
            'name':
            d_name,
            'create_option':
            DiskCreateOption.attach,
            'managed_disk': {
                'id': disk.id
            }
        })
        async_disk_attach = self.compute_client.virtual_machines.create_or_update(
            self.group_name, virtual_machine.name, virtual_machine)
        async_disk_attach.wait()

        async_vm_start = self.compute_client.virtual_machines.start(
            self.group_name, job_name)
        async_vm_start.wait()

        return virtual_machine.name

    def status(self, job_ids):
        print('\nList VMs in resource group')
        for vm in self.compute_client.virtual_machines.list(self.group_name):
            print("\tVM: {}".format(vm.name))

    def cancel(self, job_ids):
        return_vals = []

        if self.linger:
            logger.debug("Ignoring cancel requests due to linger mode")
            return [False for x in job_ids]

        for job_ids in job_ids:
            try:
                print('\nDelete VM')
                async_vm_delete = self.compute_client.virtual_machines.delete(
                    self.group_name, job_id)
                async_vm_delete.wait()
                return_vals.append(True)
            except Exception as e:
                return_vals.append(False)
                
        return return_vals

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

    def create_nic(self, network_client):
        """Create (or update, if it exists already) a Network Interface for a VM.
        """
        try:
            logger.info('\nCreating (or updating) Vnet')
            async_vnet_creation = self.network_client.virtual_networks.\
                create_or_update(
                    self.group_name, self.vnet_name, {
                        'location': self.location,
                        'address_space': {
                            'address_prefixes': ['10.0.0.0/16']
                        }
                    })
            vnet_info = async_vnet_creation.result()
            self.resources["vnet"] = vnet_info

        except Exception as e:
            logger.info('Found Existing Vnet. Proceeding.')
                

        # Create Subnet
        logger.info('\nCreating (or updating) Subnet')
        async_subnet_creation = self.network_client.subnets.create_or_update(
            self.group_name, self.vnet_name,
            "{}.subnet".format(self.group_name),
            {'address_prefix': '10.0.0.0/24'})
        subnet_info = async_subnet_creation.result()

        if not self.resources.get("subnets", None):
            self.resources["subnets"] = {}

        self.resources["subnets"][subnet_info.id] = subnet_info

        # Create NIC
        logger.info('\nCreating (or updating) NIC')
        async_nic_creation = self.network_client.network_interfaces.\
            create_or_update(
                self.group_name, "{}.{}.nic".format(self.group_name, time.time()), {
                    'location':
                    self.location,
                    'ip_configurations': [{
                        'name':
                        "{}.ip.config".format(self.group_name),
                        'subnet': {
                            'id': subnet_info.id
                        }
                    }]
                })

        nic_info = async_nic_creation.result()

        if not self.resources.get("nics", None):
            self.resources["nics"] = {}

        self.resources["nics"][nic_info.id] = nic_info

        return nic_info

    def create_vm_parameters(self, nic_id, vm_reference):
        """Create the VM parameters structure.
        """
        return {
            'location': self.region,
            'os_profile': {
                'computer_name': "{}.{}".format(self.vnet_name, time.time()),
                'admin_username': self.vm_reference["admin_username"],
                'admin_password': self.vm_reference["password"]
            },
            'hardware_profile': {
                'vm_size': vm_reference["vm_size"]
            },
            'storage_profile': {
                'image_reference': {
                    'publisher': vm_reference['publisher'],
                    'offer': vm_reference['offer'],
                    'sku': vm_reference['sku'],
                    'version': vm_reference['version']
                },
            },
            'network_profile': {
                'network_interfaces': [{
                    'id': nic_id,
                }]
            },
        }

    def create_disk(self):
        logger.info('\nCreate (empty) managed Data Disk')
        name = '{}.{}'.format(self.group_name, time.time())
        async_disk_creation = self.compute_client.disks.create_or_update(
            self.group_name, name, {
                'location': self.location,
                'disk_size_gb': self.vm_reference["disk_size_gb"],
                'creation_data': {
                    'create_option': DiskCreateOption.empty
                }
            })
        data_disk = async_disk_creation.result()
        return data_disk, name


if __name__ == '__main__':
    vm_reference = {
        'publisher': 'Canonical',
        'offer': 'UbuntuServer',
        'sku': '16.04.0-LTS',
        'version': 'latest',
        'vm_size': 'Standard_DS1_v2',
        'disk_size_gb': 10,
        "admin_username": "parsl.auto.admin",
        "password" : "@@86*worth*TRUST*problem*69@@"
    }

    provider = AzureProvider(key_file="azure_keys.json",
                             instance_type_ref=vm_reference)
    id = provider.submit()
    provider.status([id])
    provider.cancel([id])
