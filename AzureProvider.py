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
    import boto3
    from botocore.exceptions import ClientError

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


	def __init__(self,
                 image_id,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 nodes_per_block=1,
                 parallelism=1,

                 worker_init='',
                 instance_type='t2.small',
                 region='us-east-2',
                 spot_max_bid=0,

                 key_name=None,
                 key_file=None,
                 profile=None,
                 iam_instance_profile_arn='',

                 state_file=None,
                 walltime="01:00:00",
                 linger=False,
                 launcher=SingleNodeLauncher()):
        if not _boto_enabled:
            raise OptionalModuleMissing(['boto3'], "AWS Provider requires the boto3 module.")


    def submit(self, command='sleep 1', blocksize=1, tasks_per_node=1, job_name="parsl.auto"):
    	pass

    def status(self, job_ids):
    	pass

    def cancel(self, job_ids):

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





