import boto
import boto.emr
from boto.emr.instance_group import InstanceGroup

conn = boto.emr.connect_to_region('us-east-1')

instance_groups = []
instance_groups.append(InstanceGroup(
    num_instances=1,
    role="MASTER",
    type="m1.small",
    market="ON_DEMAND",
    name="Main node"))
instance_groups.append(InstanceGroup(
    num_instances=2,
    role="CORE",
    type="m1.small",
    market="ON_DEMAND",
    name="Worker nodes"))
instance_groups.append(InstanceGroup(
    num_instances=2,
    role="TASK",
    type="m1.small",
    market="SPOT",
    name="My cheap spot nodes",
    bidprice="0.002"))

cluster_id = conn.run_jobflow(
    "Name for my cluster",
    instance_groups=instance_groups,
    action_on_failure='TERMINATE_JOB_FLOW',
    keep_alive=True,
    enable_debugging=True,
    log_uri="s3://mybucket/logs/",
    hadoop_version=None,
    ami_version="2.4.9",
    steps=[],
    bootstrap_actions=[],
    ec2_keyname="my-ec2-key",
    visible_to_all_users=True,
    job_flow_role="EMR_EC2_DefaultRole",
    service_role="EMR_DefaultRole")
