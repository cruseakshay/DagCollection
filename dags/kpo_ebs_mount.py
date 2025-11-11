from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

namespace = conf.get('kubernetes', 'NAMESPACE')

affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=1,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="topology.kubernetes.io/zone", operator="In", values=["us-east-1a"])
                    ]
                ),
            )
        ]
    ),
)

# This will detect the default namespace locally and read the
# environment namespace when deployed to cloud.
if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

dag = DAG("kpo_ebs_mount", schedule_interval=None, default_args=default_args)

# This is where we define our desired resources.
compute_resources = k8s.V1ResourceRequirements(
    limits={"cpu": "800m", "memory": "3Gi"},
    requests={"cpu": "800m", "memory": "3Gi"}
)


volume = k8s.V1Volume(
    name="test",
    aws_elastic_block_store=k8s.V1AWSElasticBlockStoreVolumeSource(
        volume_id="vol-02b3f8368f559f489",
        fs_type="ext4"
    )
)


volume_mount = k8s.V1VolumeMount(
    name=volume.name,
    mount_path="/akki/test",
    sub_path="test",
    read_only=False,
    )

with dag:

    t1=KubernetesPodOperator(
        namespace=namespace,
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["df && ls -alth /akki/test/ && echo 'this is working2'>>/akki/test/trynfs && ls -alth /akki/test/ && mount -l"],
        labels={"foo": "bar"},
        name="airflow-test-pod1",
        task_id="task-one",
        in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
        config_file=config_file,
        resources=compute_resources,
        is_delete_operator_pod=True,
        get_logs=True,
        volumes=[volume],
        volume_mounts=[volume_mount],

    )
    t2=KubernetesPodOperator(
        namespace=namespace,
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["df && ls -alth /akki/test/ && cat /akki/test/trynfs && echo 'this is second task'"],
        labels={"foo": "bar"},
        name="airflow-test-pod1",
        task_id="task-two",
        in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
        config_file=config_file,
        resources=compute_resources,
        is_delete_operator_pod=True,
        get_logs=True,
        volumes=[volume],
        volume_mounts=[volume_mount],
    )
    t1>>t2
