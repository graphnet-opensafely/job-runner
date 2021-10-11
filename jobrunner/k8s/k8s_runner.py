from __future__ import print_function, unicode_literals, division, absolute_import

import datetime
import hashlib
import json
import logging
import re
import socket
import time
import dataclasses

from pathlib import Path
from typing import Tuple

from kubernetes import client, config as k8s_config

from jobrunner import config
from jobrunner.job_executor import *
from jobrunner.k8s.post import JOB_RESULTS_TAG

batch_v1 = client.BatchV1Api()
core_v1 = client.CoreV1Api()
networking_v1 = client.NetworkingV1Api()

log = logging.getLogger(__name__)

JOB_CONTAINER_NAME = "job"
WORK_DIR = "/workdir"
JOB_DIR = "/workspace"


class K8SJobAPI(JobAPI):
    def __init__(self):
        init_k8s_config()
    
    def prepare(self, job: JobDefinition) -> JobStatus:
        try:
            # 1. Validate the JobDefinition. If there are errors, return an ERROR state with message.
            key_entries = [job.workspace, job.id, job.action]
            for e in key_entries:
                if e is None or len(e.strip()) == 0:
                    raise Exception(f"empty values found in key_entries [job.workspace, job.id, job.action]={[job.workspace, job.id, job.action]}")
            
            prepare_job_name = self._get_prepare_job_name(job)
            
            # 2. Check the job is currently in UNKNOWN state. If not return its current state with a message indicated invalid state.
            status = self.get_status(job)
            if status.state != ExecutorState.UNKNOWN:
                return JobStatus(status.state, "invalid state")

            work_pv = self._get_work_pv_name(job)
            job_pv = self._get_job_pv_name(job)
            
            # 3. Check the resources are available to prepare the job. If not, return the UNKNOWN state with an appropriate message.
            storage_class = config.K8S_STORAGE_CLASS
            ws_pv_size = config.K8S_WS_STORAGE_SIZE
            job_pv_size = config.K8S_JOB_STORAGE_SIZE
            create_pv(work_pv, storage_class, ws_pv_size)
            create_pv(job_pv, storage_class, job_pv_size)
            
            namespace = config.K8S_NAMESPACE
            create_namespace(namespace)
            
            work_pvc = self._get_work_pvc_name(job)
            job_pvc = self._get_job_pvc_name(job)
            
            # 4. Create an ephemeral workspace to use for executing this job. This is expected to be a volume mounted into the container,
            # but other implementations are allowed.
            create_pvc(work_pv, work_pvc, storage_class, namespace, ws_pv_size)
            create_pvc(job_pv, job_pvc, storage_class, namespace, job_pv_size)
            
            commit_sha = job.study.commit
            inputs = ";".join(job.inputs)
            repo_url = job.study.git_repo_url
            
            # 5. Launch a prepare task asynchronously. If launched successfully, return the PREPARING state. If not, return an ERROR state with message.
            private_repo_access_token = config.PRIVATE_REPO_ACCESS_TOKEN
            prepare(prepare_job_name, commit_sha, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc)
            
            return JobStatus(ExecutorState.PREPARING)
        except Exception as e:
            if config.DEBUG == 1:
                raise e
            else:
                log.exception(str(e))
                return JobStatus(ExecutorState.ERROR, str(e))

    def execute(self, job: JobDefinition) -> JobStatus:
        try:
            # 1. Check the job is in the PREPARED state. If not, return its current state with a message.
            status = self.get_status(job)
            if status.state != ExecutorState.PREPARED:
                return JobStatus(status.state, "invalid state")
            
            # 2. Validate that the ephememeral workspace created by prepare for this job exists.  If not, return an ERROR state with message.
            job_pvc = self._get_job_pvc_name(job)
            if not is_pvc_created(job_pvc):
                return JobStatus(ExecutorState.ERROR, f"PVC not found {job_pvc}")
            
            # 3. Check there are resources availabe to execute the job. If not, return PREPARED status with an appropriate message.
            execute_job_name = self._get_execute_job_name(job)
            execute_job_arg = [job.action] + job.args
            execute_job_command = None
            execute_job_env = job.env  # TODO do we need to add DB URL for cohortextractor?
            execute_job_image = job.image
            
            namespace = config.K8S_NAMESPACE
            whitelist = config.K8S_EXECUTION_HOST_WHITELIST
            if job.allow_database_access and len(whitelist.strip()) > 0:
                network_labels = create_network_policy(namespace, [ip_port.split(":") for ip_port in whitelist.split(",")])  # allow whitelist
            else:
                network_labels = create_network_policy(namespace, [])  # deny all
            
            # 4. Launch the job execution task asynchronously. If launched successfully, return the EXECUTING state. If not, return an ERROR state with message.
            execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels)
            
            return JobStatus(ExecutorState.EXECUTING)
        except Exception as e:
            if config.DEBUG == 1:
                raise e
            else:
                log.exception(str(e))
                return JobStatus(ExecutorState.ERROR, str(e))

    def finalize(self, job: JobDefinition) -> JobStatus:
        try:
            # 1. Check the job is in the EXECUTED state. If not, return its current state with a message.
            status = self.get_status(job)
            if status.state != ExecutorState.EXECUTED:
                return JobStatus(status.state, "invalid state")
            
            # 2. Validate that the job's ephemeral workspace exists. If not, return an ERROR state with message.
            job_pvc = self._get_job_pvc_name(job)
            if not is_pvc_created(job_pvc):
                return JobStatus(ExecutorState.ERROR, f"PVC not found {job_pvc}")
            
            # 3. Launch the finalize task asynchronously. If launched successfully, return the FINALIZING state. If not, return an ERROR state with message.
            action = job.action
            execute_job_name = self._get_execute_job_name(job)
            job_pvc = self._get_job_pvc_name(job)
            work_pvc = self._get_work_pvc_name(job)
            opensafely_job_id = job.id
            opensafely_job_name = self._get_opensafely_job_name(job)
            output_spec = job.output_spec
            workspace_name = job.workspace
            
            finalize_job_name = convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
            finalize(finalize_job_name, action, execute_job_name, job_pvc, output_spec, work_pvc, workspace_name, job)
            return JobStatus(ExecutorState.FINALIZING)
        except Exception as e:
            if config.DEBUG == 1:
                raise e
            else:
                log.exception(str(e))
                return JobStatus(ExecutorState.ERROR, str(e))
    
    def get_status(self, job: JobDefinition) -> JobStatus:
        namespace = config.K8S_NAMESPACE
        
        prepare_job_name = self._get_prepare_job_name(job)
        prepare_state = read_k8s_job_status(prepare_job_name, namespace)
        if prepare_state == K8SJobStatus.UNKNOWN:
            return JobStatus(ExecutorState.UNKNOWN)
        elif prepare_state == K8SJobStatus.PENDING or prepare_state == K8SJobStatus.RUNNING:
            return JobStatus(ExecutorState.PREPARING)
        elif prepare_state == K8SJobStatus.FAILED:
            try:
                logs = read_log(prepare_job_name, namespace)
                return JobStatus(ExecutorState.ERROR, json.dumps(logs))
            except Exception as e:
                return JobStatus(ExecutorState.ERROR, str(e))
        elif prepare_state == K8SJobStatus.SUCCEEDED:
            
            execute_job_name = self._get_execute_job_name(job)
            execute_state = read_k8s_job_status(execute_job_name, namespace)
            if execute_state == K8SJobStatus.UNKNOWN:
                return JobStatus(ExecutorState.PREPARED)
            elif execute_state == K8SJobStatus.PENDING or execute_state == K8SJobStatus.RUNNING:
                return JobStatus(ExecutorState.EXECUTING)
            elif execute_state == K8SJobStatus.FAILED:
                try:
                    logs = read_log(execute_job_name, namespace)
                    return JobStatus(ExecutorState.ERROR, json.dumps(logs))
                except Exception as e:
                    return JobStatus(ExecutorState.ERROR, str(e))
            elif execute_state == K8SJobStatus.SUCCEEDED:
                
                finalize_job_name = self._get_finalize_job_name(job)
                finalize_state = read_k8s_job_status(finalize_job_name, namespace)
                if finalize_state == K8SJobStatus.UNKNOWN:
                    return JobStatus(ExecutorState.EXECUTED)
                elif finalize_state == K8SJobStatus.PENDING or finalize_state == K8SJobStatus.RUNNING:
                    return JobStatus(ExecutorState.FINALIZING)
                elif finalize_state == K8SJobStatus.FAILED:
                    try:
                        logs = read_log(finalize_job_name, namespace)
                        return JobStatus(ExecutorState.ERROR, json.dumps(logs))
                    except Exception as e:
                        return JobStatus(ExecutorState.ERROR, str(e))
                elif finalize_state == K8SJobStatus.SUCCEEDED:
                    return JobStatus(ExecutorState.FINALIZED)
        
        # should not happen
        return JobStatus(ExecutorState.ERROR, "Unknown status found in get_status()")
    
    def terminate(self, job: JobDefinition) -> JobStatus:
        # 1. If any task for this job is running, terminate it, do not wait for it to complete.
        jobs_deleted = self._delete_all_jobs(job)
        
        # 2. Return ERROR state with a message.
        return JobStatus(ExecutorState.ERROR, f"deleted {','.join(jobs_deleted)}")
    
    def cleanup(self, job: JobDefinition) -> JobStatus:
        # 1. Initiate the cleanup, do not wait for it to complete.
        namespace = config.K8S_NAMESPACE
        
        self._delete_all_jobs(job)
        
        job_pvc = self._get_job_pvc_name(job)
        job_pv = self._get_job_pv_name(job)
        
        try:
            core_v1.delete_namespaced_persistent_volume_claim(job_pvc, namespace)
        except:  # already deleted
            pass
        
        try:
            core_v1.delete_persistent_volume(job_pv)
        except:  # already deleted
            pass
        
        # 2. Return the UNKNOWN status.
        return JobStatus(ExecutorState.UNKNOWN)
    
    def get_results(self, job: JobDefinition) -> JobResults:
        namespace = config.K8S_NAMESPACE
        
        job_output = read_finalize_output(self._get_opensafely_job_name(job), job.id, namespace)
        finalize_status = read_k8s_job_status(self._get_finalize_job_name(job), namespace)
        
        # extract image id
        job_name = self._get_execute_job_name(job)
        container_name = JOB_CONTAINER_NAME
        
        exit_code = read_container_exit_code(job_name, container_name, namespace)
        
        image_id = read_image_id(job_name, container_name, namespace)
        if image_id:
            result = re.search(r'@(.+:.+)', image_id)
            if result:
                image_id = result.group(1)
        
        return JobResults(
                job_output['outputs'],
                job_output['unmatched'],
                exit_code,
                image_id
        )
    
    def _delete_all_jobs(self, job):
        namespace = config.K8S_NAMESPACE
        
        jobs_deleted = []
        prepare_job_name = self._get_prepare_job_name(job)
        try:
            batch_v1.delete_namespaced_job(prepare_job_name, namespace)
            jobs_deleted.append(prepare_job_name)
        except:  # already deleted
            pass
        execute_job_name = self._get_execute_job_name(job)
        try:
            batch_v1.delete_namespaced_job(execute_job_name, namespace)
            jobs_deleted.append(execute_job_name)
        except:  # already deleted
            pass
        finalize_job_name = self._get_finalize_job_name(job)
        try:
            batch_v1.delete_namespaced_job(finalize_job_name, namespace)
            jobs_deleted.append(finalize_job_name)
        except:  # already deleted
            pass
        return jobs_deleted
    
    @staticmethod
    def _get_work_pv_name(job):
        return convert_k8s_name(job.workspace, "pv")
    
    @staticmethod
    def _get_job_pv_name(job):
        return convert_k8s_name(job.id, "pv")
    
    @staticmethod
    def _get_job_pvc_name(job):
        return convert_k8s_name(job.id, "pvc")
    
    @staticmethod
    def _get_work_pvc_name(job):
        return convert_k8s_name(job.workspace, "pvc")
    
    @staticmethod
    def _get_opensafely_job_name(job):
        return f"{job.workspace}_{job.action}"
    
    def _get_execute_job_name(self, job):
        return convert_k8s_name(self._get_opensafely_job_name(job), "execute", additional_hash=job.id)
    
    def _get_prepare_job_name(self, job):
        return convert_k8s_name(self._get_opensafely_job_name(job), "prepare", additional_hash=job.id)
    
    def _get_finalize_job_name(self, job):
        return convert_k8s_name(self._get_opensafely_job_name(job), "finalize", additional_hash=job.id)


class K8SWorkspaceAPI(WorkspaceAPI):
    def __init__(self):
        init_k8s_config()
    
    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]):
        try:
            namespace = config.K8S_NAMESPACE
            work_pvc = convert_k8s_name(workspace, "pvc")
            
            job_name = delete_work_files(workspace, privacy, paths, work_pvc, namespace)
            status = await_job_status(job_name, namespace)
            if status != K8SJobStatus.SUCCEEDED:
                raise Exception(f"unable to delete_files {workspace} {privacy} {paths} {job_name}")
        except Exception as e:
            log.exception(e)


def init_k8s_config():
    global batch_v1, core_v1, networking_v1
    if config.K8S_USE_LOCAL_CONFIG:
        # for testing, e.g. run on a local minikube
        k8s_config.load_kube_config()
    else:
        # Use the ServiceAccount in the cluster
        k8s_config.load_incluster_config()
    
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    networking_v1 = client.NetworkingV1Api()


# TODO Deprecated - replaced by JobAPI
def create_opensafely_job(workspace_name, opensafely_job_id, opensafely_job_name, repo_url, private_repo_access_token, commit_sha, inputs, allow_network_access,
                          execute_job_image, execute_job_command, execute_job_arg, execute_job_env, output_spec):
    """
    1. create pv and pvc (ws_pvc) for the workspace if not exist
    2. check if the job exists, skip the job if already created
    3. create pv and pvc (job_pvc) for the job
    4. create a k8s job with ws_pvc and job_pvc mounted, this job consists of multiple steps running in multiple containers:
       1. pre container: git checkout study repo to job volume
       2. job container: run the opensafely job command (e.g. cohortextractor) on job_volume
       3. post container: use python re to move matching output files from job volume to ws volume
    """
    work_pv = convert_k8s_name(workspace_name, "pv")
    work_pvc = convert_k8s_name(workspace_name, "pvc")
    job_pv = convert_k8s_name(opensafely_job_id, "pv")
    job_pvc = convert_k8s_name(opensafely_job_id, "pvc")
    
    storage_class = config.K8S_STORAGE_CLASS
    ws_pv_size = config.K8S_WS_STORAGE_SIZE
    job_pv_size = config.K8S_JOB_STORAGE_SIZE
    create_pv(work_pv, storage_class, ws_pv_size)
    create_pv(job_pv, storage_class, job_pv_size)
    
    namespace = config.K8S_NAMESPACE
    create_namespace(namespace)
    
    create_pvc(work_pv, work_pvc, storage_class, namespace, ws_pv_size)
    create_pvc(job_pv, job_pvc, storage_class, namespace, job_pv_size)
    
    # Prepare
    prepare_job_name = convert_k8s_name(opensafely_job_name, "prepare", additional_hash=opensafely_job_id)
    prepare_job_name = prepare(prepare_job_name, commit_sha, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc)
    
    # Execute
    whitelist = config.K8S_EXECUTION_HOST_WHITELIST
    whitelist_network_labels = create_network_policy(namespace, [ip_port.split(":") for ip_port in whitelist.split(",")] if len(whitelist.strip()) > 0 else [])
    deny_all_network_labels = create_network_policy(namespace, [])
    execute_job_name = convert_k8s_name(opensafely_job_name, "execute", additional_hash=opensafely_job_id)
    network_labels = whitelist_network_labels if allow_network_access else deny_all_network_labels
    execute_job_name = execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels, prepare_job_name)
    
    # Finalize
    # wait for execute job finished before
    while True:
        status = read_k8s_job_status(execute_job_name, namespace)
        if status.completed():
            break
        time.sleep(.5)
    
    finalize_job_name = convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
    finalize_job_name = finalize(finalize_job_name, execute_job_arg[0], execute_job_name, job_pvc, output_spec, work_pvc, workspace_name)
    
    return [prepare_job_name, execute_job_name, finalize_job_name], work_pv, work_pvc, job_pv, job_pvc


def prepare(prepare_job_name, commit_sha, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc):
    repos_dir = WORK_DIR + "/repos"
    command = ['python', '-m', 'jobrunner.k8s.pre']
    args = [repo_url, commit_sha, repos_dir, JOB_DIR, inputs]
    env = {'PRIVATE_REPO_ACCESS_TOKEN': private_repo_access_token}
    storages = [
        (work_pvc, WORK_DIR, False),
        (job_pvc, JOB_DIR, True),
    ]
    image_pull_policy = "Never" if config.K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    namespace = config.K8S_NAMESPACE
    jobrunner_image = config.K8S_JOB_RUNNER_IMAGE
    create_k8s_job(prepare_job_name, namespace, jobrunner_image, command, args, env, storages, {}, image_pull_policy=image_pull_policy)
    return prepare_job_name


def execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels, depends_on=None):
    command = execute_job_command
    args = execute_job_arg
    storages = [
        (job_pvc, JOB_DIR, True),
    ]
    env = execute_job_env
    image_pull_policy = "Never" if config.K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    namespace = config.K8S_NAMESPACE
    create_k8s_job(execute_job_name, namespace, execute_job_image, command, args, env, storages, network_labels, depends_on=depends_on,
                   image_pull_policy=image_pull_policy)
    return execute_job_name


def finalize(finalize_job_name, action, execute_job_name, job_pvc, output_spec, work_pvc, workspace_name, job_definition: JobDefinition = None):
    # read the log of the execute job
    pod_name = None
    container_log = None
    namespace = config.K8S_NAMESPACE
    logs = read_log(execute_job_name, namespace)
    for (pod_name, container_name), container_log in logs.items():
        if container_name == JOB_CONTAINER_NAME:
            break
    
    # get the metadata of the execute job
    pods = list_pod_of_job(execute_job_name, namespace)
    print(pods)
    
    job = batch_v1.read_namespaced_job(execute_job_name, namespace)
    job_metadata = extract_k8s_api_values(job, ['env'])  # env contains sql server login
    
    job_status = read_k8s_job_status(execute_job_name, namespace)
    
    high_privacy_storage_base = Path(WORK_DIR) / "high_privacy"
    high_privacy_workspace_dir = high_privacy_storage_base / 'workspaces' / workspace_name
    high_privacy_metadata_dir = high_privacy_workspace_dir / "metadata"
    high_privacy_log_dir = high_privacy_storage_base / 'logs' / datetime.date.today().strftime("%Y-%m") / pod_name
    high_privacy_action_log_path = high_privacy_metadata_dir / f"{action}.log"
    
    medium_privacy_storage_base = Path(WORK_DIR) / "medium_privacy"
    medium_privacy_workspace_dir = medium_privacy_storage_base / 'workspaces' / workspace_name
    medium_privacy_metadata_dir = medium_privacy_workspace_dir / "metadata"
    
    execute_logs = container_log
    output_spec_json = json.dumps(output_spec)
    job_metadata = {
        "state"       : job_status.name,
        "created_at"  : "",
        "started_at"  : str(job.status.start_time),
        "completed_at": str(job.status.completion_time),
        "job_metadata": job_metadata
    }
    if job_definition:
        # add fields from JobDefinition
        job_metadata.update(dataclasses.asdict(job_definition))
    
    job_metadata_json = json.dumps(job_metadata)
    
    command = ['python', '-m', 'jobrunner.k8s.post']
    args = [JOB_DIR, high_privacy_workspace_dir, high_privacy_metadata_dir, high_privacy_log_dir, high_privacy_action_log_path, medium_privacy_workspace_dir,
            medium_privacy_metadata_dir, execute_logs, output_spec_json, job_metadata_json]
    args = [str(a) for a in args]
    env = {}
    storages = [
        (work_pvc, WORK_DIR, False),
        (job_pvc, JOB_DIR, True),
    ]
    image_pull_policy = "Never" if config.K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    jobrunner_image = config.K8S_JOB_RUNNER_IMAGE
    create_k8s_job(finalize_job_name, namespace, jobrunner_image, command, args, env, storages, {}, image_pull_policy=image_pull_policy)
    
    return finalize_job_name


def delete_work_files(workspace, privacy, paths, work_pvc, namespace):
    job_name = convert_k8s_name(workspace, f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}-delete-job", additional_hash=";".join(paths))
    if privacy == Privacy.HIGH:
        workspace_dir = Path(WORK_DIR) / "high_privacy" / 'workspaces' / workspace
    else:
        workspace_dir = Path(WORK_DIR) / "medium_privacy" / 'workspaces' / workspace
    image = "busybox"
    command = ['/bin/sh', '-c']
    args = [';'.join([f'rm -f {workspace_dir / p} || true' for p in paths])]
    storage = [
        # pvc, path, is_control
        (work_pvc, WORK_DIR, False)
    ]
    image_pull_policy = "Never" if config.K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    create_k8s_job(job_name, namespace, image, command, args, {}, storage, dict(), image_pull_policy=image_pull_policy)
    return job_name


class K8SJobStatus(Enum):
    SUCCEEDED = 0  # 0 for success
    
    UNKNOWN = 1
    PENDING = 2
    RUNNING = 3
    FAILED = 4
    
    def completed(self):
        return self == K8SJobStatus.SUCCEEDED or self == K8SJobStatus.FAILED


def convert_k8s_name(text: str, suffix: Optional[str] = None, hash_len: int = 7, additional_hash: str = None) -> str:
    """
    convert the text to the name follow the standard:
    https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
    """
    NAME_MAX_LEN = 63  # max len of name based on the spec
    
    # remove all invalid chars
    def remove_invalid_char(t):
        if t is None:
            return None
        
        t = t.lower()
        t = re.sub(r'[^a-z0-9-]+', "-", t)
        t = re.sub(r'-+', "-", t)
        t = re.sub(r'^[^a-z]+', "", t)
        t = re.sub(r'[^a-z0-9]+$', "", t)
        return t
    
    clean_text = remove_invalid_char(text)
    suffix = remove_invalid_char(suffix)
    
    # limit the length
    max_len = NAME_MAX_LEN
    if suffix is not None:
        max_len -= len(suffix) + 1
    
    data = text
    if additional_hash is not None:
        data += additional_hash
    sha1 = hashlib.sha1(data.encode()).hexdigest()[:hash_len]
    clean_text = f"{clean_text[:max_len - hash_len - 1]}-{sha1}"
    
    if suffix is not None:
        clean_text += f"-{suffix}"
    
    return clean_text


def create_namespace(name: str):
    if name == 'default':
        print(f"default namespace is used")
        return
    
    namespaces = core_v1.list_namespace()
    if name in [n.metadata.name for n in namespaces.items]:
        print(f"namespace {name} already exist")
        return
    
    core_v1.create_namespace(client.V1Namespace(
            metadata=client.V1ObjectMeta(
                    name=name
            )
    ))
    print(f"namespace {name} created")


def create_pv(pv_name: str, storage_class: str, size: str):
    all_pv = core_v1.list_persistent_volume()
    for pv in all_pv.items:
        if pv.metadata.name == pv_name:
            print(f"pv {pv_name} already exist")
            return
    
    pv = client.V1PersistentVolume(
            metadata=client.V1ObjectMeta(
                    name=pv_name,
                    labels={
                        "app": "opensafely"
                    }
            ),
            spec=client.V1PersistentVolumeSpec(
                    storage_class_name=storage_class,
                    capacity={
                        "storage": size
                    },
                    access_modes=["ReadWriteOnce"],
                    
                    # for testing:
                    host_path={"path": f"/tmp/{str(int(time.time() * 10 ** 6))}"} if config.K8S_USE_LOCAL_CONFIG else None
            )
    )
    core_v1.create_persistent_volume(body=pv)
    print(f"pv {pv_name} created")


def is_pvc_created(pvc_name: str) -> bool:
    all_pvc = core_v1.list_persistent_volume_claim_for_all_namespaces()
    for pvc in all_pvc.items:
        if pvc.metadata.name == pvc_name:
            return True
    return False


def create_pvc(pv_name: str, pvc_name: str, storage_class: str, namespace: str, size: str):
    if is_pvc_created(pvc_name):
        print(f"pvc {pvc_name} already exist")
        return
    
    pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                    name=pvc_name,
                    labels={
                        "app": "opensafely"
                    }
            ),
            spec=client.V1PersistentVolumeClaimSpec(
                    storage_class_name=storage_class,
                    volume_name=pv_name,
                    access_modes=["ReadWriteOnce"],
                    resources={
                        "requests": {
                            "storage": size
                        }
                    }
            )
    )
    core_v1.create_namespaced_persistent_volume_claim(body=pvc, namespace=namespace)
    print(f"pvc {pvc_name} created")


def create_k8s_job(
        job_name: str,
        namespace: str,
        image: str,
        command: List[str],
        args: List[str],
        env: Mapping[str, str],
        storages: List[Tuple[str, str, bool]],
        pod_labels: Mapping[str, str], depends_on: str = None,
        image_pull_policy: str = "IfNotPresent",
        block_until_created=True
):
    """
    Create k8s job dynamically. Do nothing if job with the same job_name already exist.

    @param job_name: unique identifier of the job
    @param namespace: k8s namespace
    @param image: docker image tag
    @param command: cmd for the job container
    @param args: args for the job container
    @param env: env for the job container
    @param storages: List of (pvc_name, volume_mount_path, is_control). The first storage with is_control equals True will be used for dependency control
                     if depends_on is specified
    @param pod_labels: k8s labels to be added into the pod. Can be used for other controls like network policy
    @param depends_on: k8s job_name of another job. This job will wait until the specified job finished before it starts.
    @param image_pull_policy: image_pull_policy of the container
    """
    
    all_jobs = batch_v1.list_namespaced_job(namespace)
    for job in all_jobs.items:
        if job.metadata.name == job_name:
            print(f"job {job_name} already exist")
            return
    
    volumes = []
    control_volume_mount = None
    job_volume_mounts = []
    for pvc, path, is_control in storages:
        volume_name = convert_k8s_name(pvc, 'vol')
        volume = client.V1Volume(
                name=volume_name,
                persistent_volume_claim={"claimName": pvc},
        )
        volume_mount = client.V1VolumeMount(
                mount_path=path,
                name=volume_name
        )
        
        volumes.append(volume)
        job_volume_mounts.append(volume_mount)
        if is_control:
            control_volume_mount = volume_mount
    
    # convert env
    k8s_env = [client.V1EnvVar(str(k), str(v)) for (k, v) in env.items()]
    
    job_container = client.V1Container(
            name=JOB_CONTAINER_NAME,
            image=image,
            image_pull_policy=image_pull_policy,
            command=command,
            args=args,
            env=k8s_env,
            volume_mounts=job_volume_mounts
    )
    
    if control_volume_mount:
        pre_container = client.V1Container(
                name="pre",
                image="busybox",
                image_pull_policy=image_pull_policy,
                command=['/bin/sh', '-c'],
                args=[f"while [ ! -f /{control_volume_mount.mount_path}/.control-{depends_on} ]; do sleep 1; done"],
                volume_mounts=[control_volume_mount],
        )
        post_container = client.V1Container(
                name="post",
                image="busybox",
                image_pull_policy=image_pull_policy,
                command=['/bin/sh', '-c'],
                args=[f"touch /{control_volume_mount.mount_path}/.control-{job_name}"],
                volume_mounts=[control_volume_mount]
        )
        
        if depends_on is None:
            init_containers = [job_container]
        else:
            init_containers = [pre_container, job_container]
        containers = [post_container]
    else:
        if depends_on is not None:
            raise Exception("There must be a control storage if depends_on is not None")
        else:
            init_containers = None
            containers = [job_container]
    
    job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                    name=job_name,
                    labels={
                        "app": "os-test"
                    }
            ),
            spec=client.V1JobSpec(
                    backoff_limit=0,
                    template=client.V1PodTemplateSpec(
                            metadata=client.V1ObjectMeta(
                                    name=job_name,
                                    labels=pod_labels
                            ),
                            spec=client.V1PodSpec(
                                    restart_policy="Never",
                                    volumes=volumes,
                                    init_containers=init_containers,
                                    containers=containers,
                            )
                    )
            )
    )
    batch_v1.create_namespaced_job(body=job, namespace=namespace)
    
    if block_until_created:
        while read_k8s_job_status(job_name, namespace) == K8SJobStatus.UNKNOWN:
            time.sleep(.5)
    
    print(f"job {job_name} created")


def create_network_policy(namespace, address_ports):
    if address_ports and len(address_ports) > 0:
        np_name = convert_k8s_name(f"allow-{'-'.join([f'{ip}:{port}' for ip, port in address_ports])}")
    else:
        np_name = convert_k8s_name(f"deny-all")
    pod_label = {
        'network': np_name
    }
    
    all_np = networking_v1.list_namespaced_network_policy(namespace)
    for np in all_np.items:
        if np.metadata.name == np_name:
            print(f"network policy {np_name} already exist")
            return
    
    # resolve ip for domain
    ip_ports = []
    for address, port in address_ports:
        ip_list = list({addr[-1][0] for addr in socket.getaddrinfo(address, 0, 0, 0, 0)})
        print(f'resolved ip for {address}: {ip_list}')
        for ip in ip_list:
            ip_ports.append([ip, port])
    
    # create egress whitelist
    egress = []
    for ip, port in ip_ports:
        rule = client.V1NetworkPolicyEgressRule(
                to=[
                    {
                        'ipBlock': {
                            'cidr': f'{ip}/32'
                        }
                    }
                ],
                ports=[
                    client.V1NetworkPolicyPort(
                            protocol='TCP',
                            port=int(port)
                    ),
                    client.V1NetworkPolicyPort(
                            protocol='UDP',
                            port=int(port)
                    ),
                ])
        egress.append(rule)
    
    network_policy = client.V1NetworkPolicy(
            metadata=client.V1ObjectMeta(
                    name=np_name
            ),
            spec=client.V1NetworkPolicySpec(
                    pod_selector=client.V1LabelSelector(
                            match_labels=pod_label
                    ),
                    policy_types=[
                        'Egress'
                    ],
                    egress=egress,
            )
    )
    
    networking_v1.create_namespaced_network_policy(namespace, network_policy)
    print(f"network policy {np_name} created for {'-'.join([f'{ip}:{port}' for ip, port in ip_ports])}")
    return pod_label


def read_k8s_job_status(job_name: str, namespace: str) -> K8SJobStatus:
    all_jobs = batch_v1.list_namespaced_job(namespace)
    job_found = False
    for job in all_jobs.items:
        if job.metadata.name == job_name:
            job_found = True
    if not job_found:
        return K8SJobStatus.UNKNOWN
    
    status = batch_v1.read_namespaced_job(f"{job_name}", namespace=namespace).status
    if status.succeeded:
        return K8SJobStatus.SUCCEEDED
    elif status.failed:
        return K8SJobStatus.FAILED
    elif status.active != 1:
        return K8SJobStatus.PENDING
    
    # Active
    pods = core_v1.list_namespaced_pod(namespace=namespace)
    job_pods_status = [p.status for p in pods.items if p.metadata.labels.get('job-name') == job_name]  # get must be used to avoid error when key not found
    
    init_container_statuses = job_pods_status[-1].init_container_statuses
    if init_container_statuses and len(init_container_statuses) > 0:
        waiting = init_container_statuses[-1].state.waiting
        if waiting and waiting.reason == 'ImagePullBackOff':
            return K8SJobStatus.FAILED  # Fail to pull the image in the init_containers
    
    container_statuses = job_pods_status[-1].container_statuses
    if container_statuses and len(container_statuses) > 0:
        waiting = container_statuses[-1].state.waiting
        if waiting and waiting.reason == 'ImagePullBackOff':
            return K8SJobStatus.FAILED  # Fail to pull the image in the containers
    
    return K8SJobStatus.RUNNING


def list_pod_of_job(job_name: str, namespace: str) -> List:
    # logs: read logs of the job
    pods = core_v1.list_namespaced_pod(namespace=namespace)
    pods = [p for p in pods.items if p.metadata.labels.get('job-name') == job_name]  # get must be used to avoid error when key not found
    return pods


def read_log(job_name: str, namespace: str) -> Mapping[Tuple[str, str], str]:
    # logs: read logs of the job
    pods = list_pod_of_job(job_name, namespace)
    
    logs = {}
    for pod in pods:
        pod_name = pod.metadata.name
        
        all_containers = []
        if pod.spec.init_containers:
            for container in pod.spec.init_containers:
                all_containers.append(container.name)
        for container in pod.spec.containers:
            all_containers.append(container.name)
        
        for container_name in all_containers:
            try:
                logs[(pod_name, container_name)] = core_v1.read_namespaced_pod_log(pod_name, namespace=namespace, container=container_name)
            except Exception as e:
                print(e)
    
    return logs


def extract_k8s_api_values(data, removed_fields):
    if isinstance(data, list):
        if len(data) == 0:
            return None
        else:
            return [extract_k8s_api_values(d, removed_fields) for d in data]
    elif isinstance(data, dict):
        if len(data) == 0:
            return None
        else:
            result = {}
            for key, value in data.items():
                if key in removed_fields:
                    result[key] = '<removed>'
                else:
                    extracted = extract_k8s_api_values(value, removed_fields)
                    if extracted is not None:
                        result[key] = extracted
            if len(result) == 0:
                return None
            else:
                return result
    elif hasattr(data, 'attribute_map'):
        result = {}
        attrs = data.attribute_map.keys()
        for key in attrs:
            if key in removed_fields:
                result[key] = '<removed>'
            else:
                value = getattr(data, key)
                if value is not None:
                    extracted = extract_k8s_api_values(value, removed_fields)
                    if extracted is not None:
                        result[key] = extracted
        if len(result) == 0:
            return None
        else:
            return result
    elif data is None:
        return None
    else:
        return str(data)


def read_finalize_output(opensafely_job_name, opensafely_job_id, namespace):
    finalize_job_name = convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
    logs = read_log(finalize_job_name, namespace)
    container_log = ""
    for (_, container_name), container_log in logs.items():
        if container_name == JOB_CONTAINER_NAME:
            break
    for line in container_log.split('\n'):
        if line.startswith(JOB_RESULTS_TAG):
            job_result = line[len(JOB_RESULTS_TAG):]
            return json.loads(job_result)
    return None


def read_image_id(job_name, container_name, namespace):
    pods = list_pod_of_job(job_name, namespace)
    for pod in pods:
        pod_name = pod.metadata.name
        pod_status = core_v1.read_namespaced_pod_status(pod_name, namespace)
        
        for container_status in pod_status.status.container_statuses:
            if container_status.name == container_name:
                image_id = container_status.image_id
                if image_id and len(image_id) > 0:  # may not be the final pod
                    return image_id
        
        if pod_status.status.init_container_statuses:
            for container_status in pod_status.status.init_container_statuses:
                if container_status.name == container_name:
                    image_id = container_status.image_id
                    if image_id and len(image_id) > 0:  # may not be the final pod
                        return image_id
    
    return None


def read_container_exit_code(job_name, container_name, namespace):
    pods = list_pod_of_job(job_name, namespace)
    for pod in pods:
        pod_name = pod.metadata.name
        pod_status = core_v1.read_namespaced_pod_status(pod_name, namespace)
        
        for container_status in pod_status.status.container_statuses:
            if container_status.name == container_name:
                exit_code = container_status.state.terminated.exit_code
                if exit_code is not None:  # may not be the final pod
                    return exit_code
        
        if pod_status.status.init_container_statuses:
            for container_status in pod_status.status.init_container_statuses:
                if container_status.name == container_name:
                    exit_code = container_status.state.terminated.exit_code
                    if exit_code is not None:  # may not be the final pod
                        return exit_code
    
    return None


def await_job_status(job_name, namespace, sleep_interval=.5, timeout=5 * 60 * 60) -> Optional[K8SJobStatus]:
    # describe: read the status of the job until succeeded or failed
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = read_k8s_job_status(job_name, namespace)
        if status.completed():
            print("job completed")
            return status
        time.sleep(sleep_interval)
    return None
