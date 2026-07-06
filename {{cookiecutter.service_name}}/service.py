# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
import pathlib
from typing import Optional

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()

import json
import os
import sys
from urllib.parse import urlparse

import boto3  # noqa: F401
import botocore
import jwt
import requests
import yaml
from botocore.exceptions import ClientError
from loguru import logger
from pystac import read_file
from pystac.stac_io import DefaultStacIO, StacIO
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner
from botocore.client import Config
from pystac.item_collection import ItemCollection
from kubernetes import client, config

# For DEBUG
import traceback

logger.remove()
logger.add(sys.stderr, level="INFO")

KEYCLOAK_BASE_URL = os.environ.get("KEYCLOAK_BASE_URL", "keycloak-base-url")
CLIENT_ID = os.environ.get("CLIENT_ID", "client-id")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "client-secret")

KEYCLOAK_URL = f"https://{KEYCLOAK_BASE_URL}/protocol/openid-connect/revoke"

class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()
        # Two pathways provided here to support authorisation via:
        # 1) AWS credentials, when keys are provided as environment variables or,
        # 2) Service Account, when AWS credentials are not provided as environment variables
        if os.environ["AWS_ACCESS_KEY_ID"] and os.environ["AWS_SECRET_ACCESS_KEY"]:
            self.s3_client = self.session.create_client(
                service_name="s3",
                region_name=os.environ.get("AWS_REGION"),
                endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                verify=True,
                use_ssl=True,
                config=Config(s3={"addressing_style": "path", "signature_version": "s3v4"}),
            )
        else:
            self.s3_client = self.session.create_client(
                service_name="s3",
                verify=True,
                use_ssl=True,
                config=Config(s3={"addressing_style": "path", "signature_version": "s3v4"}),
            )

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.path.startswith("/files/") and os.environ.get("WORKSPACE_DOMAIN") in parsed.netloc:
            # URL is a http path to a workspace file, get directly from s3
            parts = parsed.path.split("/", 3)
            return (
                self.s3_client.get_object(Bucket=parts[2], Key=f"{parsed.netloc.split('.')[0]}/{parts[3]}")[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        elif parsed.scheme == "s3":
            pod_name = os.getenv('HOSTNAME')
            logger.info(f"Current pod name: {pod_name}")

            return (
                self.s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=parsed.netloc,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        else:
            super().write_text(dest, txt, *args, **kwargs)


StacIO.set_default(CustomStacIO)


class EoepcaCalrissianRunnerExecutionHandler(ExecutionHandler):
    def __init__(self, conf, inputs):
        super().__init__()
        self.conf = conf
        self.inputs = inputs

        self.http_proxy_env = os.environ.get("HTTP_PROXY", None)

        eoepca = self.conf.get("eoepca", {})
        self.domain = eoepca.get("domain", "")
        self.workspace_url = eoepca.get("workspace_url", "")
        self.workspace_prefix = eoepca.get("workspace_prefix", "")
        if self.workspace_url and self.workspace_prefix:
            self.use_workspace = True
        else:
            self.use_workspace = False

        self.calling_workspace_name = self.inputs.get("calling_workspace", {}).get("value", "default")
        self.executing_workspace_name = self.inputs.get("executing_workspace", {}).get("value", "default")

        auth_env = self.conf.get("auth_env", {})
        self.ades_rx_token = auth_env.get("jwt", "")

        self.feature_collection = None

        self.init_config_defaults(self.conf)

    def pre_execution_hook(self):
        try:
            logger.info("Pre execution hook")
            self.unset_http_proxy_env()

            # DEBUG
            # logger.info(f"zzz PRE-HOOK - config...\n{json.dumps(self.conf, indent=2)}\n")

            # decode the JWT token to get the user name
            if self.ades_rx_token:
                self.username = self.get_user_name(
                    jwt.decode(self.ades_rx_token, options={"verify_signature": False})
                )

            if self.use_workspace:
                logger.info("Lookup storage details in Workspace")

                # Workspace API endpoint
                uri_for_request = f"workspaces/{self.workspace_prefix}-{self.username}"

                workspace_api_endpoint = os.path.join(self.workspace_url, uri_for_request)
                logger.info(f"Using Workspace API endpoint {workspace_api_endpoint}")

                # Request: Get Workspace Details
                headers = {
                    "accept": "application/json",
                    "Authorization": f"Bearer {self.ades_rx_token}",
                }
                get_workspace_details_response = requests.get(workspace_api_endpoint, headers=headers)

                # GOOD response from Workspace API - use the details
                if get_workspace_details_response.ok:
                    workspace_response = get_workspace_details_response.json()

                    logger.info("Set user bucket settings")

                    storage_credentials = workspace_response["storage"]["credentials"]

                    self.conf["additional_parameters"]["STAGEIN_AWS_SERVICEURL"] = storage_credentials.get("endpoint")
                    self.conf["additional_parameters"]["STAGEIN_AWS_REGION"] = storage_credentials.get("region")

                    self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = storage_credentials.get("endpoint")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = storage_credentials.get("access")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = storage_credentials.get("secret")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = storage_credentials.get("region")
                    self.conf["additional_parameters"]["STAGEOUT_OUTPUT"] = storage_credentials.get("bucketname")
                # BAD response from Workspace API - fallback to the 'pre-configured storage details'
                else:
                    logger.error("Problem connecting with the Workspace API")
                    logger.info(f"  Response code = {get_workspace_details_response.status_code}")
                    logger.info(f"  Response text = \n{get_workspace_details_response.text}")
                    self.use_workspace = False
                    logger.info("Using pre-configured storage details")
            else:
                logger.info("Using pre-configured storage details")

            if self.executing_workspace_name in ["airbus", "planet", "open-cosmos"]:
                output_prefix = f"commercial-data"
            else:
                output_prefix = "processing-results/{{cookiecutter.workflow_id}}"

            lenv = self.conf.get("lenv", {})
            self.conf["additional_parameters"]["job_id"] = lenv.get("usid", "")
            self.conf["additional_parameters"]["process"] = output_prefix
            self.conf["additional_parameters"]["CALLING_WORKSPACE"] = self.calling_workspace_name
            self.conf["additional_parameters"]["EXECUTING_WORKSPACE"] = self.executing_workspace_name
            self.conf["additional_parameters"]["workflow_id"] = "{{cookiecutter.workflow_id}}"
            self.conf["additional_parameters"]["token"] = self.inputs.get("user_token", {}).get("value", "")

        except Exception as e:
            logger.error("ERROR in pre_execution_hook...")
            logger.error(traceback.format_exc())
            raise(e)

        finally:
            self.restore_http_proxy_env()

    def post_execution_hook(self, log, output, usage_report, tool_logs):
        try:
            logger.info("Post execution hook")
            self.unset_http_proxy_env()

            # DEBUG
            # logger.info(f"zzz POST-HOOK - config...\n{json.dumps(self.conf, indent=2)}\n")

            logger.info("Set user bucket settings")
            os.environ["AWS_S3_ENDPOINT"] = self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"]
            os.environ["AWS_ACCESS_KEY_ID"] = self.conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"]
            os.environ["AWS_REGION"] = self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"]
            os.environ["STAGEOUT_PULSAR_URL"] = self.conf["additional_parameters"]["STAGEOUT_PULSAR_URL"]
            os.environ["WORKSPACE_DOMAIN"] = self.conf["additional_parameters"]["WORKSPACE_DOMAIN"]

            StacIO.set_default(CustomStacIO)

            logger.info(f"Read catalog => STAC Catalog URI: {output['StacCatalogUri']}")
            try:
                s3_path = output["StacCatalogUri"]
                if s3_path.count("s3://")==0:
                    s3_path = "s3://" + s3_path
                cat = read_file(s3_path)
            except Exception as e:
                logger.error(f"Exception: {e}")

            job_id = self.conf["additional_parameters"]["job_id"]
            collection = None
            try:
                collection = next(cat.get_all_collections())
                collection_id = collection.id
                logger.info("Got collection from outputs")
            except:
                try:
                    logger.info("No collection found in outputs, creating from items")
                    collection_id = "collection" # some default ID
                    items=cat.get_all_items()
                    itemFinal=[]
                    for i in items:
                        for a in i.assets.keys():
                            cDict=i.assets[a].to_dict()
                            cDict["storage:platform"]="EOEPCA"
                            cDict["storage:requester_pays"]=False
                            cDict["storage:tier"]="Standard"
                            cDict["storage:region"]=self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"]
                            cDict["storage:endpoint"]=self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"]
                            i.assets[a]=i.assets[a].from_dict(cDict)
                        i.collection_id=collection_id
                        itemFinal+=[i.clone()]
                    collection = ItemCollection(items=itemFinal)
                    logger.info("Created collection from items")
                except Exception as e:
                    logger.error(f"Exception: {e}"+str(e))

            # Trap the case of no output collection
            if collection is None:
                logger.error("ABORT: The output collection is empty")
                self.feature_collection = json.dumps({}, indent=2)
                return

            collection_dict=collection.to_dict()
            collection_dict["id"]=collection_id

            # Update links with HTTPS links
            workspace_domain = self.conf["additional_parameters"]["WORKSPACE_DOMAIN"]
            bucket = self.conf["additional_parameters"]["STAGEOUT_OUTPUT"]
            workspace = self.conf["additional_parameters"]["CALLING_WORKSPACE"]
            subfolder = self.conf["additional_parameters"]["process"]
            if workspace_domain:
                for link in collection_dict["links"]:
                    if "href" in link:
                        link["href"] = link["href"].replace(f"s3://{bucket}/{os.path.join(workspace, subfolder)}",
                                                            f"https://{workspace}.{workspace_domain}/files/{bucket}/{subfolder}")
                        logger.info("Updated link href to " + link["href"])

            # Set the feature collection to be returned
            self.feature_collection = json.dumps(collection_dict, indent=2)

            # Register with the workspace
            if self.use_workspace:
                logger.info(f"Register collection in workspace {self.workspace_prefix}-{self.username}")
                headers = {
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.ades_rx_token}",
                }
                api_endpoint = f"{self.workspace_url}/workspaces/{self.workspace_prefix}-{self.username}"
                r = requests.post(
                    f"{api_endpoint}/register-json",
                    json=collection_dict,
                    headers=headers,
                )
                logger.info(f"Register collection response: {r.status_code}")

                # TODO pool the catalog until the collection is available
                #self.feature_collection = requests.get(
                #    f"{api_endpoint}/collections/{collection.id}", headers=headers
                #).json()

                logger.info(f"Register processing results to collection")
                r = requests.post(f"{api_endpoint}/register",
                                json={"type": "stac-item", "url": collection.get_self_href()},
                                headers=headers,)
                logger.info(f"Register processing results response: {r.status_code}")

        except Exception as e:
            logger.error("ERROR in post_execution_hook...")
            logger.error(traceback.format_exc())
            raise(e)

        finally:
            self.restore_http_proxy_env()

    def unset_http_proxy_env(self):
        http_proxy = os.environ.pop("HTTP_PROXY", None)
        logger.info(f"Unsetting env HTTP_PROXY, whose value was {http_proxy}")

    def restore_http_proxy_env(self):
        if self.http_proxy_env:
            os.environ["HTTP_PROXY"] = self.http_proxy_env
            logger.info(f"Restoring env HTTP_PROXY, to value {self.http_proxy_env}")

    @staticmethod
    def init_config_defaults(conf):
        if "additional_parameters" not in conf:
            conf["additional_parameters"] = {}

        conf["additional_parameters"]["STAGEIN_AWS_SERVICEURL"] = os.environ.get("STAGEIN_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000")
        conf["additional_parameters"]["STAGEIN_AWS_ACCESS_KEY_ID"] = os.environ.get("STAGEIN_AWS_ACCESS_KEY_ID", "minio-admin")
        conf["additional_parameters"]["STAGEIN_AWS_SECRET_ACCESS_KEY"] = os.environ.get("STAGEIN_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        conf["additional_parameters"]["STAGEIN_AWS_REGION"] = os.environ.get("STAGEIN_AWS_REGION", "RegionOne")

        conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = os.environ.get("STAGEOUT_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000")
        conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = os.environ.get("STAGEOUT_AWS_ACCESS_KEY_ID", "minio-admin")
        conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = os.environ.get("STAGEOUT_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = os.environ.get("STAGEOUT_AWS_REGION", "RegionOne")
        conf["additional_parameters"]["STAGEOUT_OUTPUT"] = os.environ.get("STAGEOUT_OUTPUT", "eoepca")
        conf["additional_parameters"]["CALLING_WORKSPACE"] = os.environ.get("CALLING_WORKSPACE", "default")
        conf["additional_parameters"]["EXECUTING_WORKSPACE"] = os.environ.get("EXECUTING_WORKSPACE", "default")
        conf["additional_parameters"]["STAGEOUT_PULSAR_URL"] = os.environ.get("STAGEOUT_PULSAR_URL", None)
        conf["additional_parameters"]["WORKSPACE_DOMAIN"] = os.environ.get("WORKSPACE_DOMAIN", None)

        # DEBUG
        # logger.info(f"init_config_defaults: additional_parameters...\n{json.dumps(conf['additional_parameters'], indent=2)}\n")

    @staticmethod
    def get_user_name(decodedJwt) -> str:
        for key in ["username", "user_name", "preferred_username"]:
            if key in decodedJwt:
                return decodedJwt[key]
        return ""

    @staticmethod
    def local_get_file(fileName):
        """
        Read and load the contents of a yaml file

        :param yaml file to load
        """
        try:
            with open(fileName, "r") as file:
                data = yaml.safe_load(file)
            return data
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}

    def get_pod_env_vars(self):
        logger.info("get_pod_env_vars")

        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):
        logger.info("get_pod_node_selector")

        return self.conf.get("pod_node_selector", {})

    def get_secrets(self):
        logger.info("get_secrets")

        return self.local_get_file("/assets/pod_imagePullSecrets.yaml")

    def get_additional_parameters(self):
        logger.info("get_additional_parameters")

        return self.conf.get("additional_parameters", {})

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """
        try:
            logger.info("handle_outputs")

            self.conf['main']['tmpUrl']=self.conf['main']['tmpUrl'].replace("temp/",self.conf["auth_env"]["user"]+"/temp")

            servicesLogs = [
                {
                    "url": f"{self.conf['main']['tmpUrl']}/"
                            f"{self.conf['lenv']['Identifier']}-{self.conf['lenv']['usid']}/"
                            f"{os.path.basename(tool_log)}",
                    "title": f"Tool log {os.path.basename(tool_log)}",
                    "rel": "related",
                }
                for tool_log in tool_logs
            ]
            logger.info(f"servicesLogs: {servicesLogs}")
            for i in range(len(servicesLogs)):
                okeys = ["url", "title", "rel"]
                keys = ["url", "title", "rel"]
                if i > 0:
                    for j in range(len(keys)):
                        keys[j] = keys[j] + "_" + str(i)
                if "service_logs" not in self.conf:
                    self.conf["service_logs"] = {}
                for j in range(len(keys)):
                    self.conf["service_logs"][keys[j]] = servicesLogs[i][okeys[j]]

            self.conf["service_logs"]["length"] = str(len(servicesLogs))

        except Exception as e:
            logger.error("ERROR in handle_outputs...")
            logger.error(traceback.format_exc())
            raise(e)


def deactivate_api_token(token: Optional[str], token_name: str):
    """
    Deactivate API Access Token
    """
    if not token:
        logger.error(f"Failed to deactivate token for {token_name}: no token provided")
        return
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "token_type_hint": "access_token",
        "token": token,
    }

    response = requests.post(
        KEYCLOAK_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if response.status_code == 200:
        logger.info(f"Token for {token_name} deactivated successfully")
    else:
        logger.error(f"Failed to deactivate token for {token_name}: {response.status_code} - {response.text}")


def delete_pvc(v1, name: str, namespace: str = "default"):
    """"
    Delete the specified Persistent Volume Claim
    """
    try:
        v1.delete_namespaced_persistent_volume_claim(
            name=name,
            namespace=namespace,
            body=client.V1DeleteOptions()
        )
        logger.info(f"PVC {name} deleted successfully")
    except client.exceptions.ApiException as e:
        logger.error(f"Exception when deleting PVC {name}: {e}")

def delete_service_account(v1, name: str, namespace: str = "default"):
    """"
    Delete the specified temporary service account
    """
    try:
        v1.delete_namespaced_service_account(
            name=name,
            namespace=namespace,
            body=client.V1DeleteOptions()
        )
        logger.info(f"Service Account {name} deleted successfully")
    except client.exceptions.ApiException as e:
        logger.error(f"Exception when deleting Service Account {name}: {e}")

def delete_configmap(v1, name: str, namespace: str = "default"):
    """"
    Delete the specified ConfigMap
    """
    try:
        v1.delete_namespaced_config_map(
            name=name,
            namespace=namespace,
            body=client.V1DeleteOptions()
        )
        logger.info(f"ConfigMap {name} deleted successfully")
    except client.exceptions.ApiException as e:
        logger.error(f"Exception when deleting ConfigMap {name}: {e}")

def get_namespace_from_workspace(custom_api, workspace: str):
    """"
    Get the namespace associated with the given workspace
    """
    try:
        workspace = custom_api.get_namespaced_custom_object(
            group="core.telespazio-uk.io",
            version="v1alpha1",
            namespace="workspaces",
            plural="workspaces",
            name=workspace,
        )
    except Exception as e:
        logger.error(f"Error in getting {workspace} workspace CRD: {e}")
        raise e
    return workspace["spec"]["namespace"]


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs): # noqa

    try:
        with open(
            os.path.join(
                pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
                "app-package.cwl",
            ),
            "r",
        ) as stream:
            cwl = yaml.safe_load(stream)

        is_user_service = inputs["executing_workspace"]["value"] != inputs["calling_workspace"]["value"]

        # Load kubeconfig
        config.load_incluster_config()

        # Create a CoreV1Api client instance for use later
        v1 = client.CoreV1Api()

        # Create a CustomObjectsApi client instance
        custom_api = client.CustomObjectsApi()

        # Extract workspace names
        executing_workspace_name = inputs["executing_workspace"]["value"]
        calling_workspace_name = inputs["calling_workspace"]["value"]

        # Access workspace details for the executing workspace
        executing_namespace = get_namespace_from_workspace(custom_api, executing_workspace_name)

        conf.setdefault("eodhp", {})
        conf["eodhp"]["serviceAccountNameCalling"] = "default" # need to determine the correct one if user service
        conf["eodhp"]["serviceAccountName"] = "default"

        calling_service_account_name = None

        if is_user_service:
            calling_namespace = get_namespace_from_workspace(custom_api, calling_workspace_name)
            name_prefix = f"temp-{calling_namespace}"
            calling_service_account_name = f"{name_prefix}-{conf['lenv']['usid']}"
            if len(calling_service_account_name) > 63:
                calling_service_account_name = calling_service_account_name[:63]
            calling_service_account = v1.read_namespaced_service_account(name="default", namespace=calling_namespace)
            # Extract annotations
            annotations = calling_service_account.metadata.annotations
            # Create temporary service account for this workspace
            # Define the service account metadata
            service_account = client.V1ServiceAccount(
                metadata=client.V1ObjectMeta(name=calling_service_account_name, annotations=annotations)
            )

            # Create the service account in the specified namespace
            created_sa = v1.create_namespaced_service_account(
                namespace=executing_namespace, body=service_account
            )

            logger.info(f"Service account {calling_service_account_name} created in namespace {executing_namespace}")

            conf["eodhp"]["serviceAccountNameCalling"] = calling_service_account_name

        execution_handler = EoepcaCalrissianRunnerExecutionHandler(conf=conf, inputs=inputs)

        runner = ZooCalrissianRunner(
            cwl=cwl,
            conf=conf,
            inputs=inputs,
            outputs=outputs,
            execution_handler=execution_handler,
        )
        # DEBUG
        # runner.monitor_interval = 1

        # we are changing the working directory to store the outputs
        # in a directory dedicated to this execution
        working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
        os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(working_dir)

        runner._namespace_name = executing_namespace

        exit_status = runner.execute()

        # Set job_id
        job_id = conf["lenv"]["usid"]

        # Delete no longer needed resources
        delete_configmap(v1, f"params-{job_id}", executing_namespace)
        delete_configmap(v1, f"cwl-workflow-{job_id}", executing_namespace)
        delete_configmap(v1, f"pod-node-selector-{job_id}", executing_namespace)
        delete_configmap(v1, f"pod-env-vars-{job_id}", executing_namespace)

        delete_pvc(v1, f"calrissian-wdir-{job_id}", executing_namespace)

        if calling_service_account_name:
            delete_service_account(v1, calling_service_account_name, executing_namespace)

        # Deactivate workspace API tokens for both calling and executing workspace
        deactivate_api_token(inputs.get("CALLING_WORKSPACE_ACCESS_TOKEN", {}).get("value"), "CALLING_WORKSPACE_ACCESS_TOKEN")
        deactivate_api_token(inputs.get("WORKSPACE_ACCESS_TOKEN", {}).get("value"), "EXECUTING_WORKSPACE_ACCESS_TOKEN")

        if exit_status == zoo.SERVICE_SUCCEEDED:
            logger.info(f"Setting Collection into output key {list(outputs.keys())[0]}")
            outputs[list(outputs.keys())[0]]["value"] = execution_handler.feature_collection
            return zoo.SERVICE_SUCCEEDED

        else:
            conf["lenv"]["message"] = zoo._("Execution failed")
            raise Exception("Execution failed")

    except Exception as e:
        logger.error("ERROR in processing execution template...")
        logger.info("Try fetching logs if any...")
        try:
            tool_logs = runner.execution.get_tool_logs()
            execution_handler.handle_outputs(None, None, None, tool_logs)
        except Exception as e:
            logger.error("Fetching logs failed!"+str(e))
        stack = traceback.format_exc()
        logger.error(stack)
        conf["lenv"]["message"] = zoo._(f"Exception during execution...\n{stack}\n")
        return zoo.SERVICE_FAILED
