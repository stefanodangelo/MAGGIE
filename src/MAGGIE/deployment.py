import mlflow
from mlflow.models import ModelConfig, ModelSignature, infer_signature
from databricks import agents
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate
import time
from .prompt import REVIEW_APP_INSTRUCTIONS

class DeploymentManager:
    def __init__(self, model_name_full_path: str) -> None:
        if len(model_name_full_path.split('.')) != 3:
            raise Exception(r"Please specify the full model path as {catalog}.{schema}.{model_name}")
        self.model_name = model_name_full_path.split('.')[-1]
        self.model_name_full_path = self.model_name_full_path.replace('`', '')

    def log_model(self, model_py_path: str, config_yaml_path: str, run_name: str):
        if '.yaml' not in config_yaml_path:
            raise Exception("Parameter `config_yaml_path` should be a YAML file.") 
        if '.py' not in model_py_path:
            raise Exception("Parameter `model_py_path` should be a PY file.") 
        
        self.model_config = ModelConfig(development_config=config_yaml_path)
        signature = infer_signature(self.model_config.get("input_example"), self.model_config.get("output_example")) #'[string (required)]')

        # Log the model to MLflow
        with mlflow.start_run(run_name=run_name):
            self.logged_chain_info = mlflow.langchain.log_model(
                lc_model=model_py_path,
                model_config=config_yaml_path,  # Chain configuration 
                artifact_path="chain",  # Required by MLflow
                # input_example=model_config.get("input_example"),  # Save the chain's input schema.  MLflow will execute the chain before logging & capture it's output schema.
                # example_no_conversion=True,  # Required by MLflow to use the input_example as the chain's schema
                signature=signature
            )

        # Register the chain to UC
        self.uc_registered_model_info = mlflow.register_model(model_uri=self.logged_chain_info.model_uri, name=self.model_name)

    def deploy_model(self):
        # Deploy to enable the Review APP and create an API endpoint
        self.deployment_info = agents.deploy(model_name=self.model_name_full_path, model_version=self.uc_registered_model_info.version, scale_to_zero=True)

        # Add the user-facing instructions to the Review App
        agents.set_review_instructions(self.model_name_full_path, REVIEW_APP_INSTRUCTIONS)
        # self._wait_for_model_serving_endpoint_to_be_ready(5)

    def _wait_for_model_serving_endpoint_to_be_ready(self, seconds_interval=10):
        # Wait for it to be ready
        w = WorkspaceClient()
        state = ""
        for i in range(200):
            state = w.serving_endpoints.get(self.deployment_info.endpoint_name).state
            if state.config_update == EndpointStateConfigUpdate.IN_PROGRESS:
                if i % 40 == 0:
                    print(f"Waiting for endpoint to deploy {self.deployment_info.endpoint_name}. Current state: {state}")
                time.sleep(seconds_interval)
            elif state.ready == EndpointStateReady.READY:
                print('endpoint ready.')
                return
            else:
                break
        raise Exception(f"Couldn't start the endpoint, timeout, please check your endpoint for more details: {state}")