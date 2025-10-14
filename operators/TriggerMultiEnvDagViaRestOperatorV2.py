from typing import List, Any
from typing import Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from operators.logging import log_factory
from utils.composer import astro_webserver_url

logger = log_factory.getLogger(__name__)


class TriggerMultiEnvDagViaRestOperatorV2(BaseOperator):
    """
    Trigger DAG Run Via Rest API, It can also be used for Dag Runs in Different Airflow Environment
    """

    template_fields = (
        'env_name',
        'trigger_dag_id',
        'trigger_dagrun_payloads',
    )

    @apply_defaults
    def __init__(
        self,
        env_name: str,
        trigger_dag_id: str,
        trigger_dagrun_payloads: List[Dict],
        ignore_existing_dagrun: bool = True,
        astro_token_var: str = "astro_organisation_token",
        method: str = "POST",
        *args,
        **kwargs,
    ):
        self.env_name = env_name
        self.trigger_dag_id = trigger_dag_id
        self.trigger_dagrun_payloads = trigger_dagrun_payloads
        self.ignore_existing_dagrun = ignore_existing_dagrun
        self.astro_token = Variable.get(astro_token_var)
        self.method = method
        super().__init__(*args, **kwargs)

    def _build_session(self) -> requests.Session:
        retry_cfg = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods={self.method},
        )
        s = requests.Session()
        s.mount("https://", HTTPAdapter(max_retries=retry_cfg))
        return s

    def _make_airflow_api_request(
        self,
        webserver_url: str,
        session: requests.Session,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Make a request to Airflow environments web server.
        """
        url = f"https://{webserver_url}/dags/{self.trigger_dag_id}/dagRuns"
        headers = kwargs.get("headers", {})
        headers.update(
            {
                "Authorization": f"Bearer {self.astro_token}",
                "Content-Type": "application/json",
            }
        )
        kwargs["headers"] = headers

        # Set the default timeout, if missing
        if "timeout" not in kwargs:
            kwargs["timeout"] = 90

        return session.request(self.method, url, **kwargs)

    def execute(self, context: Dict):
        session = self._build_session()
        webserver_url = astro_webserver_url(self.env_name, self.astro_token)
        logger.info(f"Airflow URL is: {webserver_url}")
        for dag_trigger_json in self.trigger_dagrun_payloads:
            response = self._make_airflow_api_request(
                webserver_url, session, json=dag_trigger_json
            )
            if response.status_code == 409:
                if self.ignore_existing_dagrun:
                    logger.error(
                        f"DAG Run already exist in env: {self.env_name} for config: {dag_trigger_json}"
                    )
                else:
                    raise AirflowException(
                        f"Dag Run already exists in env: {self.env_name} for: {dag_trigger_json}"
                    )
            elif response.status_code == 200:
                logger.info(
                    f"Successfully Triggered DagRun in env: {self.env_name} for config: {dag_trigger_json}"
                )
            else:
                raise AirflowException(
                    f"Error while Triggering Dag Run in env: {self.env_name} for config: {dag_trigger_json}, Status Code: {response.status_code} with error:"
                    f" {response.headers} / {response.text}"
                )
