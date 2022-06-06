# encoding: utf-8
import os
import requests
from datetime import date
from ckan.common import config
from ckan.plugins.toolkit import get_action
import logging
import json
import time
from ckan.common import request
import ckan.logic as logic
import ckan.lib.helpers as h
import google.auth
import six.moves.urllib.parse
from google.auth.transport.requests import Request, AuthorizedSession

from google.oauth2 import id_token, service_account

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

REACHED_RESOPONSE  = False
AIRCAN_RESPONSE_AFTER_SUBMIT = None

log = logging.getLogger(__name__)

ValidationError = logic.ValidationError
NotFound = logic.NotFound

NO_SCHEMA_ERROR_MESSAGE = 'Resource <a href="{0}">{1}</a> has no schema so cannot be imported into the DataStore.'\
                        ' Please add a Table Schema in the resource schema attribute.'\
                        ' See <a href="https://github.com/datopian/ckanext-aircan#airflow-instance-on-google-composer"> Airflow instance on Google Composer </a>' \
                        ' section in AirCan docs for more.'

class GCPHandler:
    def __init__(self, config, payload):
        self.config = config
        self.payload = payload

    def get_auth_session(self):
        local_config_str = self.config.get('ckan.airflow.cloud.google_application_credentials')
        parsed_credentials = json.loads(local_config_str)
        credentials = service_account.Credentials.from_service_account_info(parsed_credentials, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        authed_session = AuthorizedSession(credentials)
        return authed_session

    def get_env_url(self):
        project_id = self.config.get('ckan.airflow.cloud.project_id')
        location = self.config.get('ckan.airflow.cloud.location')
        composer_environment = self.config.get('ckan.airflow.cloud.composer_environment')
        environment_url = (
            'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
            '/environments/{}').format(project_id, location, composer_environment)
        return environment_url


    def client_setup(self):
        authed_session = self.get_auth_session()
        environment_url = self.get_env_url()
        composer_response = authed_session.request('GET', environment_url)
        environment_data = composer_response.json()
        airflow_uri = environment_data['config']['airflowUri']
        redirect_response = requests.get(airflow_uri, allow_redirects=False)
        redirect_location = redirect_response.headers['location']
        parsed = six.moves.urllib.parse.urlparse(redirect_location)
        query_string = six.moves.urllib.parse.parse_qs(parsed.query)
        client_id = query_string['client_id'][0]
        return client_id


    def trigger_dag(self):
        log.info("Trigger DAG on GCP")
        client_id = self.client_setup()
        log.info("clien_id: {}".format(client_id))
        # This should be part of your webserver's URL:
        # {tenant-project-id}.appspot.com
        webserver_id = self.config.get('ckan.airflow.cloud.web_ui_id')
        log.info("webserver_id: {}".format(webserver_id))
        dag_name = self.config.get('ckan.airflow.cloud.dag_name')
        log.info("dag_name: {}".format(dag_name))
        webserver_url = (
            'https://'
            + webserver_id
            + '.appspot.com/api/experimental/dags/'
            + dag_name
            + '/dag_runs'
        )
        log.info("webserver_url: {}".format(webserver_url))
        # Make a POST request to IAP which then Triggers the DAG
        return self.make_iap_request(webserver_url, client_id, method='POST', json=self.payload)


    def get_google_token_id(self, client_id):
        local_config_str = self.config.get('ckan.airflow.cloud.google_application_credentials')
        parsed_credentials = json.loads(local_config_str)
        credentials = service_account.IDTokenCredentials.from_service_account_info(parsed_credentials, target_audience=client_id)
        request = Request()
        credentials.refresh(request)
        return credentials.token

    def make_iap_request(self, url, client_id, method='GET', **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 90
        
        google_open_id_connect_token = self.get_google_token_id(client_id)
        resp = requests.request(
            method, url,
            headers={'Authorization': 'Bearer {}'.format(
                google_open_id_connect_token)}, **kwargs)
        log.info('Request sent to GCP. Response code: {!r} '.format(resp.status_code))
        
        if resp.status_code == 403:
            raise Exception('Service account does not have permission to '
                            'access the IAP-protected application.')
        elif resp.status_code != 200:
            raise Exception(
                'Bad response from application: {!r} / {!r} / {!r}'.format(
                    resp.status_code, resp.headers, resp.text))
        else:
            return resp.json()

class DagStatusReport:
    def __init__(self, dag_name, execution_date, config):
        self.dag_name = dag_name
        self.config = config
        self.execution_date = (("/" + str(execution_date)) if execution_date != '' else '')

    def get_local_aircan_report(self):
        log.info("Building Airflow local status report")
        ckan_airflow_endpoint_url = self.config.get('ckan.airflow.url')
        log.info("Airflow Endpoint URL: {0}".format(ckan_airflow_endpoint_url))
        response = requests.get(ckan_airflow_endpoint_url,
                                 headers={'Content-Type': 'application/json',
                                          'Cache-Control': 'no-cache'})
        log.info(response.text)
        response.raise_for_status()
        log.info('Airflow status request completed')
        return {"success": True, "airflow_api_aircan_status": response.json()}

    def get_gcp_report(self):
        log.info("Building GCP DAG status report")
        gcp = GCPHandler(self.config, {})
        client_id = gcp.client_setup()
        webserver_id = self.config.get('ckan.airflow.cloud.web_ui_id')
        webserver_url = (
            'https://'
            + webserver_id
            + '.appspot.com/api/experimental/dags/'
            + self.dag_name
            + '/dag_runs'
            + (self.execution_date)
        )
        
        airflow_api_status = gcp.make_iap_request(webserver_url, client_id, method='GET')

        return {"success": True, "airflow_api_aircan_status": airflow_api_status, "gcp_logs": {} }

    def get_gcp_logs_for_dag(self):
        project_id = self.config.get('ckan.airflow.cloud.project_id', "")
        local_config_str = self.config.get('ckan.airflow.cloud.google_application_credentials')
        parsed_credentials = json.loads(local_config_str)
        credentials = service_account.Credentials.from_service_account_info(parsed_credentials, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        client = logging.client.Client(project_id, credentials=credentials)
        entries_filter = "resource.type:cloud_composer_environment AND resource.labels.location:us-east1 AND resource.labels.environment_name:aircan-airflow AND" + self.dag_name
        entries = client.list_entries([project_id], filter_=entries_filter)
        return entries
          
          
def aircan_submit(context, data_dict):
    log.info("Submitting resource via Aircan")
    try:
        res_id = data_dict['resource_id']
        
        user = get_action('user_show')(context, {'id': context['user']})
        ckan_api_key = user['apikey']
        
        ckan_resource = data_dict.get('resource_json', {})
        ckan_resource_url = config.get('ckan.site_url') + '/dataset/' + ckan_resource.get('package_id') + '/resource/' + res_id
        ckan_resource_name = ckan_resource.get('name')


        '''Sample schema structure we are expecting to receive frfom ckan_resource.get('schema')
            schema = {
                "fields": [
                    {
                        "name": "FID",
                        "title": "FID",
                        "type": "number",
                        "description": "FID`"
                    },
                    {
                        "name": "MktRF",
                        "title": "MktRF",
                        "type": "number",
                        "description": "MktRF`"
                    },
                    {
                        "name": "SMB",
                        "title": "SMB",
                        "type": "number",
                        "description": "SMB`"
                    },
                    {
                        "name": "HML",
                        "title": "HML",
                        "type": "number",
                        "description": "HML`"
                    },
                    {
                        "name": "RF",
                        "title": "RF",
                        "type": "number",
                        "description": "RF`"
                    }
                ]
        }
        '''

        table_schema = ckan_resource.get('schema')
        if not table_schema:    
            raise ValueError()
        schema = json.dumps(table_schema)

        # create giftless resource file uri to be passed to aircan
        pacakge_name = data_dict['pacakge_name']
        organization_name = data_dict['organization_name']
        resource_hash = data_dict['resource_hash']
        giftless_bucket = config.get('ckan.giftless.bucket', '')
        gcs_uri = 'gs://%s/%s/%s/%s' % (giftless_bucket, organization_name, pacakge_name, resource_hash)
        log.debug("gcs_uri: {}".format(gcs_uri))

        bq_table_name = ckan_resource.get('bq_table_name')
        log.debug("bq_table_name: {}".format(bq_table_name))
        payload = { 
            "conf": {
                "resource": {
                    "path": ckan_resource.get('url'),
                    "format": ckan_resource.get('format'),
                    "ckan_resource_id": res_id,
                    "schema": schema
                },
                "ckan_config": {
                    "api_key": ckan_api_key,
                    "site_url": config.get('ckan.site_url'),    
                },
                "big_query": {
                    "gcs_uri": gcs_uri,
                    "bq_project_id": config.get('ckanext.bigquery.project', 'NA'),
                    "bq_dataset_id": config.get('ckanext.bigquery.dataset', 'NA'),
                    "bq_table_name": bq_table_name
                },
                "output_bucket": str(date.today())
            }
        }
        log.debug("payload: {}".format(payload))
        global REACHED_RESOPONSE
        REACHED_RESOPONSE = True
        global AIRCAN_RESPONSE_AFTER_SUBMIT 

        if config.get('ckan.airflow.cloud','local') != "GCP":
            ckan_airflow_endpoint_url = config.get('ckan.airflow.url')
            log.info("Airflow Endpoint URL: {0}".format(ckan_airflow_endpoint_url))
            response = requests.post(ckan_airflow_endpoint_url,
                                     data=json.dumps(payload),
                                     headers={'Content-Type': 'application/json',
                                              'Cache-Control': 'no-cache'})
            log.info(response.text)
            response.raise_for_status()
            log.info('AirCan Load completed')
            
            AIRCAN_RESPONSE_AFTER_SUBMIT = {"aircan_status": response.json()}
        else:
            log.info("Invoking Airflow on Google Cloud Composer")
            dag_name = request.params.get('dag_name')
            if dag_name:
                config['ckan.airflow.cloud.dag_name'] = dag_name
            gcp_response = invoke_gcp(config, payload)
            AIRCAN_RESPONSE_AFTER_SUBMIT = {"aircan_status": gcp_response}
    except ValueError:
        log.error(NO_SCHEMA_ERROR_MESSAGE)
        h.flash_error(NO_SCHEMA_ERROR_MESSAGE.format(ckan_resource_url , ckan_resource_name),  allow_html=True)
    except Exception as e:
        return {"success": False, "errors": [e]}

    if REACHED_RESOPONSE == True:
        return AIRCAN_RESPONSE_AFTER_SUBMIT

def invoke_gcp(config, payload):
    log.info('Invoking GCP')
    gcp = GCPHandler(config, payload)
    log.info('Handler created')
    return gcp.trigger_dag()


def dag_status(context, data_dict):
    dag_name = request.params.get('dag_name')
    execution_date = request.params.get('execution_date', '')
    dag_status_report = DagStatusReport(dag_name, execution_date, config)
    if config.get('ckan.airflow.cloud','local') != "GCP":
        return dag_status_report.get_local_aircan_report()
    return dag_status_report.get_gcp_report()
