"""
Copyright 2018-2019 Splunk, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from lib.helper import get_test_folder
from kafka.admin import KafkaAdminClient, NewTopic
import logging.config
import requests
import os
import json
import time
import jsonpath

logging.config.fileConfig(os.path.join(get_test_folder(), "logging.conf"))
logger = logging.getLogger("kafka")


def create_kafka_connector(setup, params, success=True):
    '''
    Create kafka connect connector using kafka connect REST API
    '''
    response = requests.post(url=setup["kafka_connect_url"] + "/connectors", data=json.dumps(params),
                  headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
    if not success:
        logger.info(response.content)
        return response.status_code == 201

    if status := get_kafka_connector_status(
        setup, params, action='Create', state='RUNNING'
    ):
        logger.debug(f"Created connector successfully - {json.dumps(params)}")
        return True
    else:
        return False


def update_kafka_connector(setup, params, success=True):
    '''
    Update kafka connect connector using kafka connect REST API
    '''
    response = requests.put(url=setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/config",
                            data=json.dumps(params["config"]),
                            headers={'Accept': 'application/json', 'Content-Type': 'application/json'})

    if not success:
        return response.status_code == 200

    if status := get_kafka_connector_status(
        setup, params, action='Update', state='RUNNING'
    ):
        logger.info(f"Updated connector successfully - {json.dumps(params)}")
        return True
    else:
        return False


def delete_kafka_connector(setup, connector):
    '''
    Delete kafka connect connector using kafka connect REST API
    '''
    if not isinstance(connector, str):
        connector = connector['name']
    response = requests.delete(url=setup["kafka_connect_url"] + "/connectors/" + connector,
                               headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
    if response.status_code == 204:
        logger.debug(f"Deleted connector successfully - {connector}")
        return True

    logger.error(f"Failed to delete connector: {connector}, response code - {response.status_code}")
    return False


def get_kafka_connector_tasks(setup, params, sleepDuration=0):
    '''
    Get kafka connect connector tasks using kafka connect REST API
    '''
    time.sleep(sleepDuration)
    t_end = time.time() + 10
    while time.time() < t_end:
        response = requests.get(url=setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/tasks",
                                headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
        status = response.status_code
        if status == 200:
            return len(response.json())

    return 0

def get_kafka_connector_status(setup, params, action, state):
    '''
    Get kafka connect connector tasks using kafka connect REST API
    '''
    t_end = time.time() + 10
    while time.time() < t_end:
        response = requests.get(url=setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/status",
                                headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
        content = response.json()
        if content.get('connector'):
            if content['connector']['state'] == state:
                return True

    logger.error(f"Failed to {action} connector and tasks are not in a {state} state after 10 seconds")
    return False


def get_running_kafka_connector_task_status(setup, params):
    '''
    Get running kafka connect connector tasks status using kafka connect REST API
    '''
    t_end = time.time() + 60
    url = setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/status"
    header = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    while time.time() < t_end:
        content = requests.get(url=url, headers=header).json()
        if content.get('connector'):
            if content['connector']['state'] == 'RUNNING' and len(content['tasks']) > 0:
                time.sleep(2)
                content = requests.get(url=url, headers=header).json()
                logger.info(content)
                return jsonpath.jsonpath(content, '$.tasks.*.state')



def pause_kafka_connector(setup, params, success=True):
    '''
    Pause kafka connect connector using kafka connect REST API
    '''
    response = requests.put(url=setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/pause",
                            headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
    if not success:
        return response.status_code != 202

    if status := get_kafka_connector_status(
        setup, params, action='Pause', state='PAUSED'
    ):
        logger.info(f"Paused connector successfully - {json.dumps(params)}")
        return True
    else:
        return False


def resume_kafka_connector(setup, params, success=True):
    '''
    Resume kafka connect connector using kafka connect REST API
    '''
    response = requests.put(url=setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/resume",
                            headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
    if not success:
        return response.status_code == 202

    if status := get_kafka_connector_status(
        setup, params, action='Resume', state='RUNNING'
    ):
        logger.info(f"Resumed connector successfully - {json.dumps(params)}")
        return True
    else:
        return False


def restart_kafka_connector(setup, params, success=True):
    '''
    Restart kafka connect connector using kafka connect REST API
    '''
    response = requests.post(url=setup["kafka_connect_url"] + "/connectors/" + params["name"] + "/restart",
                             headers={'Accept': 'application/json', 'Content-Type': 'application/json'})

    if not success:
        return response.status_code in {202, 204}

    if status := get_kafka_connector_status(
        setup, params, action='Restart', state='RUNNING'
    ):
        logger.info(f"Restarted connector successfully - {json.dumps(params)}")
        return True
    else:
        return False


def get_running_connector_list(setup):
    # Get the list of running connectors
    content = requests.get(url=setup["kafka_connect_url"] + "/connectors",
                           headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
    content_text = content.text[1:-1].replace('\"', '')
    return [] if not content_text else content_text.split(',')


def create_kafka_topics(config, topics):
    client = KafkaAdminClient(bootstrap_servers=config["kafka_broker_url"], client_id='test')
    broker_topics = client.list_topics()
    topic_list = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in topics
        if topic not in broker_topics
    ]
    client.create_topics(new_topics=topic_list, validate_only=False)
