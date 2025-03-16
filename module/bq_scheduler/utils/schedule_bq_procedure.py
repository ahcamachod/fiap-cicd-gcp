from datetime import datetime
from google.cloud import bigquery_datatransfer, bigquery_datatransfer_v1
from google.protobuf import field_mask_pb2
from google.protobuf.json_format import MessageToDict
from google.cloud import bigquery
from google.auth import default
import json
import os
import sys

project_id = default()[1]
env = project_id.split('-')[-1]
transfer_client = bigquery_datatransfer.DataTransferServiceClient()
print(sys.argv)
zone = sys.argv[4]
service_account_name=sys.argv[5]
root_dir = '/bq_scheduler/schedules/'+zone

parent = transfer_client.common_location_path(project_id , "EU")
pubsub_topic = f'projects/{project_id}/topics/{sys.argv[3]}'
print('Pubsub Topic: ',pubsub_topic)

#Dictionary to store existing scheduled query config
existing_schedules = {}

#Dictionary to store new scheduled query config
new_schedules = {}


def create_scheduled_query(schedule_config):
    """
    Function to create new scheduled query. 
    Args
    scheduled_config (dict): Dictionary having display name, query, schedule and the start_date
    """
    try:
        display_name = schedule_config["display_name"].replace('${ENV}',env)
        query = schedule_config["query"].replace('${ENV}',env)
        schedule = schedule_config["schedule"]
        start_date = schedule_config["start_date"]
    except Exception as e:
        print(f"Failed to retrieve schedule, Error: {e}")    
        return    


    transfer_config = bigquery_datatransfer.TransferConfig(
        display_name=display_name,
        data_source_id="scheduled_query",
        params={
            "query": query
        },
        schedule=schedule,
        schedule_options=bigquery_datatransfer_v1.types.ScheduleOptions(
            start_time=datetime.fromisoformat(start_date))            
        )

    try:
        transfer_config = transfer_client.create_transfer_config(
            bigquery_datatransfer.CreateTransferConfigRequest(
                parent=parent,
                transfer_config=transfer_config,
                service_account_name=service_account_name
            )
        )

        print("Created scheduled query '{}'".format(transfer_config.name))
        
    except Exception as e:
        print(f"Failed to schedule query, Error: {e}")
        raise e

    try:
        transfer_config_name=transfer_config.name
        transfer_config = bigquery_datatransfer.TransferConfig(name=transfer_config_name)
        print('Updating pubsub topic')
        transfer_config.notification_pubsub_topic = pubsub_topic
        update_mask = field_mask_pb2.FieldMask(paths=["notification_pubsub_topic"])

        transfer_config = transfer_client.update_transfer_config(
            {"transfer_config": transfer_config, "update_mask": update_mask}
        )

        print(f"Updated config: '{transfer_config.name}'")
        print(f"Notification Pub/Sub topic: '{transfer_config.notification_pubsub_topic}'")

    except Exception as e:
        print(f"Failed to update pubsub topic for the query, Error: {e}")

def update_scheduled_query(schedule_config, existing_schedule):
    """
    Function to update existing scheduled query. 
    Args
    scheduled_config (dict): Dictionary having display new query and the schedule
    existing_schedule (dict): Dictionary having existing display name, query and the schedule
    """
    try:
        display_name = schedule_config["display_name"].replace('${ENV}',env)
        query = schedule_config["query"].replace('${ENV}',env)
        schedule = schedule_config["schedule"]
        start_date = schedule_config["start_date"]
    except Exception as e:
        print(f"Failed to retrieve schedule, Error: {e}")
        return 
        
    try:
        existing_query_name = existing_schedule["name"]
        existing_query = existing_schedule["query"]
        existing_query_schedule = existing_schedule["schedule"]
        existing_query_start_date = existing_schedule["start_date"]
    except Exception as e:
        print(f"Failed to retrieve existing schedule for {display_name}, Error: {e}")
        return 

    if query == existing_query and schedule == existing_query_schedule:
        print(f"No changes found in {display_name}. Not updating the scheduled query")
        return 

    #Get the transfer config
    transfer_config = bigquery_datatransfer.TransferConfig(name=existing_query_name)
    transfer_config.notification_pubsub_topic = pubsub_topic
    

    transfer_config.schedule = schedule
    new_params={
            "query": query
        }
    transfer_config.params = new_params    

    try:
        #Update scheduled query
        transfer_config = transfer_client.update_transfer_config(
            {
                "transfer_config": transfer_config,
                "update_mask": field_mask_pb2.FieldMask(paths=["schedule","params","notification_pubsub_topic","service_account_name"]),
                "service_account_name": service_account_name,
            }
        )

        print(f"Updated scheduled query '{display_name}':{existing_query_name}")
        return 
        
    except Exception as e:
        print(f"Failed to schedule query {display_name}, Error: {e}")
        raise e
        return 


#Get existing list of scheduled queries
configs = transfer_client.list_transfer_configs(parent=parent)

for config in configs:
    existing_config = MessageToDict(config._pb)
    existing_displayName = existing_config['displayName']
    try:
        existing_schedules[existing_displayName] = {
            "name":existing_config['name'],
            "schedule":existing_config['schedule'] if 'schedule' in existing_config.keys() else 'NA',
            "query":existing_config['params']['query'] if 'query' in existing_config.keys() else 'NA',
            "start_date":existing_config['schedule_options'] if 'schedule_options' in existing_config.keys() else 'NA'
            }
    except Exception as e:
        print(f"Failed to extract existing config {existing_displayName}, Error: {e}")

#Get all schedule from josn files
for subdir, dirs, files in os.walk(root_dir):
    for file in files:
        if ".json" in file:
            document_name = file.replace('${ENV}', env)
            document_path = os.path.join(subdir, file)
            with open(document_path, 'r') as f:
                schedule_config = json.load(f)

            query_display_name = schedule_config["display_name"].replace('${ENV}',env)
            new_schedules[query_display_name] = schedule_config
            print(query_display_name)

            #Check if the query already exists
            if schedule_config["display_name"].replace('${ENV}',env) not in existing_schedules.keys():
                print('creating new schedule query')
                #Create new scheduled query
                create_scheduled_query(schedule_config)
            else:
                #Update existing scheduled query
                print('Updating existing schedule query')
                update_scheduled_query(schedule_config,existing_schedules[query_display_name])

#Delete queries removed that are removed from github
'''for schedule in existing_schedules.keys():
    if schedule not in new_schedules.keys():
        
        try:
            transfer_client.delete_transfer_config(name=existing_schedules[schedule]["name"])
            print(f"Deleted scheduled query - ID: {existing_schedules[schedule]["name"]}, Name: {schedule}, Schedule: {existing_schedules[schedule]}")
            
        except Exception as e:
            print(f"Failed to delete scheduled query: {e}")'''
