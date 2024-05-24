import requests
import json

# you may need to install the python library "requests" if it is not already installed
# you can do this from the command line: pip install requests

#replace with your Confluent Cloud api key and secret
api_key = '<api_key>'
api_secret = '<api_secret>'

# set debug to 1 to see debug messages
debug = 0

# replace with your Ccloud cluster endpoint
base_url = '<rest_api_endpoint>'
if debug ==1: 
    print(f'Cloud Rest base URL: {base_url}')

#basic auth
auth = (api_key, api_secret)

#cluster id
cluster_id = '<cluster_id>'

#function to get the number of partitions for a topic
def get_partitions_count(cluster_id, auth):
    
    url = f'{base_url}/kafka/v3/clusters/{cluster_id}/topics'
    headers = { 
        'Content-Type': 'application/json',
               }
    
    if debug == 1:
        print(f'Url: {url} auth: {auth}, headers: {headers}')
    response = requests.get(url, auth=auth, headers=headers)

    if debug ==1:
        print(f'Response: {response}')
    if response.status_code != 200:
        print(f'Error: {response.text}')
        return None
   
    topics = response.json()['data']

    total_partitions = 0

    for topic in topics:
        topic_name = topic['topic_name']
        if debug == 1:        
            print(f'Topic Name: {topic_name}')
        topic_url = f'{url}/{topic_name}/partitions'
        if debug == 1:
            print(f'topic Url: {topic_url} auth: {auth}, headers: {headers}')
        partitions_response = requests.get(topic_url, auth=auth, headers=headers)
        if debug == 1:
            print(f'Partitions Response: {partitions_response}')
        if partitions_response.status_code != 200:
            print(f'Partitions response Error: {partitions_response.text}')
            return None
        
        partitions = partitions_response.json()['data']
        if debug == 1:
            print(f'Partition: {partitions}')
        total_partitions += len(partitions)
        
    return total_partitions

try:
    total_partitions = get_partitions_count(cluster_id, auth)
    print(f'Cluster: {cluster_id} Total partitions: {total_partitions}')

except Exception as e:
    print(f'Error: {e}')
 
