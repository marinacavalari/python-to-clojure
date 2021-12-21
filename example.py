import sys
import os
import subprocess
import json
from subprocess import call, check_output

EXPORT_PROFILE = "primary"
IMPORT_PROFILE = "secondary"

import requests
import json

headers = {"Authorization" : "Bearer your_token_here"}

endpoints = {'create': "https://your-new-workspace.cloud.databricks.com/api/2.0/clusters/create",
             'clusters':"https://your-old-workspace.cloud.databricks.com/api/2.0/clusters/list"}  

clusters_data = requests.get(url = endpoints['clusters'], headers=headers)
  
clusters_info_list = clusters_data.json()['clusters']


cluster_req_elems = ["num_workers",
                     "autoscale",
                     "cluster_name",
                     "spark_version",
                     "spark_conf",
                     "node_type_id",
                     "driver_node_type_id",
                     "custom_tags",
                     "cluster_log_conf",
                     "spark_env_vars",
                     "autotermination_minutes",
                     "enable_elastic_disk"]

i = 0
for cluster in clusters_info_list:
    cluster_req_json = clusters_info_list[i]
    cluster_json_keys = cluster_req_json.keys()
    
    if cluster_req_json['cluster_source'] == u'JOB' :
        continue
    payload = dict((key, clusters_info_list[i][key]) for key in cluster_req_elems if key in clusters_info_list[i])
    create = requests.post(url = endpoints['create'], headers=headers, data=payload)
    i += 1
        
