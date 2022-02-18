import json
import subprocess

# makes use of jq to parse databricks cli output and save into local json files, then return list outputs of valid values for api
# databricks clusters spark-versions | jq . > file.json
# databricks clusters list-node-types | jq . > file.json


def get_all_spark_versions(jsonfilename) -> list:
    p = subprocess.Popen(f'databricks clusters spark-versions | jq . > {jsonfilename}.json', shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    with open(f'./{jsonfilename}.json', 'r') as f:
        data = json.load(f)
        spark_versions_list = [x["key"] for x in data["versions"]]
    return spark_versions_list


def get_all_node_types(jsonfilename) -> list:
    p = subprocess.Popen(f'databricks clusters list-node-types | jq . > {jsonfilename}.json', shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    with open(f'./{jsonfilename}.json', 'r') as f:
        data = json.load(f)
        node_type_id_list = [x["node_type_id"] for x in data["node_types"]]
    return node_type_id_list


if __name__ == '__main__':
    sparkversion_list = get_all_spark_versions("sparkversions")
    nodetype_list = get_all_node_types("nodetypes")
    print("sparkversion list:\n"+str(sparkversion_list))
    print("nodetype list:\n"+str(nodetype_list))
