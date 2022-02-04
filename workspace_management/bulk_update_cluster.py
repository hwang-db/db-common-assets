import json
import subprocess
from copy import deepcopy


def get_all_clusters_id() -> dict:
    # retrieve a dict of [clusterid:clustername]
    clusters = dict()
    p = subprocess.Popen('databricks clusters list', shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        # clean returned objects
        output = list(filter(None, str(line, 'utf-8').split(" ")))
        cluster_id, cluster_name = output[0], output[1]
        # add into dict
        clusters[cluster_id] = cluster_name
    retval = p.wait()
    return clusters


def get_cluster_info(cluster_id: str) -> dict:
    # gets single cluster metadata
    p_res = subprocess.check_output(
        ["databricks", "clusters", "get", "--cluster-id", cluster_id])
    res_json = json.loads(p_res)
    return res_json


def modify_tag_json(cluster_original_json: dict, custom_tag: dict) -> dict:
    # given single cluster original metadata, apply and return custom_tag onto json obj
    modified_json = deepcopy(cluster_original_json)
    modified_json["custom_tags"] = custom_tag
    return modified_json


def patch_cluster(cluster_id: str, json_obj: dict) -> None:
    # update single cluster
    p_res = subprocess.check_output(
        ["databricks", "clusters", "edit", "--json", json.dumps(json_obj, indent=2).encode('utf-8')])
    return None


if __name__ == '__main__':
    all_clusters = get_all_clusters_id()
    # define your dict here for custom_tags
    custom_tags = {'costcenter': 'sales',
                   'randomtag1': 'val1',
                   'randomtag2': 'val2'}

    # update all clusters in 1 workspace example:
    for k, v in all_clusters.items():
        print(k, v)
        original_json = get_cluster_info(cluster_id=k)
        updated_json = modify_tag_json(original_json, custom_tags)
        patch_cluster(cluster_id=k, json_obj=updated_json)
        print("updated custom tag for %s..." % k)
