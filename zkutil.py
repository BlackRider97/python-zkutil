from kazoo.client import KazooClient
import logging
logging.basicConfig()

from yaml import load, dump

import sys

NODE_KEY = "key"
NODE_VALUE = "value"
NODE_CHILDREN = "children"

def get_zk_data(utf8_formated_data):
    return str(utf8_formated_data.decode("utf-8"))

def get_correct_list(utf8_formated_list):
    result = set()
    if utf8_formated_list:
        for member in utf8_formated_list:
            result.add(get_zk_data(member))
    return list(result)

def does_node_exist(zk, node_path):
    return zk.exists(node_path)

def get_value_of_node(zk, node_path):
    return get_zk_data(zk.get(node_path)[0])

def get_children_of_node(zk, node_path):
    return get_correct_list(zk.get_children(node_path))

def get_zookeeper_data(zk, node_path):
    node_exists = does_node_exist(zk, node_path)
    if node_exists:
        #add validation here
        node_value = get_value_of_node(zk, node_path)
        node_children = get_children_of_node(zk, node_path)
        result = dict()
        result[NODE_KEY],result[NODE_VALUE],result[NODE_CHILDREN]  =str(node_path), node_value, node_children
        return result
    return None

export_data_list = []
def export_data_util(src_zk, node_path):
    print "reading node of path %s" %(node_path)
    root_data = get_zookeeper_data(src_zk, node_path)
    if not root_data:
        logging.error("No data found from zookeeper")
        return

    #export_data_list.append(root_data)
    node = dict()
    node[node_path] = root_data[NODE_VALUE]
    export_data_list.append(node)

    root_children =  root_data.get( NODE_CHILDREN, None)
    if root_children:
        for child in root_children:
            if node_path == '/':
                export_data_util(src_zk, "/"+child)
            else:
                export_data_util(src_zk, node_path+"/"+child)

""" read data of each node by DFS algorithm """
def export_data(src_zk, output_yaml_file):
    export_data_util(src_zk, "/")
    output_stream = open(output_yaml_file,"w")
    dump(export_data_list, output_stream, default_flow_style=False)
    output_stream.close()
    print "\n\n********** Data successfully exported see %s ***********\n" %(output_yaml_file)
    print "Total number of node created=",len(export_data_list)
    print "\n***********************************************************\n"


def import_data(dist_zk, input_yaml_file):
    input_stream = open(input_yaml_file, 'r')
    dataMap = load(input_stream)
    total_node_skipped, total_node_created = 0, 0
    for member in dataMap:
        for node_path,node_value in member.items():
            if does_node_exist(dist_zk, node_path):
                print "node already exists at path: %s with value: %s" % (node_path, get_value_of_node(dist_zk, node_path))
                total_node_skipped += 1
                continue
            # Ensure a path, create if necessary
            try:
                one_node_back_path = node_path[:node_path.rfind("/")]
                if one_node_back_path == "/":
                    continue
                dist_zk.ensure_path()
            except Exception as e:
                pass
            print "creating node at path: %s value: %s" % (node_path, node_value)
            # Create a node with data
            dist_zk.create(node_path, node_value)
            total_node_created += 1

    print "\n\n********** Data successfully imported to given zk-path ***********\n"
    print "Total number of node created=",total_node_created
    print "Total number of node skipped=",total_node_skipped
    print "\n***********************************************************\n"

def print_usages():
    print 'usage <export/import> <source/distination-zk-path>  <output/input-yaml-file-path>'
    print "samples:"
    print "python zkutil.py export 10.0.1.87:2181 zk-backup.yml"
    print "python zkutil.py import 10.0.1.27:2181 zk-backup.yml\n"

if __name__ == '__main__':
    if len(sys.argv) < 4:
       print_usages()
       sys.exit()

    command = str(sys.argv[1]).strip()
    if command == "export":
        src_zk_hosts = str(sys.argv[2]).strip()
        src_zk = KazooClient(hosts=src_zk_hosts, read_only=True)
        src_zk.start()
        export_data(src_zk, str(sys.argv[3]).strip())
    elif command == "import":
        dist_zk_hosts = str(sys.argv[2]).strip()
        dist_zk = KazooClient(hosts=dist_zk_hosts)
        dist_zk.start()
        import_data(dist_zk, str(sys.argv[3]).strip())
    else:
        print_usages()
