from argparse import ArgumentParser
from enum import Enum
from icat_tools import utils
from icat_tools.detectors.hardlink_detector import HardlinkDetector
from icat_tools.detectors.pathinconsistency_detector import PathInconsistencyDetector
from icat_tools.dbcheck_outputprocessors import OutputItemCollector
import json


def get_arguments():
    desc = 'Generates a script to repair data objects with a replica on a consumer with an inconsistent path, except when the data object has a hard link.'
    parser = ArgumentParser(description=desc)
    parser.add_argument(
        '--config-file',
        help='Location of the irods server_config file (default: etc/irods/server_config.json )',
        default='/etc/irods/server_config.json')
    args = parser.parse_args()
    return args


def get_provider(config_filename):
    with open(config_filename) as configfile:
        data = json.load(configfile)
    return data['icat_host']


def get_list_hardlinks(connection):
    collector = OutputItemCollector()
    HardlinkDetector({}, connection, collector).run()
    return [item.get('data_id') for item in collector.get_items()
            if item.get('type') == 'hardlink']


def get_resources_dataobjects_inconsistent_path_on_consumer(
        connection, provider_hostname):
    result = {}
    resource_host_lookup = utils.get_resource_host_dict(connection)
    collector = OutputItemCollector()
    PathInconsistencyDetector({}, connection, collector).run()
    return [item for item in collector.get_items(
    ) if resource_host_lookup[item.get('resource_id')] != provider_hostname]


def main():
    args = get_arguments()
    config = utils.read_database_config(args.config_file)
    connection = utils.get_connection_database(config)

    provider_hostname = get_provider(args.config_file)
    hard_links = get_list_hardlinks(connection)
    dict_inconsistent_paths = get_resources_dataobjects_inconsistent_path_on_consumer(
        connection, provider_hostname)

    print("#!/bin/sh")
    print("# It is recommended to verify that the data objects are correctly stored on the provider before removing")
    print("# them on the consumer, for example using the irods consistency checker.")

    for object in dict_inconsistent_paths:
        data_id = object.get('data_id')
        data_name = utils.get_dataobject_name(connection, data_id)
        if data_id in hard_links:
            print(
                "# Not repairing data object {}, because it has a hard link.".format(data_name))
        else:
            print(
                'itrim -M -S {} -N 1 "{}"'.format(object.get('resource_name'), data_name))
            print('irepl -M -R irodsRescRepl "{}"'.format(data_name))
            print('ils -L "{}"'.format(data_name))
            print('echo')


if __name__ == '__main__':
    main()
