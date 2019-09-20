import sys


class CheckOutputProcessorHuman:
    def _print_report_column_table(self, dict):
        for column, value in dict.items():
            print("  {} : {}".format(column, value))

    def output_message(self, message):
        print(message)

    def output_item(self, check, values):

        if check == 'hardlinks':
            if values['type'] == 'duplicate_dataobject_entry':
                print(
                    "Duplicate dataobject entry found for data object {}\n  Resource: {}\n   Path: {}".format(
                        values['object_name'],
                        values['resource_name'],
                        values['phy_path']))
            elif values['type'] == 'hardlink':
                print(
                    "Hard link found for path {} on resource {}:\n  Data object 1: {}\n  Data object 2: {}\n".format(
                        values['phy_path'],
                        values['resource_name'],
                        values['object1'],
                        values['object2']))
            else:
                print(
                    "Error: unknown output item type for hardlink check: {}".format(
                        values['type']))
                sys.exit(1)

        elif check == 'minreplicas':
            print("Number of replicas for data object {} is {} (less than {})".format(
                values['object_name'],
                values['number_replicas'],
                values['min_replicas']))

        elif check == 'names':
            if values['type'] == 'empty_name':
                print("Empty name for " + values['check_name'])
                _print_report_column_table(values['report_columns'])
            elif values['type'] == 'buggy_characters':
                print(
                    "Name with characters that iRODS processes incorrectly for " +
                    values['check_name'])
                self._print_report_column_table(values['report_columns'])
            else:
                print(
                    "Error: unknown output item type for names check: {}".format(
                        values['type']))
                sys.exit(1)

        elif check == 'path_consistency':
            print(
                "Inconsistent directory name in resource {} for {} :\n  collection name : {}\n  directory name in vault : {}".format(
                    values['resource_name'],
                    values['phy_path'],
                    values['coll_name'],
                    values['dir_name']))

        elif check == 'ref_integrity':
            print(
                "Potential referential integrity issue found for {}.".format(values['check_name']))
            self._print_report_column_table(values['report_columns'])

        elif check == 'timestamps':
            if values['type'] == 'order':
                print(
                    "Timestamps in unexpected order for " +
                    values['check_name'])
                self._print_report_column_table(values['report_columns'])
            elif values['type'] == 'future':
                print("Timestamp from the future for " + values['check_name'])
                self._print_report_column_table(values['report_columns'])
            else:
                print(
                    "Error: unknown output item type for timetamps check: {}".format(
                        values['type']))
                sys.exit(1)

        else:
            print("Error: unknown output check type: {}".format(check))
            sys.exit(1)
