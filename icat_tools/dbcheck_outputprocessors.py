import csv
import operator
import sys


class OutputProcessor:
    def output_message(self, message):
        pass

    def output_item(self, check, values):
        pass


class CheckOutputProcessorHuman(OutputProcessor):
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
                "Potential referential integrity issue found for {}.".format(
                    values['check_name']))
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


class CheckOutputProcessorCSV(OutputProcessor):

    def __init__(self):
        self.writer = csv.writer(
            sys.stdout,
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL)

    def _column_value_to_list(self, dict):
        result = []
        for column, value in sorted(dict.items(), key=operator.itemgetter(0)):
            result.append(column)
            result.append(value)
        return result

    def output_item(self, check, values):

        if check == 'hardlinks':
            if values['type'] == 'duplicate_dataobject_entry':
                self.writer.writerow([check,
                                      'duplicate_dataobject',
                                      values['phy_path'],
                                      values['resource_name'],
                                      values['object_name']])
            elif values['type'] == 'hardlink':
                self.writer.writerow([check,
                                      'hardlink',
                                      values['phy_path'],
                                      values['resource_name'],
                                      values['object1'],
                                      values['object2']])
            else:
                print(
                    "Error: unknown output item type for hardlink check: {}".format(
                        values['type']))
                sys.exit(1)

        elif check == 'minreplicas':
            self.writer.writerow([check,
                                  values['object_name'],
                                  values['number_replicas'],
                                  values['min_replicas']])

        elif check == 'names':
            if values['type'] == 'empty_name' or values['type'] == 'buggy_characters':
                self.writer.writerow(
                    [
                        check,
                        values['type'],
                        values['check_name']] +
                    self._column_value_to_list(
                        values['report_columns']))
            else:
                print(
                    "Error: unknown output item type for names check: {}".format(
                        values['type']))
                sys.exit(1)

        elif check == 'path_consistency':
            self.writer.writerow([check,
                                  values['resource_name'],
                                  values['phy_path'],
                                  values['coll_name'],
                                  values['dir_name']])

        elif check == 'ref_integrity':
            self.writer.writerow([check, values['check_name']] +
                                 self._column_value_to_list(values['report_columns']))

        elif check == 'timestamps':
            if values['type'] == 'order' or values['type'] == 'future':
                self.writer.writerow(
                    [
                        check,
                        values['type'],
                        values['check_name']] +
                    self._column_value_to_list(
                        values['report_columns']))
            else:
                print(
                    "Error: unknown output item type for timetamps check: {}".format(
                        values['type']))
                sys.exit(1)

        else:
            print("Error: unknown output check type: {}".format(check))
            sys.exit(1)
