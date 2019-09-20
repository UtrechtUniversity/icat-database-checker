from icat_tools import utils
from icat_tools.detectors.detector import Detector
import psycopg2


class HardlinkDetector(Detector):
    def run(self):
        issue_found = False
        resource_name_lookup = utils.get_resource_name_dict(self.connection)

        for resc_id, resc_path in utils.get_resource_vault_path_dict(
                self.connection).items():

            query = "SELECT data_id, data_path FROM r_data_main WHERE resc_id = {}".format(
                resc_id)
            lookup_path = {}
            cursor = self.connection.cursor()
            cursor.execute(query)

            for row in cursor.fetchall():
                if row[1] in lookup_path:
                    issue_found = True
                    this_object = utils.get_dataobject_name(
                        self.connection, row[0])
                    other_object = utils.get_dataobject_name(
                        self.connection, lookup_path[row[1]])
                    if this_object == other_object:
                        print(
                            "Duplicate dataobject entry found for data object {}\n  Resource: {}\n   Path: {}".format(
                                this_object, resource_name_lookup[resc_id], row[1]))
                    else:
                        print(
                            "Hard link found for path {} on resource {}:\n  Data object 1: {}\n  Data object 2: {}\n".format(
                                row[1], resource_name_lookup[resc_id], this_object, other_object))
                else:
                    lookup_path[row[1]] = row[0]

        return issue_found
