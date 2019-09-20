from icat_tools import utils
from icat_tools.detectors.detector import Detector
import psycopg2


class MinreplicaIssueDetector(Detector):
    def run(self):
        issue_found = False
        resource_name_lookup = utils.get_resource_name_dict(self.connection)
        query = "SELECT data_id, resc_id FROM r_data_main"
        cursor = self.connection.cursor()
        cursor.execute(query)
        data_resc_lookup = {}

        for row in cursor.fetchall():
            if row[0] in data_resc_lookup:
                if row[1] not in data_resc_lookup[row[0]]:
                    data_resc_lookup[row[0]][row[1]] = ""
            else:
                data_resc_lookup[row[0]] = {row[1]: ""}

        for data_id, resc_dict in data_resc_lookup.items():
            number_replicas = len(resc_dict.keys())
            if number_replicas < self.args.min_replicas:
                issue_found = True
                object_name = utils.get_dataobject_name(
                    self.connection, data_id)
                print("Number of replicas for data object {} is {} (less than {})".format(
                    object_name, number_replicas, self.args.min_replicas))

        return issue_found
