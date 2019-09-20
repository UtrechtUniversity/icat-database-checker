from icat_tools import utils
from icat_tools.detectors.detector import Detector
import pathlib
import psycopg2


class PathInconsistencyDetector(Detector):
    def run(self):
        issue_found = False
        resource_path_lookup = utils.get_resource_vault_path_dict(
            self.connection)
        resource_name_lookup = utils.get_resource_name_dict(self.connection)
        coll_path_lookup = utils.get_coll_path_dict(self.connection)

        query = "SELECT data_id, coll_id, resc_id, data_path FROM r_data_main"
        cursor = self.connection.cursor()
        cursor.execute(query)

        for row in cursor.fetchall():
            vaultpath = pathlib.Path(resource_path_lookup[row[2]])
            dirname = pathlib.Path(*pathlib.Path(row[3]).parts[:-1])
            dirname_without_vault = dirname.relative_to(vaultpath)
            collname_parts = pathlib.Path(coll_path_lookup[row[1]]).parts
            collname_parts_without_zone = list(collname_parts[2:])
            collname_without_zone = pathlib.Path(*collname_parts_without_zone)
            if collname_without_zone != dirname_without_vault:
                print("Inconsistent directory name in resource {} for {} :\n  collection = {}\n  directory name in vault = {}".format(
                    resource_name_lookup[row[2]], row[3], collname_without_zone, dirname_without_vault))
                issue_found = True

        return issue_found
