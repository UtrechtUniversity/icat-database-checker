from icat_tools import utils
from icat_tools.detectors.detector import Detector
import pathlib


class PathInconsistencyDetector(Detector):
    def get_name(self):
        return "path_consistency"

    def run(self):
        issue_found = False
        resource_path_lookup = utils.get_resource_vault_path_dict(
            self.connection)
        resource_name_lookup = utils.get_resource_name_dict(self.connection)
        coll_path_lookup = utils.get_coll_path_dict(self.connection)

        if self.args.data_object_prefix is None:
            query_condition = "WHERE r_resc_main.resc_type_name in ('unixfilesystem', 'unix file system')"
        else:
            query_condition = "WHERE concat ( ( select coll_name from r_coll_main where coll_id = r_data_main.coll_id ), '/', r_data_main.data_name) LIKE '{}%' AND r_resc_main.resc_type_name in ('unixfilesystem', 'unix file system')".format( self.args.data_object_prefix)

        query = ( "SELECT r_data_main.data_name, r_data_main.coll_id, r_data_main.resc_id, r_data_main.data_path " +
                  "FROM r_data_main INNER JOIN r_resc_main ON r_resc_main.resc_id = r_data_main.resc_id " +
                  query_condition )
        cursor = self.connection.cursor(self.get_name())
        cursor.execute(query)

        for row in cursor:
            vaultpath = pathlib.Path(resource_path_lookup[row[2]])
            dirname = pathlib.Path(*pathlib.Path(row[3]).parts[:-1])
            try:
                dirname_without_vault = dirname.relative_to(vaultpath)
            #If the dirname doesn't start with the vault path, there's either a path inconsistency or a file is on the wrong resource. Either way, this is an inconsistency so report it
            except ValueError:
                self.output_item({
                    'resource_name': resource_name_lookup[row[2]],
                    'phy_path': row[3],
                    'data_name': "{}/{}".format(coll_path_lookup[row[1]],row[0])})
                issue_found = True
                continue
            collname = coll_path_lookup[row[1]]
            collname_parts = pathlib.Path(collname).parts
            collname_parts_without_zone = list(collname_parts[2:])
            collname_without_zone = pathlib.Path(*collname_parts_without_zone)
            if collname_without_zone != dirname_without_vault:
                self.output_item({
                    'resource_name': resource_name_lookup[row[2]],
                    'phy_path': row[3],
                    'data_name': "{}/{}".format(collname,row[0])})
                issue_found = True

        cursor.close()
        return issue_found
