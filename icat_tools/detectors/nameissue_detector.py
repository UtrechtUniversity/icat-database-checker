from icat_tools import utils
from icat_tools.detectors.detector import Detector
import psycopg2


class NameIssueDetector(Detector):

    def get_name(self):
        return "names"

    def _get_name_check_data(self):
        data = {
            'collection': {
                'table': 'r_coll_main',
                'report_columns': ['coll_id', 'coll_name'],
                'name': 'coll_name'},
            'data object': {
                'table': 'r_data_main',
                'report_columns': ['data_id', 'data_name', 'coll_id'],
                'name': 'data_name'},
            'resource': {
                'table': 'r_resc_main',
                'report_columns': ['resc_id', 'resc_name'],
                'name': 'resc_name'},
            'user': {
                'table': 'r_user_main',
                'report_columns': ['user_id', 'user_name'],
                'name': 'user_name'},
            'zone': {
                'table': 'r_zone_main',
                'report_columns': ['zone_id', 'zone_name'],
                'name': 'zone_name'}}
        return data.items()

    def _check_name_empty(self, table, name, report_columns):
        query = "SELECT {} FROM {} WHERE {} = ''".format(
            ",".join(report_columns), table, name)
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def _check_name_buggy_characters(self, table, name, report_columns):
        query = r"SELECT {} FROM {} WHERE {} ~ '[\`\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f]'".format(
            ",".join(report_columns), table, name)
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def run(self):
        issue_found = False
        for check_name, check_params in self._get_name_check_data():
            if self.args.v:
                self.output_message("Check: names - " + check_name)

            result_empty = self._check_name_empty(
                check_params['table'],
                check_params['name'],
                check_params['report_columns'])
            for row in result_empty:
                output = {'type': 'empty_name', 'check_name' : check_name, 'report_columns': {}}
                column_num = 0
                for report_column in check_params['report_columns']:
                    output['report_columns'][str(report_column)] = str(
                        row[column_num])
                    column_num = column_num + 1
                self.output_item(output)
                issue_found = True

            result_buggy_characters = self._check_name_buggy_characters(
                check_params['table'],
                check_params['name'],
                check_params['report_columns'])
            for row in result_buggy_characters:
                output = {'type': 'buggy_characters', 'check_name' : check_name, 'report_columns': {}}
                column_num = 0
                for report_column in check_params['report_columns']:
                    if str(report_column) == 'coll_id':
                        coll_name = utils.get_collection_name(
                            self.connection, str(row[column_num]))
                        if coll_name is not None:
                            output['report_columns']['Collection name'] = coll_name
                    else:
                        output['report_columns'][str(report_column)] = str(
                            row[column_num])
                    column_num = column_num + 1
                self.output_item(output)
                issue_found = True

        return issue_found
