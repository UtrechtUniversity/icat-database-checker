from icat_tools import utils
from icat_tools.detectors.detector import Detector


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

    def _get_prefix_condition(self,table):
        if table == 'r_data_main' and self.args.data_object_prefix is not None:
            return "AND concat ( ( select coll_name from r_coll_main where coll_id = r_data_main.coll_id ), '/', r_data_main.data_name) LIKE '{}%'".format(self.args.data_object_prefix)
        else:
            return ""

    def _check_name_empty(self, table, name, report_columns):
        query = "SELECT {} FROM {} WHERE {} = '' {}".format(
            ",".join(report_columns), table, name, self._get_prefix_condition(table))
        cursor = self.connection.cursor("{}._check_name_empty".format(self.get_name()))
        cursor.execute(query)
        return cursor

    def _check_name_buggy_characters(self, table, name, report_columns):
        query = r"SELECT {} FROM {} WHERE {} ~ '[\`\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f]' {}".format( ",".join(report_columns), table, name, self._get_prefix_condition(table))
        cursor = self.connection.cursor("{}._check_buggy_characters".format(self.get_name()))
        cursor.execute(query)
        return cursor

    def _check_name_trailing_slash(self, table, name, report_columns):
        query = "SELECT {} FROM {} WHERE {} != '/' AND {} LIKE '%/' {}".format( ",".join(report_columns), table, name, name, self._get_prefix_condition(table))
        cursor = self.connection.cursor("{}._check_name_trailing_slash".format(self.get_name()))
        cursor.execute(query)
        return cursor

    def run(self):
        issue_found = False

        def _do_output(type_name, report_columns, query_result):
            """Internal function for translating query results of a check to a generic
               output dictionary and feeding it to the output processor. Also translates
               collection IDs to collection names for readability."""
            nonlocal issue_found

            for row in query_result:
                output = {'type': type_name, 'check_name' : check_name, 'report_columns': {}}
                column_num = 0
                for report_column in report_columns:
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

            query_result.close()
            return issue_found

        for check_name, check_params in self._get_name_check_data():
            if self.args.v:
                self.print_progress("Running empty name test for: " + check_name)

            result_empty = self._check_name_empty(
                check_params['table'],
                check_params['name'],
                check_params['report_columns'])
            _do_output("empty_name", check_params['report_columns'], result_empty)


            if check_name in ["data object", "collection"]:
                if self.args.v:
                    self.print_progress("Running trailing slash test for: " + check_name)

                result_trailing_slash = self._check_name_trailing_slash(
                    check_params['table'],
                    check_params['name'],
                    check_params['report_columns'])
                _do_output("trailing_slash", check_params['report_columns'], result_trailing_slash)

            if self.args.v:
                self.print_progress("Running problematic character name test for: " + check_name)

            result_buggy_characters = self._check_name_buggy_characters(
                check_params['table'],
                check_params['name'],
                check_params['report_columns'])
            _do_output("buggy_characters", check_params['report_columns'], result_buggy_characters)

        return issue_found
