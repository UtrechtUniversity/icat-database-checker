class Detector(object):
    def __init__(self, args, connection, output_processor):
        self.args = args
        self.connection = connection
        self.output_processor = output_processor

    def output_item(self, values):
        self.output_processor.output_item(self.get_name(), values)

    def output_message(self, message):
        self.output_processor.output_message(message)

    def print_progress(self, message):
        self.output_processor.print_progress(message)

    def print_error(self, message):
        self.output_processor.print_error(message)

    def exit_error(self, message):
        self.output_processor.exit_error(message)

    def get_name(self):
        return "detector_superclass"
