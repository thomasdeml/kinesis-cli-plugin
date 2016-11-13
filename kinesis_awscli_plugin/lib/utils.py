import signal
import os
import sys

from awscli.formatter import get_formatter


class Utils:
    @staticmethod
    def register_ctrl_c_handler():
        signal.signal(signal.SIGINT, Utils.ctrl_c_handler)

    @staticmethod
    def ctrl_c_handler(signum, frame):
        print("\nYou hit Ctrl+C.\nExiting ")
        exit()

    @staticmethod
    def example_text(module_path, example_file):
        module_path = os.path.dirname(os.path.abspath(module_path))
        example_file_path = os.path.join(module_path,
                                     'examples/kinesis/' + example_file)
        return open(example_file_path, 'r').read()

    @staticmethod
    def display_response(session, command_name, response, parsed_globals):
        output = parsed_globals.output
        if output is None:
            output = session.get_config_variable('output')
        formatter = get_formatter(output, parsed_globals)
        formatter(command_name, response)
