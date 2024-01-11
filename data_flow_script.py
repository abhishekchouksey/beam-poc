###First EXAMPLE - Just simplest pipeline
# '''
# TO RUN in Docker
# first build
#     docker build -t beam-poc .
# To run
#     Docker run -v $(pwd):/usr/src/app beam-poc
# '''
# import apache_beam as beam


# def transformation(text):
#     return text.upper()


# with beam.Pipeline() as pipeline:
#     (
#         pipeline
#         | 'ReadInputText' >> beam.io.ReadFromText('./example_1/input.txt')
#         | 'ApplyTransformation' >> beam.Map(transformation)
#         | 'WriteOutPutText' >> beam.io.WriteToText('./example_1/output.txt')
#     )


## SECOND EXAMPLE  --  Just simplest pipeline with injected script
# #

# '''
# TO RUN in Docker
# first build
#     docker build -t beam-poc .
# To run
#     Docker run -v $(pwd):/usr/src/app beam-poc
# '''

# import apache_beam as beam
# import importlib.util


# def load_external_transformation(script_path):
#     import_module = importlib.util.spec_from_file_location('module_name', script_path)
#     import_spec = importlib.util.module_from_spec(import_module)

#     import_module.loader.exec_module(import_spec)

#     return import_spec.external_transformation


# external_transformation = load_external_transformation('./example_2/transformation_script.py')

# with beam.Pipeline() as pipeline:
#     (
#         pipeline
#         | 'ReadInputText' >> beam.io.ReadFromText('./example_2/input.txt')
#         | 'ApplyTransformation' >> beam.Map(external_transformation)
#         | 'WriteOutPutText' >> beam.io.WriteToText('./example_2/output.txt')
#     )


## THIRD EXAMPLE with more complex idea

# '''
# TO RUN in Docker
# first build
#     docker build -t beam-poc .
# To run

# Docker run -v $(pwd):/usr/src/app beam-poc --input=./example_3/input.json --output=./example_3/output.json --script_path=./example_3/external_transformation.py --missing_field=age --default_value=1000
# '''

# import apache_beam as beam
# import importlib.util
# import json
# import argparse
# import ast


# class DynamicTransformationFn(beam.DoFn):
#     def __init__(self, script_path, missing_field, default_value, *unused_args, **unused_kwargs):
#         self.script_path = script_path
#         self.missing_field = missing_field
#         self.default_value = default_value
#         super().__init__(*unused_args, **unused_kwargs)

#     def start_bundle(self):
#         import_module = importlib.util.spec_from_file_location("external_module_name", self.script_path)
#         external_module_name = importlib.util.module_from_spec(import_module)
#         import_module.loader.exec_module(external_module_name)
#         self.processFn = external_module_name.process_json

#     def process(self, element):
#         try:
#             # return element
#             # parsed_json = ast.literal_eval(element)
#             json_obj = json.loads(element)
#             transformation_json = self.processFn(json_obj, self.missing_field, self.default_value)

#             yield json.dumps(transformation_json)
#         except json.JSONDecodeError as e:
#             print(str(e))
#         except Exception as e:
#             print(e)


# def run(argv=None):
#     arg_parser = argparse.ArgumentParser()

#     # arg_parser.add_argument('--script_path', dest='script_path', required=True)
#     # # arg_parser.add_argument('--input', dest='input', required=True)
#     # # arg_parser.add_argument('--output', dest='output', required=True)
#     # # arg_parser.add_argument('--missing_field', dest='missing_field', required=True)
#     # # arg_parser.add_argument('--default_value', dest='default_value', required=True)
#     # known_args, pipeline_args = arg_parser.parse_known_args(argv)

#     with beam.Pipeline() as pipeline:
#         (
#             pipeline
#             | 'ReadInputText' >> beam.io.ReadFromText('./example_3/input.json')
#             | 'DynamicTransform'
#             >> beam.ParDo(DynamicTransformationFn('./example_3/external_transformation.py', 'age', '300'))
#             | 'WriteOutputText' >> beam.io.WriteToText('./example_3/output.txt')
#         )


# if __name__ == '__main__':
#     run()

## EXAMPLE 4 - COMPLEX ONE - IT WILL READ
# from BQ using supplied SQL

import apache_beam as beam
import argparse
import json
import importlib.util


def prep_payload(json_obj):
    pass


def read_sql_query(file_path):
    pass


class ReadfromBigQuery(beam.PTransform):
    pass


class DynamicTransformation(beam.DoFn):
    def __init__(self, script, *unused_args, **unused_kwargs):
        self.script = script
        self.script_module = None
        super().__init__(*unused_args, **unused_kwargs)

    def setup(self):
        script_spec = importlib.util.spec_from_file_location('external_module', self.script)
        self.script_module = importlib.util.module_from_spec(script_spec)
        script_spec.loader.exec_module(self.script_module)

    def process(self, element, *args, **kwargs):
        json_payload = json.loads(element['payload'])
        metadata = element['metadata']

        ## WE CAN READ AS MANY AS COLUMNS

        executable_func = metadata.get('executable_func_name')

        if hasattr(self.script_module, executable_func):
            transform_func = getattr(self.script_module, executable_func)
            yield transform_func(json_payload)

        else:
            yield f'function name {executable_func} does not exist'


def run(argv=None):
    arg_parser = argparse.ArgumentParser()
    # arg_parser.add_argument('--script_path', dest='script_path', required=True)
    # arg_parser.add_argument('--input', dest='input', required=True)
    # arg_parser.add_argument('--output', dest='output', required=True)
    # arg_parser.add_argument('--missing_field', dest='missing_field', required=True)
    # arg_parser.add_argument('--default_value', dest='default_value', required=True)
    known_args, pipeline_args = arg_parser.parse_known_args(argv)

    sql_query = '/example_4/sample_sql.sql'  # it will be an input
    output_topic = 'RAW-TOPIC'
    sql_script = './example_4/external_transformation.py'

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'ReadFromBigQuery' >> ReadfromBigQuery(read_sql_query(sql_query))
            | 'DynamicTransform' >> beam.ParDo(DynamicTransformation(sql_script))
            | 'extra_prep' >> beam.Map(prep_payload)
            | 'dotheMagic' >> beam.Map(name_of_function)
            | 'serializeToJson' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | 'WriteOutputText' >> beam.io.WriteToPubSub(output_topic)
        )


if __name__ == '__main__':
    run()
