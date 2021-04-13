from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "gcp-expert-sandbox-allen-c1fcfd19238a.json"

class DataIngestion:

    def parse_method(self, string_input):
        values = re.sub('\r\n', '', string_input)

        row = json.loads(values)

        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.')

    # This defaults to the lake dataset in your BigQuery project. You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output BQ table to write results to.')


    parser.add_argument('--temp_bucket',
                        dest='temp_bucket',
                        required=True,
                        help='temp bucket name.')
    

    parser.add_argument('--credential',
                        dest='credential',
                        required=True,
                        help='credential json key.')


    parser.add_argument('--schema',
                        dest='schema_string',
                        required=True,
                        help='data schema json format.')


    parser.add_argument('--skip_json_lines',
                        dest='skip_json_lines',
                        type=int,
                        required=False,
                        help='skip csv lines.',
                        default=0)

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args += ["--runner=DataflowRunner", 
                      "--save_main_session", 
                      #"--staging_location=gs://%s/staging" % (known_args.temp_bucket),
                      "--temp_location=gs://%s/temp" % (known_args.temp_bucket)]

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = known_args.credential
    #schema_string='{"fields":[{"name":"usage","type":"record","fields":[{"name":"cpu","type":"STRING"},{"name":"mem","type":"STRING"}]},{"name":"_proc_PID_io","type":"record","fields":[{"name":"syscw","type":"INTEGER","mode":"repeated"},{"name":"cancelled_write_bytes","type":"INTEGER","mode":"repeated"},{"name":"wchar","type":"INTEGER","mode":"repeated"},{"name":"syscr","type":"INTEGER","mode":"repeated"},{"name":"read_bytes","type":"INTEGER","mode":"repeated"},{"name":"rchar","type":"INTEGER","mode":"repeated"},{"name":"write_bytes","type":"INTEGER","mode":"repeated"}]},{"name":"_proc_PID_stat","type":"record","fields":[{"name":"ds_agent","type":"STRING","mode":"repeated"}]},{"name":"_proc_PID_status","type":"record","fields":[{"name":"ShdPnd","type":"INTEGER","mode":"repeated"},{"name":"CapInh","type":"INTEGER","mode":"repeated"},{"name":"Cpus_allowed_list","type":"STRING","mode":"repeated"},{"name":"SigBlk","type":"INTEGER","mode":"repeated"},{"name":"State","type":"STRING","mode":"repeated"},{"name":"TracerPid","type":"INTEGER","mode":"repeated"},{"name":"FDSize","type":"INTEGER","mode":"repeated"},{"name":"VmRSS","type":"INTEGER","mode":"repeated"},{"name":"Gid","type":"INTEGER","mode":"repeated"},{"name":"CapBnd","type":"STRING","mode":"repeated"},{"name":"Utrace","type":"INTEGER","mode":"repeated"},{"name":"VmExe","type":"INTEGER","mode":"repeated"},{"name":"Pid","type":"INTEGER","mode":"repeated"},{"name":"SigIgn","type":"INTEGER","mode":"repeated"},{"name":"Groups","type":"INTEGER","mode":"repeated"},{"name":"Name","type":"STRING","mode":"repeated"},{"name":"Uid","type":"INTEGER","mode":"repeated"},{"name":"VmSwap","type":"INTEGER","mode":"repeated"},{"name":"SigCgt","type":"STRING","mode":"repeated"},{"name":"VmStk","type":"INTEGER","mode":"repeated"},{"name":"VmPeak","type":"INTEGER","mode":"repeated"},{"name":"VmData","type":"INTEGER","mode":"repeated"},{"name":"nonvoluntary_ctxt_switches","type":"INTEGER","mode":"repeated"},{"name":"voluntary_ctxt_switches","type":"INTEGER","mode":"repeated"},{"name":"Mems_allowed_list","type":"STRING","mode":"repeated"},{"name":"Mems_allowed","type":"STRING","mode":"repeated"},{"name":"SigQ","type":"STRING","mode":"repeated"},{"name":"Tgid","type":"INTEGER","mode":"repeated"},{"name":"Cpus_allowed","type":"STRING","mode":"repeated"},{"name":"CapEff","type":"STRING","mode":"repeated"},{"name":"VmLck","type":"INTEGER","mode":"repeated"},{"name":"VmPTE","type":"INTEGER","mode":"repeated"},{"name":"VmSize","type":"INTEGER","mode":"repeated"},{"name":"CapPrm","type":"STRING","mode":"repeated"},{"name":"PPid","type":"INTEGER","mode":"repeated"},{"name":"SigPnd","type":"INTEGER","mode":"repeated"},{"name":"Threads","type":"INTEGER","mode":"repeated"},{"name":"VmHWM","type":"INTEGER","mode":"repeated"},{"name":"VmLib","type":"INTEGER","mode":"repeated"}]}]}'
    schema = parse_table_schema_from_json(known_args.schema_string)
    
    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
     p | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=known_args.skip_json_lines)
    
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType

             schema=schema,

             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
