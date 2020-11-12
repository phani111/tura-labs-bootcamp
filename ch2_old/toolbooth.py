import sys
import os
import logging
import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# setup python logging
logging.basicConfig(format='[%(levelname)-8s] [%(asctime)s] [%(module)-35s][%(lineno)04d] : %(message)s', level=logging.INFO)
logger = logging

TOLLBOOTH_HEADERS = 'date,tollbooth,license_plate,cornsilk,slate_gray,navajo_white'

def parse_csv(line):
    # breakout csv values into a list and strip out space, ", and carriage return
    values = [v.strip(' "\n') for v in str(line).split(',')]
    keys = TOLLBOOTH_HEADERS.split(',')
    # pack row in {key: value} dict with column values
    return dict(zip(keys, values))

class ParseRecordsAndAddTotals(beam.DoFn):
    def process(self, element, prices, *args, **kwargs):
        assert all(nut in prices for nut in ['cornsilk', 'slate_gray', 'navajo_white'])

        record_date = datetime.strptime(element['date'], '%Y.%m.%d')
        element['date'] = element['date']
        element['tollbooth'] = int(element['tollbooth'])
        element['cornsilk'] = int(element['cornsilk'])
        element['slate_gray'] = int(element['slate_gray'])
        element['navajo_white'] = int(element['navajo_white'])

        element['total'] = (
            (prices['cornsilk'] * element['cornsilk']) +
            (prices['slate_gray'] * element['slate_gray']) +
            (prices['navajo_white'] * element['navajo_white'])
        )

        element['week'] = record_date.isocalendar()[1]
        element['month'] = record_date.strftime("%Y.%m")

        yield element

def run():
    print("Town of Squirrelwink Bureau of Tolls and Nuts Affair \n\n PART-1")

    parser = argparse.ArgumentParser(description="Squirrelwink")
    parser.add_argument('-i', '--input', type=str, default='./data/input', help='path to data')
    parser.add_argument('-o', '--output', type=str,
                        default='./data/output',
                        help='Output folder')

    known_args, beam_args = parser.parse_known_args(sys.argv)

    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as pipeline:
        # current prices
        prices = ( 
            pipeline
            | beam.Create([('cornsilk', 2.0),
                            ('slate_gray', 3.5),
                            ('navajo_white', 7.0)])
        )
        
        logger.info("reading from csv file")

        records = (
            pipeline
            | beam.io.ReadFromText(os.path.join(known_args.input, 'tollbooth_logs.csv'), skip_header_lines=1)
            | beam.Map(parse_csv)
            | beam.ParDo(ParseRecordsAndAddTotals(), prices=beam.pvalue.AsDict(prices))
        )

        logger.info("save output to file")
        (
            records 
            | beam.io.WriteToText(os.path.join(known_args.output, "output"), file_name_suffix='.txt')
        )

if __name__ == '__main__':
    run()