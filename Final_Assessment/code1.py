import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText



# ###############
def run(argv=None):

    p = beam.Pipeline(options=PipelineOptions())


    class Printer(beam.DoFn):
        def process(self, element):
            print element

    class Transaction(beam.DoFn):
        def process(self, element):
            #tripduration,starttime,stoptime,start_station_id,start_station_name,start_station_latitude,start_station_longitude,end_station_id,end_station_name,end_station_latitude,end_station_longitude,bikeid,usertype,birth_year,gender,t1,t2,t3,t4,t5 = element.split(',')
            t=[]
            t=element.split(',')
	    print(len(t))
            if t[0]!='tree_id': # just to avoid the problems caused by the csv table header
                #return [{"tripduration": tripduration,"starttime": starttime,"stoptime" : stoptime,"start_station_id" : start_station_id,"start_station_name" : start_station_name,"start_station_latitude" : start_station_latitude,"start_station_longitude" : start_station_longitude,"end_station_id" : end_station_id,"end_station_name" : end_station_name,"end_station_latitude" : end_station_latitude,"end_station_longitude": end_station_longitude ,"bikeid": bikeid,"usertype": usertype,"birth_year": birth_year,"gender": gender}]
                #return [{"tripduration": t[0],"starttime": t[1],"stoptime" : t[2],"start_station_id" : t[3],"start_station_name" : t[4],"start_station_latitude" : t[5],"start_station_longitude" : t[6],"end_station_id" : t[7],"end_station_name" : t[8],"end_station_latitude" : t[9],"end_station_longitude": t[10] ,"bikeid": t[11],"usertype": t[12],"birth_year": t[13],"gender": t[14]}]
                return[{"site_order": int(t[4]),"site_order_double": int(t[4])*2,"legal_status": t[1],"legal_status_r": t[1][::-1]}]


    data_from_source = (p
                        | 'Read the source file' >> ReadFromText('gs://group-c-assessment/final.csv')
                        | 'Clean the items' >> beam.ParDo(Transaction())
                        )

    project_id = "pe-training"  # replace with your project ID
    dataset_id = 'group_3_assessment'  # replace with your dataset ID
    table_id = 'new'  # replace with your table ID
    #table_schema = ('tripduration:INTEGER,starttime:TIMESTAMP,stoptime:TIMESTAMP,start_station_id:INTEGER,start_station_name:STRING	,start_station_latitude:FLOAT,start_station_longitude:FLOAT,end_station_id:INTEGER,end_station_name:STRING,end_station_latitude:FLOAT,end_station_longitude:FLOAT,bikeid:INTEGER,usertype:STRING,birth_year:INTEGER	,gender:STRING')
    table_schema = ('site_order:INTEGER,site_order_double:INTEGER,legal_status:STRING,legal_status_r:STRING')
    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects
    data_from_source | 'Write' >> beam.io.WriteToBigQuery(
                    table=table_id,
                    dataset=dataset_id,
                    project=project_id,
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=int(100)
                    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()




