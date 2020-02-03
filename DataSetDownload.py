# Refer Exercise2.pptx for the architecture of AWS Kinesis, Glue and Redshift
# Here I am trying to download the data and send data as streaming
# Idea is to compress tsv into parquet . Kinesis Data firehouse is embedded with Glue job to convert data into parquet and load into Redshift 


# Data can be directly loaded into Redshift as well

# 

import json
import sys
import time
import os
import boto3
import gzip

class AWS_CONST:
    KINESIS_STREAM = 'Loadawsreview'
    IN_BUCKET = 'amazon-reviews-pds'
    INFLE = 'tsv/amazon_reviews_us_Camera_v1_00.tsv.gz'
    PARQUET_BUCKET = 'awsreview2812'

class kinesisprod:
    
    kinesis_client = boto3.client('kinesis', region_name='eu-west-1')
    
    def put_to_stream(self,filename):

        with open(filename,encoding="utf8") as fp:
            for cnt, line in enumerate(fp):
                print("here")
                self.kinesis_client.put_record(StreamName=AWS_CONST.KINESIS_STREAM,
                                                Data=line,
                                                PartitionKey=str(cnt))   
  
class loaddata:
    
    s3 = boto3.client('s3')
    kinesis_data = kinesisprod()
    
    
    def __init__(self, filename, outfile):
        self.filename = filename
        self.outfile = outfile
    
    def downloadS3data(self):
        
        #self.s3.upload_file(filename,'loadweatherdatahist',filename)
        self.s3.download_file(AWS_CONST.IN_BUCKET, AWS_CONST.INFLE,self.filename )
    
    def unzip(self):
        
        
        input = gzip.GzipFile(self.filename, 'rb')
        output = open(self.outfile, 'wb')
        output.write(input.read())
        input.close()
        output.close()  
        
        self.kinesis_data.put_to_stream(self.outfile) 
        
    def ParquettoRedshift(self):
        self.kinesis_data.put_to_stream(self.outfile) 
             
def main():
    AWSdata = loaddata('AWSOutFile.tsv.gz','AWSOutFile.tsv')
    AWSdata.downloadS3data()
    AWSdata.unzip()
    AWSdata.ParquettoRedshift()
    
    
    
    
if __name__ == "__main__":
    main()