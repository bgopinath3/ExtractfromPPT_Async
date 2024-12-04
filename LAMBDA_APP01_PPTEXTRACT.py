import os
from io import BytesIO
import tarfile
import boto3
import subprocess
import brotli
import uuid
from logging import exception
import logging
import time
from botocore.exceptions import ClientError
from textractor import Textractor
from textractor.data.constants import TextractFeatures
from textractor.visualizers.entitylist import EntityList
from textractor.data.text_linearization_config import TextLinearizationConfig
from textractcaller.t_call import call_textract, Textract_Features,get_full_json,Textract_API
import trp
from textractprettyprinter.t_pretty_print import get_text_from_layout_json
import json
import datetime

#set libre office installation location 
libre_office_install_dir = "/tmp/instdir"
#set Textract Client
textract = boto3.client('textract', region_name='us-west-1')
# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
#configure dynamodb for status
extracttext_dydb = boto3.resource("dynamodb", region_name = "us-west-1")
# Define the DynamoDB table
table = extracttext_dydb.Table("ExtractTextStatus")
# MIME Types
mime_types = ['application/vnd.openxmlformats-officedocument.presentationml.presentation','application/vnd.ms-powerpoint']
mime_extension = ['.ppt','.pptx','.PPT','.PPTX','ppt','pptx','PPT','PPTX']
## Update Status Start
def update_status(task_id, status, error_message=None, source_file_url=None, converted_file_url=None):
    update_expression = "SET #s = :s"
    expression_attribute_values = {":s": status}
    expression_attribute_names = {"#s": "status"}
    # Get the current datetime
    update_expression += " , UpdatedDate = :t"
    current_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    expression_attribute_values [":t"]= current_datetime
    #expression_attribute_names = {"#ts": "UpdatedDate"}
    
    if error_message:
        update_expression += ", error_message = :e"
        expression_attribute_values[":e"] = error_message
        
    if source_file_url:
        update_expression += ", source_file_url = :u"
        expression_attribute_values[":u"] = source_file_url
        
    if converted_file_url:
        update_expression += ", converted_file_url = :u"
        expression_attribute_values[":u"] = converted_file_url

    table.update_item(
        Key={"task_id": task_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names,
    )
## Update Status End

## Process Textract Result Start
def GetTextractorJobResults(jobId,region_name,task_id):
    # This routine is for Layout aware textractor import Textractor for Call_trextrator
    try:
        #task_id=jobId
        #textract = boto3.client('textract', region_name=region_name)
        logger.info(f"inside Layout aware trxtract job")
        response = get_full_json(job_id=jobId, textract_api=Textract_API.ANALYZE,
                        boto3_textract_client=textract)
        update_status(task_id, "PROCESSING")
        logger.info(f"response is :{response}")
        return response
        
    except Exception as exception :
        update_status(task_id, "FAILED", error_message=str(exception))
        logger.error(f"Exception in GetTextractorJobResults is {str(exception)} ")
        return False
        
def GetLayoutTextTextractResult(json_pages, jobId,bucket_name,region_name,task_id,file_name,key_prefix):
    # this code is for Layout Aware Textrator & Printer for call_textract
    try:
        #dynamicfilename = f"/{file_name}_{jobId}.json"
        #save_txt_path = "s3://{}/{}".format(bucket_name, dynamicfilename)
        logger.info(" In Layout Extract")
        layout = get_text_from_layout_json(textract_json=json_pages
                                       ,exclude_figure_text=False # optional
                                       ,exclude_page_header=True # optional
                                       ,exclude_page_footer=True # optional
                                       ,exclude_page_number=True # optional
                                       #,save_txt_path=save_txt_path  #optional
                                      ) 
        logger.info(f"Json Layout : {layout} ")
        update_status(task_id, "PROCESSING")
        UploadResultToS3Bucket(jobId, layout,bucket_name,region_name,task_id,file_name,key_prefix)
        logger.info(f"upload to s3 completed")
        return True
    except Exception as exception :
        update_status(task_id, "FAILED", error_message=str(exception))
        logger.error(f"Exception in GetLayoutTextTextractResult and error is {str(exception)} ")
        return False
        
def UploadResultToS3Bucket(jobId, data,Bucket_name,region_name,task_id,file_name,key_prefix):
    try:
        s3_client = boto3.resource('s3', region_name=region_name)
        
        logger.info(f"Inside upload results to S3 bucket..")
        logger.info(f"bucket for uploading result {Bucket_name}")
        #dynamicfilename = jobId +".json"
        #dynamicfilename = f"{key_prefix}/{file_name}_{jobId}.json"
        # replacing textract job id by task id
        dynamicfilename = f"{key_prefix}/{file_name}_{task_id}.json"
        #dynamicfilename = jobId +".txt"
        logger.info(f"Dynamic file name is : {dynamicfilename}")
        local_file_path = "/tmp/textractresult.json"
        #local_file_path = "/tmp/textractresult.txt"
        with open(local_file_path, 'w') as fp:
            #fp.write(data)
            json.dump(data, fp)
        logger.info(f"Result is stored in local .json file..")
        s3_client.meta.client.upload_file(local_file_path, Bucket_name, dynamicfilename)
        logger.info(f"file uploaded successfully..")
        converted_file_url= "s3://{}/{}".format(Bucket_name, dynamicfilename)
        
        update_status(task_id, "COMPLETED",converted_file_url= converted_file_url)
        logger.info(f"Result S3 location {converted_file_url}")
        os.remove(local_file_path)
        logger.info(f"file deleted after upload to s3..")
    except Exception as exception :
        update_status(task_id, "FAILED", error_message=str(exception))
        logger.error("Exception in upload to s3 bucket and error is {} ".format(exception))
    
## Process Textract Result End


##Textract Invoke Code Start
def TagS3ObjectWithJobId(s3_bucket, s3_key, JobId,region_name,task_id):
    try:
        s3_client = boto3.client('s3',region_name=region_name)
        put_tags_response = s3_client.put_object_tagging(
                                Bucket=s3_bucket,
                                Key=s3_key,    
                                Tagging={
                                    'TagSet': [
                                        {
                                            'Key': 'TableExtractJobId',
                                            'Value': JobId
                                        },
                                    ]
                                }
                            )
        if put_tags_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Successfully tagged..")
            return True
        else:
            update_status(task_id, "FAILED", error_message=str(" ERROR : TAGGING S3 object JobId failed"))
            logger.info("Tagging failed..")
            return False
    except Exception as exception :
        update_status(task_id, "FAILED", error_message=str(exception))
        logger.error("Exception happend message is: ", exception)
        return False

        
def ProcessDocumentforLayout(s3_bucket, s3_key,region_name,task_id):
    sleepy_time = 1
    retry = 0
    flag = 'False'
    try:
        from_path = "s3://{}/{}".format(s3_bucket, s3_key)
        #textract = boto3.client('textract', region_name=region_name)
        #snsclient = boto3.client('sns')
        logger.info (f"ProcessDocumentforLayout file path is : {from_path} ")
        logger.info('Region {region_name}')
        while retry < 4 and  flag == 'False' :
            response = call_textract(input_document=from_path,
                                     features=[Textract_Features.LAYOUT,Textract_Features.TABLES,Textract_Features.FORMS],
                                                notification_channel= None, #{'RoleArn': roleArn, 'SNSTopicArn': SNSTopicArn},
                                                return_job_id= True,
                                                force_async_api = True,
                                                boto3_textract_client=textract
                                                )
            logger.info(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.info(f"Start Job Id: {response['JobId']}")
                #update_status(task_id, "PROCESSING",source_file_url=from_path)
                update_status(task_id, "PROCESSING")
                #message = json.dumps({"default":json.dumps(response)})
                #snsresponse = snsclient.publish(TopicArn=SNSTopicArn, Message=message, MessageStructure='json') 
                #if snsresponse['ResponseMetadata']['HTTPStatusCode'] == 200:
                #    print("Published multi-format messageid %s.", snsresponse["MessageId"])
                return response['JobId']
            else:
                update_status(task_id, "RETRYING",source_file_url=from_path)
                time_to_sleep = 2**retry
                retry +=1
                time.sleep(time_to_sleep)
    except Exception as exception :
        logger.error("Exception happend message is: {exception}")
        update_status(task_id, "FAILED", error_message=str(exception))
        return False
## Textract Invoke Code END

## PPT to PDF Conversion Start
def load_libre_office():
    if os.path.exists(libre_office_install_dir) and os.path.isdir(
        libre_office_install_dir
    ):
        logger.info("We have a cached copy of LibreOffice, skipping extraction")
    else:
        logger.info(
            "No cached copy of LibreOffice, extracting tar stream from Brotli file."
        )
        buffer = BytesIO()
        with open("/opt/lo.tar.br", "rb") as brotli_file:
            d = brotli.Decompressor()
            while True:
                chunk = brotli_file.read(1024)
                buffer.write(d.decompress(chunk))
                if len(chunk) < 1024:
                    break
            buffer.seek(0)
            logger.info("Extracting tar stream to /tmp for caching.")
            with tarfile.open(fileobj=buffer) as tar:
                tar.extractall("/tmp")
                logger.info("Done caching LibreOffice!")
    return f"{libre_office_install_dir}/program/soffice.bin"


def download_from_s3(bucket, key, download_path,aws_region):
    try:
        s3 = boto3.client("s3",region_name=aws_region)
        s3.download_file(bucket, key, download_path)
        return True
    except Exception as e:
        logger.error(f"Error in Download to s3 : {str(e)}")
        return False


def upload_to_s3(file_path, bucket, key,aws_region):
    try:
        s3 = boto3.client("s3",region_name=aws_region)
        s3.upload_file(file_path, bucket, key)
        return True
    except Exception as e:
        logger.error(f"Error in upload to s3 : {str(e)}")
        return False

def delete_from_s3(bucket,file_path,aws_region):
    try:
        s3 = boto3.client("s3",region_name=aws_region)
        s3.delete_object(Bucket=bucket,Key=file_path)
        return True
    except Exception as e:
        logger.error(f"Error in delete object from s3 : {str(e)}")
        return False
        
def convert_office_to_pdf(soffice_path, word_file_path, output_dir):
    conv_cmd = f"{soffice_path} --headless --norestore --invisible --nodefault --nofirststartwizard --nolockcheck --nologo --convert-to pdf:writer_pdf_Export --outdir {output_dir} {word_file_path}"
    response = subprocess.run(
        conv_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if response.returncode != 0:
        response = subprocess.run(
            conv_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if response.returncode != 0:
            return False
    return True
## PPT to PDF conversion end

def lambda_handler(event, context):
    try:
        
        task_id = event.get("task_id", str(uuid.uuid4()))
        bucket = event["bucket_name"]
        bucket_region = event["bucket_region"]
        key = event["object_key"]
        
        update_status(task_id, "PROCESSING")
        
        s3 = boto3.client("s3",region_name=bucket_region)
        key_name, key_ext = os.path.splitext(key)
        metadata = s3.head_object(Bucket=bucket, Key=key)
        # check if file is PPT or PPTX else fail
        contenttype = metadata['ContentType']
        if contenttype not in mime_types :
            if key_ext not in mime_extension :
                update_status(task_id, "FAILED", error_message= "Only Powerpoint with with extension .ppt or .pptx is allowed")
                return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Only Powerpoint with with extension .ppt or .pptx is allowed"}

        from_s3_path = "s3://{}/{}".format(bucket, key)
        key_prefix, base_name = os.path.split(key)
        download_path = f"/tmp/{base_name}"
        output_dir = "/tmp"
        if download_from_s3(bucket, key, download_path,bucket_region) :
            update_status(task_id, "PROCESSING", source_file_url=from_s3_path)
        else:
            update_status(task_id, "FAILED", error_message= "Download from S3 Failed")
            return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Download from S3 Failed"}
        
        soffice_path = load_libre_office()
        is_converted = convert_office_to_pdf(soffice_path, download_path, output_dir)
        if is_converted:
            file_name, _ = os.path.splitext(base_name)
           
            logger.info(f"key_prefix {key_prefix}")
            logger.info(f"base_name {base_name}")
            logger.info(f"file_name {file_name}")
            logger.info(f"output_dir {output_dir}")
            
            if upload_to_s3(
                f"{output_dir}/{file_name}.pdf",
                bucket,
                f"{key_prefix}/{file_name}.pdf",
                bucket_region
            ) :
                update_status(task_id, "PROCESSING")
            else:
                update_status(task_id, "FAILED", error_message=str( "Upload to S3 Failed"))
                return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Upload to S3 Failed"}
                
            temp_s3_key = f"{key_prefix}/{file_name}.pdf"
            temp_s3_path = "s3://{}/{}".format(bucket, temp_s3_key)
            logger.info( "Temp s3 path : {temp_s3_path}")
            logger.info("Temp s3 key {temp_s3_key}")
            TextractResult = ProcessDocumentforLayout(bucket, temp_s3_key,bucket_region,task_id)
            if TextractResult :
                logger.info("job id returned..")
                TagResults = TagS3ObjectWithJobId(bucket, temp_s3_key, TextractResult,bucket_region,task_id)
                if TagResults :
                    logger.info("Tagging successfully completed")
                    result = GetTextractorJobResults(TextractResult,bucket_region,task_id)
                    logger.info('Result is : {result}')
                    logger.info ("Textract Results extracted successfully..")
                    if result:
                        if GetLayoutTextTextractResult(result, TextractResult,bucket,bucket_region,task_id,file_name,key_prefix) :
                            logger.info ("Process completed successfully..")
                            delete_from_s3(bucket,temp_s3_key,bucket_region)
                            if os.path.exists(download_path):
                                os.remove(download_path)
                                logger.info(f"Deleted temporary file {download_path}")
                            return {"statusCode": 200, "task_id": task_id, "status": "SUCCESSFUL","response": " Process completed successfully"}
                        else :
                            logger.error ("Inside GetLayoutTextTextractResult - Textract extract text data did not retrieved..")
                            return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Inside GetLayoutTextTextractResult - Textract extract text data did not retrieved.."}
                    else :
                        logger.error("Inside GetTextractorJobResults - Textract extract results did not retrieved..")
                        return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Inside GetTextractorJobResults - Textract extract results did not retrieved.."}
                    #return TextractResult
                else :
                    logger.error("Inside GetTextractorJobResults - Textract extract results did not retrieved..")
                    return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Inside TagS3ObjectWithJobId - Textract extract tagging failed"}
                    #return False
            else :
                logger.error("Inside GetTextractorJobResults - Textract extract results did not retrieved..")
                return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Inside ProcessDocumentforLayout - Textract Extract failed"}
                #return False
            #return {"response": "file converted to PDF and available at same S3 location of input key"}
        else:
            update_status(task_id, "FAILED", error_message= "cannot convert this document to PDF")
            return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": "Inside convert_office_to_pdf -cannot convert this document to PDF"}
    except Exception as exception :
        update_status(task_id, "FAILED", error_message=str(exception))
        logger.error(f"Exception : {str(exception)}")
        return {"statusCode": 500, "task_id": task_id, "status": "FAILED","response": str(exception)}
 #   finally:
 #       # Clean up the temporary files
 #       if os.path.exists(download_path):
 #           os.remove(download_path)
 #           logger.info(f"Deleted temporary file {download_path}")

            
