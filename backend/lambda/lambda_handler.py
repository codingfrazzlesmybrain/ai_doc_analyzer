import json
import boto3
import logging

# Set up logging to send messages to CloudWatch
logging.getLogger().setLevel(logging.INFO)

# Create clients for S3, Comprehend, and Textract
s3_client = boto3.client('s3')
comprehend_client = boto3.client('comprehend')
textract_client = boto3.client('textract')

def lambda_handler(event, context):
    # Check if the event contains S3 records; if not, return an error
    if 'Records' not in event or not event['Records']:
        return {
            'statusCode': 400,
            'body': json.dumps('No S3 records found')
        }

    # Loop through each file upload event from S3
    for record in event['Records']:
        # Get the file key (name) from the S3 event
        key = record['s3']['object']['key']
        logging.info(f"Processing file: {key}")
        file_content = ""

        # Skip if the file is already processed
        if '_processed' in key and (key.endswith('.txt') or key.endswith('.pdf')):
            logging.info(f"Skipping processed file: {key}")
            continue

        # Get the bucket name for processing
        bucket = record['s3']['bucket']['name']

        from time import sleep

        if key.endswith('.pdf'):
            try:
                # Start Textract asynchronous job
                start_resp = textract_client.start_document_text_detection(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
                )
                job_id = start_resp['JobId']
                logging.info(f"Started Textract job {job_id} for {key}")

                # Poll for job completion (simple, beginner-friendly)
                max_wait_seconds = 120  # how long to wait total (adjust as needed)
                waited = 0
                sleep_interval = 5
                job_status = None

                while waited < max_wait_seconds:
                    job_resp = textract_client.get_document_text_detection(JobId=job_id)
                    job_status = job_resp.get('JobStatus')
                    logging.info(f"Textract job {job_id} status: {job_status}")
                    if job_status in ['SUCCEEDED', 'FAILED']:
                        break
                    sleep(sleep_interval)
                    waited += sleep_interval

                if job_status != 'SUCCEEDED':
                    logging.error(f"Textract job {job_id} did not succeed (status={job_status})")
                    continue

                # Job succeeded — gather text from pages
                pages = []
                # The get_document_text_detection returns paginated results; loop until no NextToken
                next_token = None
                while True:
                    if next_token:
                        resp = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
                    else:
                        resp = textract_client.get_document_text_detection(JobId=job_id)
                    blocks = resp.get('Blocks', [])
                    # Collect LINE blocks (or WORD if you prefer)
                    for block in blocks:
                        if block.get('BlockType') == 'LINE':
                            pages.append(block.get('Text', ''))
                    next_token = resp.get('NextToken')
                    if not next_token:
                        break

                file_content = ' '.join(pages)
                if not file_content.strip():
                    logging.error(f"No valid text extracted from {key} after Textract job {job_id}")
                    continue

            except textract_client.exceptions.AccessDeniedException as e:
                logging.error(f"Textract access denied for {key}: {e}")
                continue
            except Exception as e:
                logging.error(f"Error extracting text from {key}: {str(e)}")
                continue

        # --- Minimal safe handling for .txt files ---
        if key.endswith('.txt'):
            file_content = ""
            try:
                resp = s3_client.get_object(Bucket=bucket, Key=key)
                file_content = resp['Body'].read().decode('utf-8')
            except Exception as e:
                logging.error(f"Error reading {key}: {str(e)}")
                continue

        # Skip if no text
        if not file_content.strip():
            logging.info(f"No text found in {key}, skipping Comprehend and writing placeholder.")
            output_key = f"processed/{key.rsplit('/', 1)[1].replace('.txt', '_processed.txt')}"
            s3_client.put_object(
                Bucket=bucket,
                Key=output_key,
                Body="No text was extracted from the file.",
                ContentType="text/plain",
                ContentDisposition="inline"
            )
            logging.info(f"Wrote placeholder processed file: {output_key}")
            continue

        # Now safe to call Comprehend
        word_count = len(file_content.split())
        logging.info(f"Word count for {key}: {word_count}")
        try:
            sentiment = comprehend_client.detect_sentiment(Text=file_content, LanguageCode='en')
            entities = comprehend_client.detect_entities(Text=file_content, LanguageCode='en')
        except Exception as e:
            logging.error(f"Error analyzing {key} with Comprehend: {str(e)}")
            continue

        # Create a new key for the processed file
        output_key = f"processed/{key.rsplit('/', 1)[1].replace('.txt', '_processed.txt').replace('.pdf', '_processed.pdf')}"
        logging.info(f"Saving to output_key: {output_key}")

        # Prepare output with word count, sentiment, and entities
        output_data = f"Word count: {word_count}\nSentiment: {sentiment['Sentiment']}\nEntities: {json.dumps(entities['Entities'])}"

        # Save the processed data to S3
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=output_data,
                ContentType="text/plain",
                ContentDisposition="inline"
            )
            logging.info(f"Successfully saved processed file to {output_key}")
        except Exception as e:
            logging.error(f"Error saving {output_key}: {str(e)}")
            continue

        # Generate a presigned URL for the processed file
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': output_key},
            ExpiresIn=3600
        )
        logging.info(f"Presigned URL for {key}: {url}")

    # Return success message
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }