import boto3
import streamlit as st
from botocore.exceptions import ClientError

region = 'eu-west-1'
s3_resource = boto3.resource('s3', region_name=region)
s3_client = boto3.client('s3', region_name=region)
comprehend = boto3.client('comprehend', region_name=region)

st.title("AI Document Analyzer")

bucket_name = 'ai-doc-records-lee-b'

# CHECK IF BUCKET EXISTS, IF NOT CREATE ONE IN SET REGION
all_buckets = [bucket.name for bucket in s3_resource.buckets.all()]

if bucket_name not in all_buckets:
    print(f"{bucket_name} does not exist, creating one now...")
    s3_resource.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration = {'LocationConstraint': region}
    )
    print(f"{bucket_name} - bucket created in '{region}'")
else:
    print(f"{bucket_name} already exists, no bucket created.")

# UPLOAD FILE
uploaded_file = st.file_uploader("Upload a text document (TXT or PDF) ", type=['txt', 'pdf'])

if uploaded_file is not None:
    try:
        s3_client.upload_fileobj(uploaded_file,bucket_name,uploaded_file.name)
        st.success(f"Upload {uploaded_file.name} successful!")
    except ClientError as e:
        st.error(f"Error uploading: {e}")
    else:
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=uploaded_file.name)
            text = response['Body'].read().decode('utf-8')
        except ClientError as e:
            st.error(f"Error reading file '{uploaded_file.name}")
        else:
            # ANALYZE WITH COMPREHEND
            if text:
                sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
                entities = comprehend.detect_entities(Text=text, LanguageCode='en')

                st.subheader('Analysis Results')
                st.write("Sentiment:", sentiment['Sentiment'])
                st.write("Key Entities:")

                for entity in entities['Entities']:
                    st.write(f"- {entity['Text']} ({entity['Type']}) ")



