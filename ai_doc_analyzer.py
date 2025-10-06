import boto3
import streamlit as st
from botocore.exceptions import ClientError
import time

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
        CreateBucketConfiguration={'LocationConstraint': region}
    )
    print(f"{bucket_name} - bucket created in '{region}'")
else:
    print(f"{bucket_name} already exists, no bucket created.")

# UPLOAD FILE
uploaded_file = st.file_uploader("Upload a text or PDF file", type=['txt', 'pdf'])

import time
import logging
from botocore.exceptions import ClientError

def check_s3_results(file_key, max_wait_time=300, sleep_interval=5):
    """
    Wait for processed/<name>_processed.txt or .pdf to appear.
    Returns presigned URL string if found, otherwise None.
    """
    start_time = time.time()
    base_name = file_key.rsplit('/', 1)[-1]        # e.g. "LeeBurtonCV_2025.pdf"
    name_only = base_name.rsplit('.', 1)[0]        # e.g. "LeeBurtonCV_2025"
    expected_txt_key = f"processed/{name_only}_processed.txt"
    expected_pdf_key = f"processed/{name_only}_processed.pdf"

    logging.info(f"Waiting for processed keys: {expected_txt_key} or {expected_pdf_key}")

    while time.time() - start_time < max_wait_time:
        for candidate in (expected_txt_key, expected_pdf_key):
            try:
                s3_client.head_object(Bucket=bucket_name, Key=candidate)
                # exists — return presigned URL
                url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': candidate},
                    ExpiresIn=3600
                )
                logging.info(f"Found processed object: {candidate}")
                return candidate, url
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                # 404 / NotFound: keep waiting; other errors log & keep waiting
                if code in ("404", "NoSuchKey", "NotFound"):
                    logging.debug(f"{candidate} not found yet.")
                else:
                    logging.warning(f"head_object for {candidate} failed with {code}: {e}")
        # optional: show a log every loop so you know it's polling
        logging.info("Still waiting for processed file...")
        time.sleep(sleep_interval)

    logging.error(f"Timeout after {max_wait_time} seconds waiting for processed file for {file_key}")
    return None, None


if uploaded_file is not None:
    try:
        s3_key = f"uploads/{uploaded_file.name}"
        s3_client.upload_fileobj(uploaded_file, bucket_name, s3_key)
        st.success(f"Upload {uploaded_file.name} successful!")
    except ClientError as e:
        st.error(f"Error uploading: {e}")
    else:
        # Optional: immediate local analysis for .txt as before...
        # Now wait for processed result, with spinner shown to user
        with st.spinner("Processing — this can take up to a few minutes..."):
            processed_key, result_url = check_s3_results(s3_key, max_wait_time=300, sleep_interval=5)

        if result_url:
            st.success("Processing complete — result ready.")
            # If the result is a text file, fetch and display it inline:
            if processed_key.endswith("_processed.txt"):
                try:
                    resp = s3_client.get_object(Bucket=bucket_name, Key=processed_key)
                    text = resp['Body'].read().decode('utf-8')
                except ClientError as e:
                    st.error(f"Could not read processed file: {e}")
                else:
                    # Display nicely
                    lines = text.splitlines()
                    st.subheader("Processed output")
                    if lines:
                        st.write(lines[0])  # word count
                        if len(lines) > 1:
                            st.write(lines[1])  # sentiment
                        if len(lines) > 2:
                            st.write("Entities:")
                            try:
                                import json
                                ents = json.loads(lines[2].split("Entities: ",1)[1])
                                for ent in ents:
                                    st.write(f"- {ent.get('Text')} ({ent.get('Type')})")
                            except Exception:
                                st.text_area("Raw processed text", text, height=300)
                    else:
                        st.text_area("Processed file (empty?)", text, height=300)
            else:
                # It's likely a .pdf processed file (text in a .pdf key) — provide link / embed
                st.markdown(f"[Open processed file]({result_url})")
                st.markdown(f'<iframe src="{result_url}" width="100%" height="600"></iframe>', unsafe_allow_html=True)
        else:
            st.error("Processing did not complete within the wait time. Check Lambda logs for details.")
