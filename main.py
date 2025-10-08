import boto3
import json
import time
import logging
import streamlit as st
from botocore.exceptions import ClientError

# SET REGION AND RESOURCES
region = 'eu-west-1'
s3_resource = boto3.resource('s3', region_name=region)
s3_client = boto3.client('s3', region_name=region)
comprehend = boto3.client('comprehend', region_name=region)

st.title("AI Document Analyzer")

# SET BUCKET NAME, CHECK IF BUCKET EXISTS - IF NOT CREATE ONE IN SET REGION
bucket_name = 'ai-doc-records-lee-b'
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

# FUNCTION TO CHECK S3 RESULTS
def check_s3_results(file_key, max_wait_time=300, sleep_interval=5):
    start_time = time.time()
    base_name = file_key.rsplit('/', 1)[-1]
    name_only = base_name.rsplit('.', 1)[0]
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
                if code in ("404", "NoSuchKey", "NotFound"):
                    logging.debug(f"{candidate} not found yet.")
                else:
                    logging.warning(f"head_object for {candidate} failed with {code}: {e}")

        logging.info("Still waiting for processed file...")
        time.sleep(sleep_interval)

    logging.error(f"Timeout after {max_wait_time} seconds waiting for processed file for {file_key}")
    return None, None

#FUNCTION TO DETECT DOCUMENT TYPE (CV, INVOICE, ETC..)
def detect_document_type(uploaded_file):

    try:
        if uploaded_file.type == "text/plain":
            text = uploaded_file.getvalue().decode("utf-8", errors="ignore").lower()
        elif uploaded_file.type == "application/pdf":
            text = uploaded_file.getvalue().decode("latin-1", errors="ignore").lower()
        else:
            text = ""

        # CHECK FOR KEYWORDS IN DOCUMENT : RETURN DOCUMENT TYPE
        if any(word in text for word in ["cv", "resume", "curriculum vitae"]):
            return "CV / Resume"
        elif "invoice" in text:
            return "Invoice"
        else:
            return "Unknown"

    except Exception as e:
        print(f"Error detecting document type: {e}")
        return "Unknown"

#FUNCTION TO CHECK CV SKILLS AND EXTRACT THEM
def cv_skill_extract(text):
    skills_list = ["python", "c++", "c#", "java", "aws", "excel",
        "customer service", "communication", "sql", "javascript",
        "react", "node", "project management", "leadership"]
    text_lower = text.lower()
    found_skills = [skill for skill in skills_list if skill in text_lower]

    return found_skills

# UPLOAD MULTIPLE FILES
uploaded_files = st.file_uploader(
    "Upload one or more text or PDF files",
    type=['txt', 'pdf'],
    accept_multiple_files=True
)

if uploaded_files:
    for uploaded_file in uploaded_files:
        st.write(f"📄 Processing: {uploaded_file.name}")
        doc_type = detect_document_type(uploaded_file)
        st.write(f"**Detected Document Type :** {doc_type}")

        try:
            # Step 1 — Upload file to S3 under 'uploads/'
            s3_key = f"uploads/{uploaded_file.name}"
            s3_client.upload_fileobj(uploaded_file, bucket_name, s3_key)
            st.success(f"✅ {uploaded_file.name} uploaded successfully!")


            # Step 2 — Wait for Lambda to process it and drop a result in /processed/
            with st.spinner("Processing — this may take a few minutes..."):
                processed_key, result_url = check_s3_results(
                    s3_key,
                    max_wait_time=300,
                    sleep_interval=5
                )

            # Step 3 — If result found, display it
            if result_url:
                st.success(f"Processing complete for {uploaded_file.name}")

                if processed_key.endswith("_processed.txt"):
                    try:
                        resp = s3_client.get_object(Bucket=bucket_name, Key=processed_key)
                        text = resp["Body"].read().decode("utf-8")

                    except ClientError as e:
                        st.error(f"Could not read processed file: {e}")
                    else:
                        # It's a processed text file — try reading the text content and display it neatly
                        try:
                            lines = text.splitlines()
                            st.subheader("Processed text file output")

                            if lines:
                                st.write(lines[0])  # Word count
                                if len(lines) > 1:
                                    st.write(lines[1])  # Sentiment

                                if len(lines) > 2:
                                    st.write("Entities:")

                                    try:
                                        entities_json = lines[2].split("Entities:", 1)[-1].strip()
                                        entities = json.loads(entities_json)
                                        grouped = {}
                                        for ent in entities:
                                            score = ent.get("Score", 0)
                                            if score < 0.80:
                                                continue
                                            grouped.setdefault(ent.get("Type", "UNKNOWN"), []).append(
                                                ent.get("Text", ""))

                                        for etype, texts in grouped.items():
                                            st.markdown(f"**{etype}**")
                                            for t in texts:
                                                st.write(f"- {t}")
                                            st.markdown("---")
                                    except Exception as e:
                                        st.warning(f"Could not parse entities: {e}")
                            else:
                                st.text_area("Processed text file (empty?)", text, height=300)

                        except ClientError as e:
                            st.error(f"Could not read processed text: {e}")


                if processed_key.endswith("_processed.pdf"):
                    try:
                        resp = s3_client.get_object(Bucket=bucket_name, Key=processed_key)
                        text = resp["Body"].read().decode("utf-8")
                    except ClientError as e:
                        st.error(f"Could not read processed file: {e}")
                    else:
                        # It's a processed PDF — try reading the text content and display it neatly
                        try:
                            lines = text.splitlines()
                            st.subheader("Processed PDF file output")

                            if lines:
                                st.write(lines[0])  # Word count
                                if len(lines) > 1:
                                    st.write(lines[1])  # Sentiment

                                if len(lines) > 2:
                                    st.write("Entities:")

                                    try:
                                        entities_json = lines[2].split("Entities:", 1)[-1].strip()
                                        entities = json.loads(entities_json)
                                        grouped = {}
                                        for ent in entities:
                                            score = ent.get("Score", 0)
                                            if score < 0.80:
                                                continue
                                            grouped.setdefault(ent.get("Type", "UNKNOWN"), []).append(
                                                ent.get("Text", ""))

                                        for etype, texts in grouped.items():
                                            st.markdown(f"**{etype}**")
                                            for t in texts:
                                                st.write(f"- {t}")
                                            st.markdown("---")
                                    except Exception as e:
                                        st.warning(f"Could not parse entities: {e}")
                            else:
                                st.text_area("Processed PDF (empty?)", text, height=300)

                        except ClientError as e:
                            st.error(f"Could not read processed PDF: {e}")

            else:
                st.error(f"Processing timed out for {uploaded_file.name}. Check Lambda logs.")
        except ClientError as e:
            st.error(f"Error uploading {uploaded_file.name}: {e}")
