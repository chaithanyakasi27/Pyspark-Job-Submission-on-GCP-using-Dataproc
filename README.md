
# PySpark Job Submission on GCP using Dataproc

## Prerequisites
Before you start, ensure the following:

- You have a Google Cloud account and project set up.
- You have installed the **Google Cloud SDK** (`gcloud` CLI).
- You have basic knowledge of **Google Cloud Storage (GCS)**, **Google Dataproc**, and **PySpark**.

## 1. Install and Configure Google Cloud SDK (gcloud CLI)
1. **Install Google Cloud SDK**:  
   Download and install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).

2. **Configure the gcloud CLI**:  
   Open a terminal and run the following command:
   ```bash
   gcloud init
   ```
   Follow the prompts to log in using your Google account and select the correct project and region.

3. **Authenticate and configure the project**:  
   Run the following command to authenticate and configure your project:
   ```bash
   gcloud auth login
   gcloud config set project project=inductive-vista-453815-q1
   ```

---

## 2. Create a Key Pair (for SSH Access)
1. **Generate SSH Key Pair**:  
   In your terminal, generate an SSH key pair:
   ```bash
   ssh-keygen -t rsa -f ~/.ssh/dataproc-key
   ```

2. **Upload SSH Key to GCP**:  
   Upload the public key to GCP using the following command:
   ```bash
   gcloud compute project-info add-metadata --metadata-from-file ssh-keys=~/.ssh/dataproc-key.pub
   ```

---

## 3. Create a Cloud Storage Bucket and Prepare Data
1. **Create a Bucket in Google Cloud Storage (GCS)**:  
   Run the following command to create a GCS bucket:
   ```bash
   gsutil mb gs://data-source-2025-jan/
   ```

2. **Upload Files to GCS**:  
   Upload your `Financial.csv` and `mypysparkscript.py` using `gsutil`:
   ```bash
   gsutil cp D:\data engineer resources\Spark-Job\emr-cluster-GCP\datasets\Financial.csv gs://data-source-2025-jan/data/year-2025/month-01/day-19/Financial.csv
   gsutil cp D:\data engineer resources\Spark-Job\emr-cluster-GCP\datasets\mypysparkscript.py gs://data-source-2025-jan/scripts/mypysparkscript.py
   ```

![]()



## 4. Create a Dataproc Cluster
1. **Create the Dataproc Cluster**:  
   Use the following command to create a Dataproc cluster:
   ```bash
   gcloud dataproc clusters create my-dataproc-cluster-2  --region=us-central1 --zone=us-central1-b --single-node  --image-version=2.0-debian10 --project=inductive-vista-453815-q1 --scopes="cloud-platform" 
   ```

   ![]()

2. **Get the Cluster Information**:  
   After cluster creation, note the cluster name for job submission.

---

## 5. Submit the PySpark Job
1. **Submit the PySpark Job**:  
   Use the following command to submit your PySpark job:
   ```bash
   gcloud dataproc jobs submit pyspark gs://spark-job-data-bucket/scripts/mypysparkscript_aggregatebykey.py  --cluster=my-dataproc-cluster-2 --region=us-central1 --id=Spark_groupby_job
   ```

   This command will submit your PySpark script to the Dataproc cluster, where it will process the input file (`Financial.csv`) from GCS and write output to the specified GCS path.

---

## 6. Monitor the Job Status
1. **Monitor Job Status**:  
   You can check the status of your job from the GCP Console:
   - Go to **Dataproc > Jobs** and view the status.
   - Alternatively, use the following CLI command to check job status:
     ```bash
     gcloud dataproc jobs describe Spark_groupby_job --region=us-central1
     ```

---

## 7. Check Output
1. **Verify Output in GCS**:  
   Once the job completes, check the output in your GCS bucket under the specified directory:
   ```bash
   gsutil ls gs://data-source-2025-jan/output/
   ```

2. **Review Logs**:  
   If there are errors or logs to review, check the Dataproc job logs in the GCP Console or via the CLI:
   ```bash
   gcloud dataproc jobs wait Spark_groupby_job --region=us-central1

   ```



