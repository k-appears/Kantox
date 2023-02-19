# Provider configuration
provider "aws" {
  access_key = "ACCESS_KEY_HERE"
  secret_key = "SECRET_KEY_HERE"
  region     = "us-east-1"
}

# Create an S3 bucket to store the PySpark file
resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-bucket"
}


# Upload the PySpark file to the S3 bucket
resource "aws_s3_object" "my_object" {
  bucket = aws_s3_bucket.my_bucket.id
  key    = "my-pyspark-main.py"
  source = "/home/user/pyspark/main.py"
}

# Create an EMR cluster with PySpark installed
resource "aws_emr_cluster" "my_cluster" {
  name          = "my-emr-cluster"
  release_label = "emr-6.3.0"
  instances {
    master_instance_type = "m5.xlarge"
    core_instance_type   = "m5.xlarge"
    instance_count       = 1
  }
  applications {
    name = "Spark"
  }
  service_role = ""
}

# Run the PySpark script on the EMR cluster
resource "aws_emr_step" "my_step" {
  cluster_id = aws_emr_cluster.my_cluster.id
  name       = "my-pyspark-step"
  action_on_failure = "TERMINATE_CLUSTER"

  hadoop_jar_step {
    jar  = "command-runner.jar"
    args = [
      "spark-submit",
      "--deploy-mode", "cluster",
      "--master", "yarn",
      "--py-files", "s3://${aws_s3_bucket.my_bucket.bucket}/${aws_s3_object.my_object.key}",
      "s3://${aws_s3_bucket.my_bucket.bucket}/main.py",
      "arg1", "arg2" # Replace with your desired command line arguments
    ]
  }
}
