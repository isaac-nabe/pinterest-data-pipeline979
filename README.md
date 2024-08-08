# Pinterest Data Emulation Project

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.

## Project Overview

This project involves setting up an infrastructure that emulates the data processing pipeline of Pinterest. You will work with simulated data representing user posts, geolocation information, and user details. The project is divided into milestones, with specific tasks to guide you through the setup.

## Table of Contents

- [Getting Started](#getting-started)
- [Data Description](#data-description)
- [Milestones](#milestones)
  - [Milestone 1: Setting Up the Environment](#milestone-1-setting-up-the-environment)
  - [Milestone 2: AWS Integration](#milestone-2-aws-integration)
  - [Milestone 3: Configuring the EC2 Kafka Client](#milestone-3-configuring-the-ec2-kafka-client)
  - [Milestone 4: Connect an MSK Cluster to an S3 Bucket](#milestone-4-connect-an-msk-cluster-to-an-s3-bucket)
- [Security](#security)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

To get started with this project, follow the steps below:

1. **Download the Project Files**: Obtain the zip package containing the necessary scripts and data files.
2. **Install Dependencies**: Ensure you have Python and necessary libraries installed. You can install required libraries using:
   ```bash
   pip install -r requirements.txt
    ```
## Data Description

The project includes three main data tables:
```
• pinterest_data: Contains data about posts being uploaded to Pinterest.
• geolocation_data: Contains geolocation data for each Pinterest post.
• user_data: Contains data about the users who uploaded each post.
```

## Milestones

### Milestone 1: Setting Up the Environment
**Run the Script:** Execute `user_posting_emulation.py` to simulate user posts and print out `pin_result`, `geo_result`, and `user_result`. These outputs represent one entry in their corresponding tables.

**Database Credentials:** Create a separate `db_creds.yaml` file for the database credentials (HOST, USER, PASSWORD values). Ensure this file is included in your `.gitignore` to avoid uploading sensitive details to GitHub.

### Milestone 2: AWS Integration
**Sign into AWS to start building the pipeline by following these steps:**

**Access Provided RDS:** Use the credentials provided to access the existing RDS instance which contains the necessary tables.

### Milestone 3: Configuring the EC2 Kafka Client
**Task 1:** Create a .pem Key Locally

1. Create Key Pair File:

>- Navigate to Parameter Store in your AWS account.
>- Using your KeyPairId, locate the specific key pair associated with your EC2 instance.
>- Select this key pair and under the Value field, select Show. Copy its entire value (including the BEGIN and END header) and paste it in a .pem file in VSCode.

2. Save Key Pair File:

>- Navigate to the EC2 console and identify the instance with your unique UserId.
>- Select this instance, and under the Details section, find the Key pair name and note it.
>- Save the previously created file in VSCode using the format: `user_id.pem`.

**Task 2:** Connect to the EC2 Instance
>- Follow the Connect instructions (SSH client) on the EC2 console to connect to your EC2 instance.

**Task 3:** Set Up Kafka on the EC2 Instance
1. Install Kafka:
>- Install Kafka on your client EC2 machine (ensure it matches the cluster version, 2.12-2.8.1).

2. Install IAM MSK Authentication Package:
>- Install the IAM MSK authentication package on your client EC2 machine.

3. Configure IAM Role:
>- Navigate to the IAM console on your AWS account.
>- Select the role with the format: `<your_UserId>-ec2-access-role`.
>- Copy the role ARN and make a note of it.
>- Edit the trust policy to add the principal IAM roles and replace ARN with the copied role ARN.

4. Configure Kafka Client:
>- Modify the client.properties file inside your kafka_folder/bin directory for AWS IAM authentication.

**Task 4:** Create the Kafka Topics
1. Retrieve Cluster Information:
>- Obtain the Bootstrap servers string and the Plaintext Apache Zookeeper connection string from the MSK Management Console.

2. Create Topics:
>- Navigate back to the bin folder in your EC2 instance:
```
cd /home/ec2-user/kafka_2.12-2.8.1/bin
```

>- Create the following three topics using the Kafka create topic command:
```
./kafka-topics.sh --bootstrap-server <BootstrapServerString> --command-config client.properties --create --topic <topic_name>
```

>- Ensure your topic names follow the `<your_UserId>.topic_name` syntax to avoid permission errors.

>- On successful topic creation, you will see a message similar to:
```
Created topic <UserID>.geo
```

>- Ensure your CLASSPATH environment variable is set properly and you have installed the aws-msk-iam-auth-1.1.5-all.jar package in the kafka_folder/libs.

### Milestone 4: Connect an MSK Cluster to an S3 Bucket
**Task 1:** Create a custom plugin with MSK connect

1. **Step 1:** Go to the S3 console and find the bucket that contains your UserId. The bucket name should have the following format: `user-<your_UserId>-bucket`. Make a note of the bucket name, as you will need it in the next steps.

2. **Step 2:** On your EC2 client, download the Confluent.io Amazon S3 Connector **and copy it to the S3 bucket** you have identified in the previous step.
>Connect to your EC2 Instance in your terminal:
```
ssh -i "your_key.pem" ec2-user@your-ec2-instance.amazonaws.com
```
>Download the S3 connector using the below:
```
wget https://d2p6pa21dvn84.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.5.13/confluentinc-kafka-connect-s3-10.5.13.zip
```

3. **Step 3:** Create your custom plugin in the MSK Connect console. For this project our AWS account only has permissions to create a custom plugin with the following name: `<your_UserId>-plugin`. Make sure to use this name when creating your plugin.
>The plugin object can be input as:
```
s3://<user_bucket>>/confluentinc-kafka-connect-s3-10.5.13.zip
```

**Task 2:** Create a connector with MSK connect

- For this project our AWS account only has permissions to create a connector with the following name: `<your_UserId>-connector`. **Make sure to use this name when creating your connector.**

**Make sure to use the correct configurations for your connector:**
```
connector.class=io.confluent.connect.s3.S3SinkConnector
# same region as our bucket and cluster
s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3
# include nomeclature of topic name, given here as an example will read all data from topic names starting with msk.topic....
topics.regex=`<YOUR_UUID>`.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=`<BUCKET_NAME>`
```

- Pay attention to the topics.regex field in the connector configuration. Make sure it has the following structure: `<your_UserId>.*`.
>This will ensure that data going through all the three previously created Kafka topics will get saved to the S3 bucket.

- When building the connector, make sure to choose the IAM role used for authentication to the MSK cluster in the Access permissions tab. Remember the role has the following format `<your_UserId>-ec2-access-role`. This is the same role you have previously used for authentication on your EC2 client, and contains all the necessary permissions to connect to both MSK and MSK Connect.

- Now that you have built the plugin-connector pair, data passing through the IAM authenticated cluster, will be automatically stored in the designated S3 bucket.

## File Structure

```
pinterest-data-pipeline979/
├── user_posting_emulation.py
├── .gitignore/
│   ├── db_creds.yaml
│   └── user_id.pem
├── README.md
└── requirements.txt
```

## Security
>- Ensure that your db_creds.yaml file is not uploaded to your repository by adding it to your .gitignore file.

## License

This project is licensed under the MIT License. See the [LICENSE](https://choosealicense.com/licenses/mit/) file for more details.