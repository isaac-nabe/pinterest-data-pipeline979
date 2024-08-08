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
>- Using your KeyPairId (found in the email with your AWS login credentials), locate the specific key pair associated with your EC2 instance.
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