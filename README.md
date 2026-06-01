# Big-Data-Analysis-Technology-Comparison

This project evaluates and compares different technologies for big data analysis using a real-world dataset of significant size.

The study focuses on:

- Data processing design
- Data preparation and cleaning
- Comparison of different analytical technologies
- Efficiency and scalability of the implemented solutions

The project compares the performance of Hadoop, Spark Core, and Spark SQL in both local and clustered environments.

---

## Overview


---

## Prerequisites

To reproduce the distributed experiments, the following requirements are necessary:

- An active AWS Academy account with credits
- Access to the AWS Academy Learner Lab
- AWS CLI installed on your local machine


# AWS CLI Installation

AWS CLI is required to interact with AWS services and manage the cluster infrastructure.

# Installation (Linux, macOS, Windows)

#### Linux

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

#### macOS

```bash
brew install awscli
```

#### Windows (PowerShell)

```bash
msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi
```

## AWS Academy Setup

The Learner Lab must be launched manually from the AWS Academy dashboard. Once activated, AWS Academy provisions a temporary AWS environment and a set of session-based credentials. These credentials are required to access AWS services and are valid only for the duration of the lab session. They can be retrieved from the **AWS Details → AWS CLI** section.

To configure the AWS CLI on the local machine using the provided credentials, run:

```bash
aws configure
```

You will then be prompted to enter the required parameters:

```bash
AWS Access Key ID []: 
AWS Secret Access Key []: 
AWS Session Token []: 
Default region name []: 
Default output format []: json 
```

# This step must be repeated every time a new session is started.
