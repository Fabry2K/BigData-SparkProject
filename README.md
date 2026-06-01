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

The goal of this project is to analyze how Large Language Models (LLMs) interact with relational databases, both through SQL query generation (Text-to-SQL) and direct reasoning over serialized tabular data (Direct QA).

To this end, we implement a unified pipeline supporting both paradigms and assess their performance using data-centric metrics combined with a qualitative error analysis across different query types and database schemas. Experiments are conducted under multiple prompt configurations, including zero-shot, few-shot, and attribute-enhanced settings, to investigate their impact on model performance.

The experiments are conducted on a subset of the Spider benchmark (`book_1.sqlite` dataset), and evaluated using execution-based metrics.

---

## Prerequisites

To reproduce the distributed experiments, the following requirements are necessary:

- An active AWS Academy account
- Available AWS Academy credits
- Access to an AWS Academy Learner Lab
- AWS CLI installed on your local machine
- SSH client installed and configured
- Basic familiarity with AWS EC2 and cloud networking concepts


## AWS CLI Installation

AWS CLI is required to interact with AWS services and manage the cluster infrastructure.

### Installation (Linux, macOS, Windows)

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

