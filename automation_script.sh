#!/bin/bash

saml2aws login --role=arn:aws:iam::784347022195:role/DataLake-GPS --session-duration=3600 --username=ghernandez@integralads.com --password=$snowflake_pass
export AWS_DEFAULT_REGION=us-east-1
export AWS_PROFILE=saml 

python3 main-script.py