#!/bin/sh 
source .env
set pmi_aws_role
set aws_region

saml2aws login --role=$pmi_aws_role --session-duration=3600
export AWS_DEFAULT_REGION=$aws_region
export AWS_PROFILE=saml 

python3 main-script.py