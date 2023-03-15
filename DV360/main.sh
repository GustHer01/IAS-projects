#!/bin/sh

saml2aws login --role=$dv360_role --session-duration=3600
export AWS_DEFAULT_REGION=us-east-1
export AWS_PROFILE=saml

python3 automation_check.py
