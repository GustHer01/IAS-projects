#!/bin/sh 

#source ~/.zshrc
#echo "Starting script"
#echo $snowflake_user
#PATH="/opt/anaconda3/bin:/opt/anaconda3/condabin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Users/gustavoh/Downloads/spark3/spark-3.2.1-bin-hadoop3.2/bin"
saml2aws login --role=arn:aws:iam::784347022195:role/DataLake-GPS --session-duration=3600 --username=ghernandez@integralads.com --password=$snowflake_pass
export AWS_DEFAULT_REGION=us-east-1
export AWS_PROFILE=saml 

python3 main-script.py

#echo "Finished"