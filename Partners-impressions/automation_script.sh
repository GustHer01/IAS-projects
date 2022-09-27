#!/bin/sh 

#source ~/.zshrc
#echo "Starting script"
#echo $snowflake_user
#PATH="/opt/anaconda3/bin:/opt/anaconda3/condabin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Users/gustavoh/Downloads/spark3/spark-3.2.1-bin-hadoop3.2/bin"
saml2aws login --role=$pmi_aws_role --session-duration=3600
export AWS_DEFAULT_REGION=$aws_region
export AWS_PROFILE=saml 

python3 main-script.py

#echo "Finished"