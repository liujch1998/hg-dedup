# in the pmr cli, change user to ubuntu; if using x2idn, add a separate ebs volume to the launch params, and extend sleep time to 10 seconds after creating screen

export AWS_DEFAULT_REGION=us-east-1
export NAME="jiachengl-dedup-cc_news"
poormanray create --name ${NAME} --number 1 --instance-type i7i.48xlarge --detach --ami-id ami-084568db4383264d4 # ubuntu
sleep 60
poormanray setup --name ${NAME} # setup AWS credentials
poormanray run --name ${NAME} --script aws_workflow.sh --detach # --detach to run in screen
# poormanray terminate --name ${NAME}
