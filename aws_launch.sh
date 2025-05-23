# in the pmr cli, change user to ubuntu; if using x2idn, add a separate ebs volume to the launch params, and extend sleep time to 10 seconds after creating screen

export AWS_DEFAULT_REGION=us-east-1
export NAME="jiachengl-dedup-10x-b"
poormanray create --name ${NAME} --number 1 --instance-type x2idn.32xlarge --detach --ami-id ami-084568db4383264d4 # ubuntu
sleep 60
poormanray setup --name ${NAME} # setup AWS credentials
poormanray run --name ${NAME} --script aws_workflow.sh --detach # --detach to run in screen
# poormanray map --name ${NAME} --script ablations
# poormanray terminate --name ${NAME}
