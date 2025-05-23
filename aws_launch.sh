# in the pmr cli, add a separate ebs volume to the launch params (if using x2idn instance), and change user to ubuntu

export AWS_DEFAULT_REGION=us-east-1
export NAME="jiachengl-dedup-10x-b"
poormanray create --name ${NAME} --number 1 --instance-type x2idn.32xlarge --detach --ami-id ami-084568db4383264d4 # ubuntu
sleep 30
poormanray setup --name ${NAME} # setup AWS credentials
poormanray run --name ${NAME} --script aws_workflow.sh # --detach to run in screen
# poormanray map --name ${NAME} --script ablations
# poormanray terminate --name ${NAME}
