# in the pmr cli, add a separate ebs volume to the launch params, and change user to ubuntu

export AWS_DEFAULT_REGION=us-east-1
poormanray create --name jiachengl-dedup-alldressed --number 1 --instance-type x2idn.32xlarge --detach --ami-id ami-084568db4383264d4 # ubuntu
sleep 30
poormanray setup --name jiachengl-dedup-alldressed # setup AWS credentials
poormanray run --name jiachengl-dedup-alldressed --script aws_workflow.sh
# poormanray map --name jiachengl-dedup-alldressed --script ablations
poormanray terminate --name jiachengl-dedup-alldressed
