# AWS

### List all resource in AWS
```bash
aws resourcegroupstaggingapi get-resources
```

### List all resource in AWS with specific tag
```bash
aws resourcegroupstaggingapi get-resources --tag-filters Key=Name,Values=MyInstance
```


## IAM


### List all user in IAM
```bash
aws iam list-users
```

### List all group in IAM
```bash
aws iam list-groups
```

### List all role in IAM
```bash
aws iam list-roles
```


## CloudFormation

### List all stack in CloudFormation
```bash
aws cloudformation list-stacks
```

### List all active stack in CloudFormation
```bash
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE ROLLBACK_COMPLETE UPDATE_ROLLBACK_COMPLETE
```

### List all resource in CloudFormation
```bash
aws cloudformation list-stack-resources --stack-name <stack-name>
```

### List all resource in CloudFormation with specific type
```bash
aws cloudformation list-stack-resources --stack-name <stack-name> --resource-type AWS::EC2::Instance
```

### Export CloudFormation stack to JSON
```bash
aws cloudformation get-template --stack-name <stack-name> --output json
```

### List all change set in CloudFormation
```bash
aws cloudformation list-change-sets --stack-name <stack-name>
```

### List all change set in CloudFormation with specific status
```bash
aws cloudformation list-change-sets --stack-name <stack-name> --status CREATE_COMPLETE
```

### Export CloudFormation change set to JSON
```bash
aws cloudformation get-change-set --stack-name <stack-name> --change-set-name <change-set-name> --output json
```

### Push CloudFormation change set
```bash
aws cloudformation execute-change-set --stack-name <stack-name> --change-set-name <change-set-name>
```

### Delete CloudFormation stack
```bash
aws cloudformation delete-stack --stack-name <stack-name>
```



## S3

### List all bucket in S3
```bash
aws s3api list-buckets
```

### List all object in S3 bucket
```bash
aws s3api list-objects --bucket <bucket-name>
```


## EC2

### List all instance in EC2
```bash
aws ec2 describe-instances
```


## Lambda

### List all function in Lambda
```bash
aws lambda list-functions
```





# Azure

### List all resource in Azure
```bash
az resource list
```
