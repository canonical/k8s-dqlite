## AWS Performance Test

Run test:

```
cd test/performance/aws   
source bin/activate  
pip3 install -r requirements.txt
```

### Prerequisites

Export the AWS credentials:

```
[my-profile]
region=us-east-1
aws_access_key_id=replace-with-your-access-key-id
aws_secret_access_key=replace-with-your-secret-access-key
aws_session_token=replace-with-your-session-token
```

```
export AWS_PROFILE=my-profile
```

Stand up the infrastructure:

```
python3 three_nodes.py
```
