## AWS Performance Test

Run test:

```
cd test/performance/aws   
source bin/activate  
pip3 install -r requirements.txt
```

### Prerequisites

Export the AWS credentials in the environment:

```
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=your_default_region
export AWS_SESSION_TOKEN=your_session_token 

```

Stand up the infrastructure:

```
python3 three_nodes.py
```
