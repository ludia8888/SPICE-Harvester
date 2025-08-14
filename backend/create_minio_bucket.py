#!/usr/bin/env python3
"""
Create MinIO events bucket with correct credentials
"""

import boto3
from botocore.exceptions import ClientError
import sys

# Try different credential combinations
credentials = [
    {"access": "admin", "secret": "spice123!"},
    {"access": "minioadmin", "secret": "minioadmin123"},
    {"access": "minioadmin", "secret": "spice123!"},
    {"access": "admin", "secret": "minioadmin123"}
]

endpoint = "http://localhost:9000"
bucket_name = "events"

for cred in credentials:
    print(f"\nTrying: {cred['access']} / {'*' * len(cred['secret'])}")
    
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=cred['access'],
            aws_secret_access_key=cred['secret'],
            use_ssl=False,
            verify=False,
            region_name='us-east-1'
        )
        
        # Try to list buckets
        response = s3_client.list_buckets()
        print(f"✅ Connected! Found {len(response['Buckets'])} buckets")
        
        # List existing buckets
        for bucket in response['Buckets']:
            print(f"   - {bucket['Name']}")
        
        # Try to create events bucket
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Created '{bucket_name}' bucket!")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyOwnedByYou', 'BucketAlreadyExists']:
                print(f"✅ '{bucket_name}' bucket already exists")
            else:
                print(f"❌ Failed to create bucket: {error_code}")
        
        # Success - save working credentials
        print(f"\n🎉 WORKING CREDENTIALS:")
        print(f"   Access Key: {cred['access']}")
        print(f"   Secret Key: {cred['secret']}")
        sys.exit(0)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"❌ Failed: {error_code}")
        continue
    except Exception as e:
        print(f"❌ Error: {str(e)[:100]}")
        continue

print("\n❌ No working credentials found!")