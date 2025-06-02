# idemgotency-dynamodb
idempotency library for go with Dynamodb

# Pre-requisite
- Create a config.yaml file like the template
- Create tables with idempotency_db.json

# How to use
- First, initialize idempotency instance with the path of the input config file
- Execute the idempotent operation with ExecuteExactlyOnce function
- If the lock time of operation is not defined in config.yaml, it will be the default lock time