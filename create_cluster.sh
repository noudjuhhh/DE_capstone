aws redshift create-cluster --cluster-identifier dwh --cluster-type multi-node --node-type dc2.large --number-of-nodes 4 --db-name dwh --master-username dwhuser --master-user-password Passw0rd --region us-east-1