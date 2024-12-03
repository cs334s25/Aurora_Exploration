#!/bin/bash

# Prompt user for their IP address
read -p "Enter your IP address (CIDR format, e.g., 203.0.113.0/32): " user_ip

# Validate the IP address format (basic check)
if [[ ! $user_ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$ ]]; then
    echo "Invalid IP address format. Please use CIDR notation (e.g., 203.0.113.0/32)."
    exit 1
fi

# Allow access from the provided IP address
echo "Updating security group to allow access from $user_ip..."
aws ec2 authorize-security-group-ingress \
    --group-id "sg-008cb891a8397f5d2" \
    --protocol tcp \
    --port 5432 \
    --cidr "$user_ip"

if [ $? -eq 0 ]; then
    echo "Access successfully granted to $user_ip on port 5432."
else
    echo "Failed to update the security group. Check your AWS CLI configuration and permissions."
    exit 1
fi
