#!/bin/bash

### {{ ansible_managed }}

[[ "$2" ]] && . "$2"

aws_signing_helper "${1:-credential-process}" --certificate "$cert" --private-key "$key" --trust-anchor-arn "$anchor" --profile-arn "$profile" --role-arn "$role"

