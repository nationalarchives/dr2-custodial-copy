#!/bin/bash

### {{ ansible_managed }}

set -x

systemctl --user stop iamra-update
systemctl --user stop custodial-copy
systemctl --user stop cc-reconciler.timer
systemctl --user stop cc-reconciler.service
systemctl --user start cc-reconciler-stopper.service

# can't be disabled because generated :(
#systemctl --user disable iamra-update
#systemctl --user disable custodial-copy
#systemctl --user disable cc-reconciler.service
systemctl --user disable cc-reconciler.timer

sleep 3

echo "if there's anything still running below you need to stop it with podman [pod] stop xxxxx"
podman pod ps
podman ps
