#!/bin/bash

### {{ ansible_managed }}

set -x

systemctl --user stop iamra-update.service
systemctl --user stop custodial-copy.service
systemctl --user stop cc-reconciler.service

systemctl --user stop cc-reconciler.timer
systemctl --user stop cc-reconciler-stopper.timer
systemctl --user stop aws-login-for-podman.timer
systemctl --user stop podman-auto-update.timer

# can't be enabled or disabled because generated :(
#systemctl --user enable iamra-update.service
#systemctl --user enable custodial-copy.service

systemctl --user disable cc-reconciler.timer
systemctl --user disable cc-reconciler-stopper.timer
systemctl --user disable aws-login-for-podman.timer
systemctl --user disable podman-auto-update.timer

sleep 3

echo "if there's anything still running below you need to stop it with podman [pod] stop xxxxx"
podman pod ps
podman ps
