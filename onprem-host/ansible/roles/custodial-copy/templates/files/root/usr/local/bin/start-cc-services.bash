#!/bin/bash

### {{ ansible_managed }}

set -x

systemctl --user start aws-login-for-podman.service
systemctl --user start podman-auto-update.service
systemctl --user start iamra-update.service
systemctl --user start custodial-copy.service

systemctl --user start cc-reconciler.timer
systemctl --user start cc-reconciler-stopper.timer
systemctl --user start aws-login-for-podman.timer
systemctl --user start podman-auto-update.timer

# can't be enabled or disabled because generated :(
#systemctl --user enable iamra-update.service
#systemctl --user enable custodial-copy.service

systemctl --user enable cc-reconciler.timer
systemctl --user enable cc-reconciler-stopper.timer
systemctl --user enable aws-login-for-podman.timer
systemctl --user enable podman-auto-update.timer

sleep 3

podman ps -a
