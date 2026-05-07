#!/bin/bash

### {{ ansible_managed }}

set -x

systemctl --user start aws-login-for-podman
systemctl --user start iamra-update
systemctl --user start custodial-copy
systemctl --user start cc-reconciler.timer

# can't be enabled because generated :(
#systemctl --user enable iamra-update
#systemctl --user enable custodial-copy
#systemctl --user enable cc-reconciler.service
systemctl --user enable cc-reconciler.timer

sleep 3

podman ps -a
