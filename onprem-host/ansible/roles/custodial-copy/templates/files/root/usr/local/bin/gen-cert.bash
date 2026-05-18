#!/bin/bash -e

### {{ ansible_managed }}

# usage - cd to the directory you want the certs in, then call script with the
# purpose after _ e.g.
#   ./gen-cert my-purpose-here
# certificate will be generated in "current" subdirectory with CN of
# "username@domain purpose" unless overridden with arg 2
# new key will be generated every time

if ! [[ $1 ]]
then
  echo >&2 "missing purpose argument"
  exit 11
fi

cn="$USER@${HOSTNAME#*.} $1"
[[ $2 ]] && cn=$2
timestamp=$(date +%F-%T)
dir="$timestamp"

mkdir "$dir"
chmod o-rwx,g+rx-w "$dir"
cd "$dir" || exit 12

cat >"$cn.cfg" <<EOF
  [req]
    default_bits = 2048
    prompt = no
    default_md = sha256
    distinguished_name = dn
  [dn]
    C = GB
    O = The National Archives
    OU = Digital Archiving
    CN = $cn
  [v3_ext]
    basicConstraints = CA:FALSE
    keyUsage = digitalSignature
    extendedKeyUsage = clientAuth
EOF

openssl genrsa -out "$cn.key" 4096
openssl req -new -key "$cn.key" -out "$cn.csr" -config "$cn.cfg"
chmod g+r "$cn.key"
echo
echo "$dir/$cn.csr"
cat "$cn.csr"
ln -sf "${dir##*/}" ../current
cd - >/dev/null

