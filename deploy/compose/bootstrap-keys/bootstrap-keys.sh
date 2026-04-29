#!/bin/sh
set -eu

state_dir="${1:-/workspace/.local}"
private_key="$state_dir/bootstrap_private.pem"
public_key="$state_dir/bootstrap_public.pem"

mkdir -p "$state_dir"

if [ -f "$private_key" ] && [ -f "$public_key" ]; then
  echo "bootstrap keypair already present in $state_dir"
  exit 0
fi

tmp_private="$(mktemp "$state_dir/bootstrap_private.pem.tmp.XXXXXX")"
tmp_public="$(mktemp "$state_dir/bootstrap_public.pem.tmp.XXXXXX")"

cleanup() {
  rm -f "$tmp_private" "$tmp_public"
}
trap cleanup EXIT INT TERM

umask 077
openssl genrsa -out "$tmp_private" 2048 >/dev/null 2>&1
openssl rsa -in "$tmp_private" -pubout -out "$tmp_public" >/dev/null 2>&1
chmod 600 "$tmp_private"
chmod 644 "$tmp_public"
mv "$tmp_private" "$private_key"
mv "$tmp_public" "$public_key"
trap - EXIT INT TERM

echo "generated bootstrap keypair in $state_dir"
