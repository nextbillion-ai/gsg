#!/usr/bin/env bash

# error codes
# 0 - exited without problems
# 1 - OS or ARCH not supported by this script

set -e

# detect OS
OS="$(uname)"
case $OS in
Linux)
  OS='linux'
  ;;
Darwin)
  OS='darwin'
  ;;
*)
  echo 'OS not supported'
  exit 1
  ;;
esac

# detect ARCH
ARCH="$(uname -m)"
case "$ARCH" in
x86_64 | amd64)
  ARCH='amd64'
  ;;
aarch64 | arm64)
  ARCH='arm64'
  ;;
*)
  echo 'ARCH not supported'
  exit 1
  ;;
esac

# download
download_link="https://static.nextbillion.io/tools/gsg/latest/${OS}-${ARCH}/gsg?$(date +%s)"
echo downloading from "$download_link"
curl "$download_link" >gsg

# mounting to environment
case "$OS" in
'linux')
  downlod_path="/usr/bin/gsg"
  mv -f gsg "$downlod_path"
  chmod 755 "$downlod_path"
  chown root:root "$downlod_path"
  gsg version
  ;;
'darwin')
  downlod_path="$HOME/.gsg/bin/gsg"
  mkdir -p "$HOME/.gsg/bin"
  mv -f gsg "$downlod_path"
  chmod 755 "$downlod_path"
  printf "\nPlease add:\n"
  printf "export PATH=\"\$HOME/.gsg/bin:\$PATH\""
  printf "\ninto your bash profile, e.g., .bash_profile/.zshrc, etc.\n"
  ;;
*)
  echo 'OS not supported'
  exit 1
  ;;
esac

# display instructions of installation
printf "\nSuccessfully installed!\n"
printf 'Remember to set GOOGLE_APPLICATION_CREDENTIALS env to use gsg.\n\n'
exit 0
