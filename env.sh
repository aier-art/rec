#!/usr/bin/env bash

set -e
env_sh() {
  local nowdir=$(pwd)
  cd "$(dirname $(realpath ${BASH_SOURCE[0]}))"/../conf/conn
  local i
  for i in $@; do
    set -o allexport
    source "$i".sh
    set +o allexport
  done
  cd $nowdir
  unset -f env_sh
}

env_sh host ak gt apg kv qdrant
export RUST_BACKTRACE=short
export RUST_LOG=debug,watchexec=off,watchexec_cli=off,globset=warn,h2=warn,tower=warn,xxai_tokio_postgres=warn
