#!/usr/bin/env bash

DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -ex

export NATIVE=1

source ./sh/cflag.sh

cargo build $RUST_FEATURES --release --target $RUST_TARGET

name=$(grep "^name" Cargo.toml | sed 's/name = //g' | awk -F\" '{print $2}')

outdir=/opt/xxai.art/bin
sudo mkdir -p $outdir

sudo mv target/$RUST_TARGET/release/$name $outdir/$name
