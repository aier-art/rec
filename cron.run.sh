#!/usr/bin/env bash

DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR

source $DIR/env.sh
set -ex
exec timeout 1h /opt/xxai.art/bin/rec
