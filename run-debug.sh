#!/bin/bash
export RUST_LOG=$1
cargo build && jq 'keys | .[]' -r cluster.json | xargs -n 1 -P 0 -- kitty --detach --hold target/debug/raft-server && kitty --detach --hold target/debug/dashboard
