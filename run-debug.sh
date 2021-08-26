#!/bin/bash
export RUST_LOG=trace
cargo build && jq 'keys | .[]' -r cluster.json | xargs -n 1 -P 0 -- kitty --detach target/debug/raft
