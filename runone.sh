#!/bin/bash
export RUST_LOG=raft
jq "keys | .[$1]" -r cluster.json | xargs -n 1 -- target/debug/raft
