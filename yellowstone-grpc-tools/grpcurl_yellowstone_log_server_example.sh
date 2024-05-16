#!/usr/bin/bash


grpcurl -plaintext -import-path . -proto yellowstone-log.proto \
    -d '{"initial_offset_policy": 0, "event_subscription_policy": 0 }' \
    '127.0.0.1:10001' yellowstone.log.YellowstoneLog.Consume