#!/usr/bin/bash


# Subscripte to account update only
grpcurl \
    -max-msg-sz 10000000 \
    -plaintext -import-path . \
    -proto yellowstone-log.proto \
    -d '{"initial_offset_policy": 0, "event_subscription_policy": 0 }' \
    '127.0.0.1:10001' yellowstone.log.YellowstoneLog.Consume


# Create a static consumer group
grpcurl \
    -plaintext -import-path . \
    -proto yellowstone-log.proto \
    -d '{"instance_id_list": ["a", "b"], "redundancy_instance_id_list": ["c", "d"] }' \
    '127.0.0.1:10001' yellowstone.log.YellowstoneLog.CreateStaticConsumerGroup