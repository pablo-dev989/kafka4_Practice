#!/bin/bash

for i in {1..30}
do
    # Create a unique key and value for each message
    if (( i % 2 == 0 )); then
        key="user-A"
    else
        key="user-C"
    fi
    value="$key:, \"user performed a task\""

    echo "$value"
    # Pause for 500 milliseconds before sending the next message
    sleep 0.500

done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
    --topic user-clicks \
    --bootstrap-server kafka-1:29092 \
    --property "parse.key=true" \
    --property "key.separator=:"

echo "30 messages have been sent to the user-clicks topic."