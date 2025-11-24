# Interoperability Test: Erlang Producer + Python Consumer (Key_Shared)

This test verifies interoperability between:
- **Erlang Producer**: Produces messages with configurable `batch_size` (supports both single message and batch formats)
- **Python Consumer**: Consumes messages using `Key_Shared` subscription strategy

## Prerequisites

1. **Pulsar Server**: Must be running and accessible
   ```bash
   # Using docker-compose (from project root)
   docker-compose up -d pulsar
   ```

2. **Python Pulsar Client**: Install the Python SDK
   ```bash
   pip install pulsar-client
   ```

3. **Erlang/OTP**: Required to run the Erlang producer script

## Files

- `interop_single_message_producer.escript` - Erlang script that produces messages with configurable `batch_size` (defaults to 1)
- `interop_key_shared_consumer.py` - Python script that consumes with Key_Shared strategy (multiple consumers)
- `run_interop_test.sh` - Test runner script that orchestrates producer + multiple consumers

## Usage

### Option 1: Run the complete multi-consumer test (recommended)

This test runs with **2 consumers** by default to verify Key_Shared dispatch consistency:

```bash
cd test/interop
./run_interop_test.sh
```

You can customize the number of consumers:

```bash
NUM_CONSUMERS=3 ./run_interop_test.sh
```

**What this test verifies:**
- ✅ Messages with the same key always go to the same consumer (Key_Shared consistency)
- ✅ Messages are distributed across consumers (load balancing)
- ✅ All messages are received across all consumers
- ❌ Test fails if all messages are consumed by a single consumer (when multiple consumers are configured)

**Example output:**
```
Consumer 1: 20 messages (key1: 10, key5: 10)
Consumer 2: 30 messages (key2: 10, key3: 10, key4: 10)
Total: 50/50 messages

✓ Key Consistency: Each key always goes to same consumer
✓ Load Distribution: Messages distributed across consumers
```

### Option 2: Run with custom parameters

You can customize test parameters:

```bash
cd test/interop
export PULSAR_HOST="pulsar://localhost:6650"
export TOPIC="persistent://public/default/interop-test"
export NUM_MESSAGES=100
export NUM_CONSUMERS=3
export BATCH_SIZE=2  # ⚠️ Warning: BATCH_SIZE > 1 is expected to fail (default: 1 for single message format)
./run_interop_test.sh
```

**Environment Variables:**
- `PULSAR_HOST` - Pulsar service URL (default: `pulsar://localhost:6650`)
- `TOPIC` - Topic name (default: `persistent://public/default/interop-test`)
- `NUM_MESSAGES` - Number of messages to produce (default: `50`)
- `NUM_CONSUMERS` - Number of consumers to start (default: `2`)
- `BATCH_SIZE` - Producer batch size (default: `1` for single message format)
  - **⚠️ Note**: If `BATCH_SIZE > 1`, the test is expected to fail as batch messages may not distribute correctly across Key_Shared consumers
- `TIMEOUT` - Consumer timeout in seconds (default: `60`)
- `SUBSCRIPTION` - Subscription name (default: timestamp-based unique name)

## What the Test Verifies

1. **Message Formats**: The producer supports both formats:
   - `batch_size=1`: Uses single message format (no SingleMessageMetadata headers)
   - `batch_size>1`: Uses batch format (with SingleMessageMetadata headers)

2. **Key Distribution**: Messages are sent with different keys (`key1`, `key2`, `key3`, `key4`, `key5`) to test Key_Shared distribution

3. **Interoperability**: Python SDK can correctly consume messages produced by Erlang library in single message format (`batch_size=1`). Note: Batch format (`batch_size>1`) may not work correctly with Key_Shared consumers.

4. **Key_Shared Strategy**: Multiple Python consumers use Key_Shared subscription to verify:
   - **Consistency**: Messages with the same key always go to the same consumer
   - **Load Balancing**: Messages are distributed across consumers
   - **Completeness**: All messages are received across all consumers

## Avoiding Old Messages

The test uses two mechanisms to ensure it only consumes messages from the current test run:

1. **Unique Subscription Name**: By default, the test uses a timestamp-based subscription name (`interop-test-sub-<timestamp>`), ensuring each run uses a fresh subscription
2. **InitialPosition.Latest**: The consumer uses `InitialPosition.Latest`, which means it only consumes messages published AFTER the subscription is created

You can override the subscription name by setting the `SUBSCRIPTION` environment variable, but be aware that this may cause the consumer to receive old messages if the subscription already exists.

## Expected Output

### Successful Test (BATCH_SIZE=1)

The test should show:
- Producer successfully sends all messages
- Multiple consumers receive messages (distributed by key)
- **Key Consistency**: Each key always goes to the same consumer
- **Load Distribution**: Messages are distributed across consumers
- All messages are received and acknowledged successfully

Example:
```
Consumer 1: 20 messages (key1: 10, key5: 10)
Consumer 2: 30 messages (key2: 10, key3: 10, key4: 10)
Total: 50/50 messages

✓ Key Consistency: Each key always goes to same consumer
✓ Load Distribution: Messages distributed across consumers
```

### Failed Test (BATCH_SIZE > 1)

When `BATCH_SIZE > 1`, the test is expected to fail with:
```
❌ FAILED: All messages consumed by a single consumer (Consumer 1)
   Expected distribution across 2 consumers
   Unique keys received: 5
   Keys: ['key1', 'key2', 'key3', 'key4', 'key5']

   ❌ ERROR: Multiple unique keys should be distributed across consumers
   This indicates a problem with Key_Shared subscription routing
```

## Testing Different Batch Sizes

You can test different batch sizes by passing the `batch_size` parameter to the producer script:

```bash
# Test with single message format (batch_size=1) - Recommended
escript interop_single_message_producer.escript pulsar://localhost:6650 persistent://public/default/interop-test 50

# Test with batch format (batch_size=2) - ⚠️ Expected to fail
escript interop_single_message_producer.escript pulsar://localhost:6650 persistent://public/default/interop-test 50 2

# Test with larger batches (batch_size=5) - ⚠️ Expected to fail
escript interop_single_message_producer.escript pulsar://localhost:6650 persistent://public/default/interop-test 50 5
```

**⚠️ Important**: The default `batch_size` is 1 (single message format). Tests with `BATCH_SIZE > 1` are expected to fail because batch messages may not distribute correctly across Key_Shared consumers, causing all messages to be consumed by a single consumer.

## Understanding Key_Shared and Batch Messages

**⚠️ Current Limitation**: With `BATCH_SIZE > 1`, this test is expected to fail. Batch messages may not distribute correctly across Key_Shared consumers, resulting in all messages being consumed by a single consumer.

There's a common misconception that Key_Shared subscription doesn't work with batch messages. While the Pulsar broker unpacks batches before routing messages to consumers, there may be implementation-specific issues that prevent proper distribution in this Erlang client.

**For reliable Key_Shared testing, use `BATCH_SIZE=1` (single message format).**

### Test Failure Conditions

The test will fail if:
- All messages are consumed by a single consumer when multiple consumers are configured (`NUM_CONSUMERS > 1`)
- This failure occurs regardless of the number of unique keys, as the test expects load distribution across consumers

## Troubleshooting

1. **Consumer doesn't receive messages**:
   - Check that Pulsar server is running
   - Verify topic name matches
   - Check subscription name is unique (or delete old subscription)

2. **Connection errors**:
   - Verify `PULSAR_HOST` is correct
   - Check network connectivity
   - Ensure Pulsar server is accessible

3. **Python import errors**:
   - Install pulsar-client: `pip install pulsar-client`

4. **Erlang script errors**:
   - Ensure pulsar application is compiled: `rebar3 compile`
   - Check Erlang/OTP version compatibility

5. **Test fails with "All messages consumed by a single consumer"**:
   - This is expected when `BATCH_SIZE > 1`
   - Use `BATCH_SIZE=1` for reliable Key_Shared testing
   - Verify that multiple consumers are actually running (check logs)
