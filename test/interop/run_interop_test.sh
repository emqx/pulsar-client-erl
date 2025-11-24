#!/bin/bash
# Interoperability test script
# Tests message production (Erlang) with Key_Shared consumption (Python)
# Uses multiple consumers to verify Key_Shared dispatch consistency
# Defaults to batch_size=1 (single message format) for backward compatibility

set -e

# Default values
PULSAR_HOST="${PULSAR_HOST:-pulsar://localhost:6650}"
TOPIC="${TOPIC:-persistent://public/default/interop-test}"
# Use timestamp-based subscription name to avoid consuming old messages from previous runs
SUBSCRIPTION="${SUBSCRIPTION:-interop-test-sub-$(date +%s)}"
NUM_MESSAGES="${NUM_MESSAGES:-50}"
NUM_CONSUMERS="${NUM_CONSUMERS:-2}"
TIMEOUT="${TIMEOUT:-60}"
BATCH_SIZE="${BATCH_SIZE:-1}"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Determine format description based on batch_size
if [ "$BATCH_SIZE" = "1" ]; then
    FORMAT_DESC="single message format"
else
    FORMAT_DESC="batch format with SingleMessageMetadata"
fi

echo "=========================================="
echo "Interoperability Test (Multi-Consumer)"
echo "=========================================="
echo "Erlang Producer (batch_size=$BATCH_SIZE, $FORMAT_DESC)"
echo "  -> Python Consumers (Key_Shared, $NUM_CONSUMERS consumers)"
echo ""
echo "Configuration:"
echo "  Pulsar Host: $PULSAR_HOST"
echo "  Topic: $TOPIC"
echo "  Subscription: $SUBSCRIPTION"
echo "  Messages: $NUM_MESSAGES"
echo "  Batch Size: $BATCH_SIZE"
echo "  Consumers: $NUM_CONSUMERS"
echo "=========================================="
echo ""

# Check if Python pulsar-client is installed
if ! python3 -c "import pulsar" 2>/dev/null; then
    echo -e "${RED}Error: Python pulsar-client not installed${NC}"
    echo "Install it with: pip install pulsar-client"
    exit 1
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Check if Erlang script exists
ERL_SCRIPT="$SCRIPT_DIR/interop_single_message_producer.erl"
if [ ! -f "$ERL_SCRIPT" ]; then
    echo -e "${RED}Error: Erlang script not found: $ERL_SCRIPT${NC}"
    exit 1
fi

# Check if Python multi-consumer script exists
PYTHON_SCRIPT="$SCRIPT_DIR/interop_key_shared_consumer.py"
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo -e "${RED}Error: Python script not found: $PYTHON_SCRIPT${NC}"
    exit 1
fi

# Make scripts executable
chmod +x "$ERL_SCRIPT" "$PYTHON_SCRIPT"

echo -e "${YELLOW}Step 1: Starting $NUM_CONSUMERS Python consumers in background...${NC}"
echo "  Note: Consumers use InitialPosition.Latest to only consume new messages"
echo "  Consumers will verify Key_Shared dispatch consistency"
python3 "$PYTHON_SCRIPT" "$PULSAR_HOST" "$TOPIC" "$SUBSCRIPTION" "$NUM_MESSAGES" \
    --num-consumers "$NUM_CONSUMERS" --timeout "$TIMEOUT" &
CONSUMER_PID=$!

# Give consumers time to connect and create subscription
# Important: Producer must start AFTER consumers create subscription with Latest position
sleep 4

echo ""
echo -e "${YELLOW}Step 2: Starting Erlang producer (batch_size=$BATCH_SIZE)...${NC}"
if escript "$ERL_SCRIPT" "$PULSAR_HOST" "$TOPIC" "$NUM_MESSAGES" "$BATCH_SIZE"; then
    echo -e "${GREEN}Producer completed successfully${NC}"
else
    echo -e "${RED}Producer failed${NC}"
    kill $CONSUMER_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo -e "${YELLOW}Step 3: Waiting for consumers to finish...${NC}"
wait $CONSUMER_PID
CONSUMER_EXIT=$?

if [ $CONSUMER_EXIT -eq 0 ]; then
    echo ""
    echo -e "${GREEN}=========================================="
    echo "Test PASSED"
    echo "==========================================${NC}"
    echo ""
    echo "Key_Shared verification:"
    echo "  ✓ Messages with same key always go to same consumer"
    echo "  ✓ Messages are distributed across consumers"
    echo "  ✓ All messages received"
    exit 0
else
    echo ""
    echo -e "${RED}=========================================="
    echo "Test FAILED"
    echo "==========================================${NC}"
    exit 1
fi
