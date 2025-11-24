#!/usr/bin/env python3
"""
Interoperability test: Consume messages produced by Erlang library using Python SDK
with Key_Shared subscription strategy using multiple consumers.

This script runs multiple consumers with the same subscription name to verify:
1. Messages with the same key always go to the same consumer (consistency)
2. Messages are distributed across consumers (load balancing)
"""

import sys
import time
import argparse
import threading
from collections import defaultdict
from pulsar import Client, ConsumerType, InitialPosition


class ConsumerThread(threading.Thread):
    """Thread that runs a single consumer."""

    def __init__(self, consumer_id, pulsar_host, topic, subscription_name, num_messages, timeout_seconds, results):
        super().__init__()
        self.consumer_id = consumer_id
        self.pulsar_host = pulsar_host
        self.topic = topic
        self.subscription_name = subscription_name
        self.num_messages = num_messages
        self.timeout_seconds = timeout_seconds
        self.results = results
        self.received_messages = defaultdict(list)
        self.message_count = 0
        self.error = None

    def run(self):
        """Run the consumer."""
        client = Client(self.pulsar_host)
        consumer = None

        try:
            consumer = client.subscribe(
                topic=self.topic,
                subscription_name=self.subscription_name,
                consumer_type=ConsumerType.KeyShared,
                initial_position=InitialPosition.Latest
            )

            print(f"[Consumer {self.consumer_id}] Created and connected", flush=True)

            start_time = time.time()
            consecutive_timeouts = 0
            max_consecutive_timeouts = 5  # Stop after 5 consecutive timeouts
            last_message_time = start_time

            # Keep receiving messages until timeout or all messages received
            while True:
                try:
                    msg = consumer.receive(timeout_millis=2000)

                    # Reset timeout counter on successful receive
                    consecutive_timeouts = 0
                    last_message_time = time.time()

                    key = msg.partition_key() if msg.partition_key() else b"no-key"
                    value = msg.data()

                    self.received_messages[key].append({
                        'value': value,
                        'message_id': str(msg.message_id()),
                        'publish_time': msg.publish_timestamp()
                    })

                    self.message_count += 1
                    consumer.acknowledge(msg)

                    if self.message_count % 10 == 0:
                        print(f"[Consumer {self.consumer_id}] Received {self.message_count} messages", flush=True)

                    # Check overall timeout
                    if time.time() - start_time > self.timeout_seconds:
                        print(f"[Consumer {self.consumer_id}] Overall timeout reached: {self.message_count} messages", flush=True)
                        break

                except Exception as e:
                    if "timeout" in str(e).lower():
                        consecutive_timeouts += 1
                        # If we've had multiple consecutive timeouts, likely no more messages
                        if consecutive_timeouts >= max_consecutive_timeouts:
                            print(f"[Consumer {self.consumer_id}] No more messages (consecutive timeouts: {consecutive_timeouts}), received {self.message_count} total", flush=True)
                            break
                        continue
                    else:
                        self.error = str(e)
                        print(f"[Consumer {self.consumer_id}] Error: {e}", flush=True)
                        import traceback
                        traceback.print_exc()
                        break

            print(f"[Consumer {self.consumer_id}] Finished: received {self.message_count} messages", flush=True)

            # Store results
            self.results[self.consumer_id] = {
                'message_count': self.message_count,
                'messages_by_key': {k.decode() if isinstance(k, bytes) else k: len(v)
                                   for k, v in self.received_messages.items()},
                'all_keys': [k.decode() if isinstance(k, bytes) else k
                            for k in self.received_messages.keys()],
                'error': self.error
            }

        except Exception as e:
            self.error = str(e)
            self.results[self.consumer_id] = {
                'message_count': 0,
                'messages_by_key': {},
                'all_keys': [],
                'error': str(e)
            }
            print(f"[Consumer {self.consumer_id}] Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                if consumer:
                    consumer.close()
            except:
                pass
            client.close()


def verify_key_consistency(results, total_messages):
    """Verify that messages with the same key always go to the same consumer."""
    print("\n" + "="*60)
    print("Verifying Key_Shared Consistency")
    print("="*60)

    # Build a map: key -> consumer_id
    key_to_consumer = {}
    all_keys = set()

    for consumer_id, result in results.items():
        if result['error']:
            print(f"[Consumer {consumer_id}] Error: {result['error']}")
            return False

        for key in result['all_keys']:
            all_keys.add(key)
            if key in key_to_consumer:
                if key_to_consumer[key] != consumer_id:
                    print(f"\n❌ KEY CONSISTENCY VIOLATION!")
                    print(f"   Key '{key}' was received by both:")
                    print(f"   - Consumer {key_to_consumer[key]} (first)")
                    print(f"   - Consumer {consumer_id} (later)")
                    return False
            else:
                key_to_consumer[key] = consumer_id

    # Verify all messages for each key went to the same consumer
    print("\n✓ Key Consistency Check:")
    for key in sorted(all_keys):
        consumer_id = key_to_consumer.get(key, "unknown")
        total_for_key = sum(results[cid]['messages_by_key'].get(key, 0)
                           for cid in results.keys())
        print(f"   Key '{key}': All {total_for_key} messages → Consumer {consumer_id}")

    return True


def verify_load_distribution(results, num_consumers):
    """Verify that messages are distributed across consumers."""
    print("\n" + "="*60)
    print("Verifying Load Distribution")
    print("="*60)

    consumer_counts = {cid: results[cid]['message_count'] for cid in results.keys()}
    total_received = sum(consumer_counts.values())

    print(f"\nMessages received per consumer:")
    for consumer_id in sorted(consumer_counts.keys()):
        count = consumer_counts[consumer_id]
        percentage = (count / total_received * 100) if total_received > 0 else 0
        print(f"   Consumer {consumer_id}: {count} messages ({percentage:.1f}%)")

    # Check if messages are distributed (at least one consumer received messages)
    active_consumers = [cid for cid, count in consumer_counts.items() if count > 0]

    # Collect all unique keys received by each consumer
    all_keys_by_consumer = {}
    unique_keys_total = set()
    for consumer_id, result in results.items():
        keys = set(result.get('all_keys', []))
        all_keys_by_consumer[consumer_id] = keys
        unique_keys_total.update(keys)

    if len(active_consumers) == 0:
        print("\n❌ No consumers received any messages!")
        return False
    elif len(active_consumers) == 1 and num_consumers > 1:
        # All messages went to a single consumer - this is a failure
        print(f"\n❌ FAILED: All messages consumed by a single consumer (Consumer {active_consumers[0]})")
        print(f"   Expected distribution across {num_consumers} consumers")
        print(f"   Unique keys received: {len(unique_keys_total)}")
        if unique_keys_total:
            print(f"   Keys: {sorted(unique_keys_total)}")
        print("\n   Key distribution by consumer:")
        for consumer_id in sorted(all_keys_by_consumer.keys()):
            keys = all_keys_by_consumer[consumer_id]
            if keys:
                print(f"     Consumer {consumer_id}: {sorted(keys)}")
            else:
                print(f"     Consumer {consumer_id}: (no messages)")
        if len(unique_keys_total) > 1:
            print("\n   ❌ ERROR: Multiple unique keys should be distributed across consumers")
            print("   This indicates a problem with Key_Shared subscription routing")
        else:
            print("\n   ❌ ERROR: With multiple consumers, messages should be distributed")
            print("   This test expects multiple consumers to share the load")
        return False
    elif len(active_consumers) < num_consumers:
        print(f"\n⚠️  Only {len(active_consumers)} out of {num_consumers} consumers received messages")
        print(f"   Unique keys received: {len(unique_keys_total)}")
        if unique_keys_total:
            print(f"   Keys: {sorted(unique_keys_total)}")
        print("\n   Key distribution by consumer:")
        for consumer_id in sorted(all_keys_by_consumer.keys()):
            keys = all_keys_by_consumer[consumer_id]
            if keys:
                print(f"     Consumer {consumer_id}: {sorted(keys)}")
            else:
                print(f"     Consumer {consumer_id}: (no messages)")
        print("\n   Note: This is acceptable if:")
        print("     - There are fewer unique keys than consumers")
        print("     - All keys hash to the same consumer's hash range (rare but possible)")

    print(f"\n✓ Load Distribution: {len(active_consumers)} consumer(s) active")
    return True


def run_multi_consumer_test(pulsar_host, topic, subscription_name, num_messages,
                            num_consumers, timeout_seconds=60):
    """Run test with multiple consumers."""
    print("="*60)
    print("Key_Shared Multi-Consumer Test")
    print("="*60)
    print(f"Pulsar Host: {pulsar_host}")
    print(f"Topic: {topic}")
    print(f"Subscription: {subscription_name}")
    print(f"Expected Messages: {num_messages}")
    print(f"Number of Consumers: {num_consumers}")
    print(f"Strategy: Key_Shared")
    print("="*60)
    print()

    # Shared results dictionary
    results = {}

    # Start all consumers
    threads = []
    for i in range(num_consumers):
        thread = ConsumerThread(
            consumer_id=i+1,
            pulsar_host=pulsar_host,
            topic=topic,
            subscription_name=subscription_name,
            num_messages=num_messages,  # Each consumer tries to get all messages
            timeout_seconds=timeout_seconds,
            results=results
        )
        threads.append(thread)
        thread.start()
        time.sleep(0.5)  # Stagger consumer starts

    print(f"\nStarted {num_consumers} consumers, waiting for messages...\n")

    # Wait for all consumers to finish
    for thread in threads:
        thread.join()

    # Print summary
    print("\n" + "="*60)
    print("Consumer Summary")
    print("="*60)

    total_received = 0
    for consumer_id in sorted(results.keys()):
        result = results[consumer_id]
        total_received += result['message_count']
        print(f"\nConsumer {consumer_id}:")
        print(f"  Messages received: {result['message_count']}")
        if result['messages_by_key']:
            print(f"  Messages by key: {result['messages_by_key']}")
        if result['error']:
            print(f"  Error: {result['error']}")

    print(f"\nTotal messages received: {total_received}/{num_messages}")

    # Verify tests
    if total_received != num_messages:
        print(f"\n❌ Expected {num_messages} messages but received {total_received}")
        return False

    # Verify key consistency
    if not verify_key_consistency(results, num_messages):
        return False

    # Verify load distribution
    if not verify_load_distribution(results, num_consumers):
        return False

    print("\n" + "="*60)
    print("✅ ALL CHECKS PASSED")
    print("="*60)
    print("\nKey_Shared subscription verified:")
    print("  ✓ Messages with same key always go to same consumer")
    print("  ✓ Messages are distributed across consumers")
    print("  ✓ All messages received")

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Consume messages from Pulsar using Key_Shared subscription with multiple consumers'
    )
    parser.add_argument('pulsar_host', help='Pulsar service URL (e.g., pulsar://localhost:6650)')
    parser.add_argument('topic', help='Topic name (e.g., persistent://public/default/interop-test)')
    parser.add_argument('subscription_name', help='Subscription name')
    parser.add_argument('num_messages', type=int, help='Total number of messages to consume')
    parser.add_argument('--num-consumers', type=int, default=2,
                       help='Number of consumers to start (default: 2)')
    parser.add_argument('--timeout', type=int, default=60,
                       help='Timeout in seconds (default: 60)')

    args = parser.parse_args()

    success = run_multi_consumer_test(
        args.pulsar_host,
        args.topic,
        args.subscription_name,
        args.num_messages,
        args.num_consumers,
        args.timeout
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
