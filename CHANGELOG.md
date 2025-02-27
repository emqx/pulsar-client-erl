# 2.1.0

- Added `drop_if_high_mem` config for producer.  With this enabled, when buffer is memory
  only and system memory is under high load, buffered data will be more aggressively
  dropped to try and avoid OOM kill.
- Added `max_inflight` configuration for producer.

# 2.0.1

- Change packet framing from 'packet-raw' to 'packet-4'.
  This also fixes the inflight state leak in producer process.

# 2.0.0

- Fixed handling of `Redirect` lookup type in `CommandLookupTopicResponse`.  Previously,
  the broker service URL that was returned was interpreted by the producer as the broker
  to connect to instead of the client re-issuing the lookup command to the new broker
  endpoint.  Several `pulsar_client` APIs were refactored to accomodate this fix.

# 1.0.0

- Removed support for hot upgrades.
- Added `telemetry` support.

# 0.8.5

## Bug fixes

- Fixed an issue where `pulsar_producers` would stop and not restart if it encountered
  problems when trying to reach `pulsar_client`.

# 0.8.4

## Bug fixes

- Fixed an issue where a producer process might've end up stuck with a closed connection
  while it believed to be in the `connected` state.

# 0.8.3

## Bug fixes

- Fixed `case_clause` error in `pulsar_producer` that could be caused
  by a connection failure to the Pulsar brokers depending on timing
  and the precise state of the state machine.

# 0.8.2

## Enhancements

- Made `pulsar_producers:all_connected/1` more performant when
  producers are under heavy throughput.
- Made `pulsar_client` fail fast when an authentication error is
  detected.

# 0.8.1

## Bug fixes

- Fixed `pulsar_{producers,consumers}:all_connected/1` to return
  `false` when there are no producer/consumer pids registered.
  [#56](https://github.com/emqx/pulsar-client-erl/pull/56)

# 0.8.0

## Enhancements

- When producing messages asynchronously, the PID of the producer that
  was called is now returned, so it can be monitored if necessary.
- Added support for specifying per-request callback functions to be
  called when the send receipt is received from Pulsar.
- Added support for binary URLs.  Previously, only Erlang strings
  (lists) were allowed.
- Added support for checking if all consumers or producers are in the
  connected state.  Since connection is asynchronous, it might be
  useful to use this API for health checking.

## Bug fixes

- Fixed `pulsar_producers:clientid/0` typespec.  It was defined as a
  `binary()`, but the clientid is actually an `atom()`.

# 0.7.0

## Enhancements

- Requires OTP 24 or higher.
- Added [`replayq`](https://github.com/emqx/replayq) to enable
  buffering of messages while the connection to Pulsar is unavailable.
  Messages have an optional retention period after which they'll be
  deemed expired and discarded.  Default retention period is
  `infinity` (does not expire) any messages.
- Producer is more sturdy: it'll tolerate more failures without being
  killed, so that messages will be enqueued to `replayq`.
- The producer callback will either be called with the first argument as `{ok, SendReceipt}` when
  publishing is successful or `{error, expired}` if the payload has
  expired before it got sent to Pulsar.
- Special formatting has been added when printing the process state as
  to avoid printing secrets during crashes.

# 0.6.4 and earlier

Older version changes not tracked here.
