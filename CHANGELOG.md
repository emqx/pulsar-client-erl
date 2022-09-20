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
- The producer callback will either return `{ok, SendReceipt}` when
  publishing is successful or `{error, expired}` if the payload has
  expired before it got sent to Pulsar.

# 0.6.4 and earlier

Older version changes not tracked here.
