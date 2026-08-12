# Changelog

## Unreleased

* `Behaviour changes`
  - **`NewWriter` now sets `RequiredAcks: kafka.RequireOne`.** Previously the writer was built with
    the kafka-go zero value, `RequireNone` (acks=0): the broker response was never read, so messages
    lost during a partition leader change were reported as written. Producers will now see
    acknowledgement latency they did not have before, and `WriteMessages` will return errors that
    used to be swallowed. Callers with tight context deadlines may start seeing
    `context.DeadlineExceeded`.
    Set `writer.RequiredAcks` on the returned writer to choose a different trade-off; see
    "Write acknowledgements" in the README.
* `Features`
  - `AlterTransport` and `AlterDialer` now report a hook that fails or returns nil, instead of
    letting a nil value reach the writer, reader config or client.
