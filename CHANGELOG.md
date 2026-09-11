# Changelog

All notable changes to this library are documented here.

## [Unreleased]

### Changed

- `NewWriter` sets `RequiredAcks` to `RequireOne`, instead of leaving the
  kafka-go zero value `RequireNone` (acks=0), where the broker response is never
  read and messages lost during a partition leader change are reported as
  written. Producers now see acknowledgement latency, and `WriteMessages`
  returns errors that used to be swallowed; callers with tight context deadlines
  may start seeing `context.DeadlineExceeded`. Set `RequiredAcks` on the returned
  writer for a different trade-off, see "Write acknowledgements" in the README.

### Fixed

- `AlterTransport` and `AlterDialer` report a hook that fails or returns nil.
  A nil result used to reach the writer, reader config or client, and the failure
  surfaced later, at the first write.
