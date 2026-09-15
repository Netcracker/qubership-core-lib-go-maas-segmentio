# Changelog

All notable changes to this library are documented here.

## [Unreleased]

### Added

- `WriterOptions.RequiredAcks`, so the acknowledgement trade-off can be chosen
  where the writer is built rather than assigned to the writer afterwards. Left
  nil the default is unchanged: kafka-go's `RequireNone` (acks=0), where the
  broker response is never read and messages lost during a partition leader
  change are reported as written. That is the right choice for telemetry and the
  wrong one for anything a reader reconciles against — see "Write
  acknowledgements" in the README, which now states what each level costs.

### Fixed

- `AlterTransport` and `AlterDialer` report a hook that fails or returns nil.
  A nil result used to reach the writer, reader config or client, and the failure
  surfaced later, at the first write.
