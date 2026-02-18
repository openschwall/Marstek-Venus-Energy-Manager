# Changelog

## [1.0.1] - 2026-02-18

### Changed
- Remove V3-exclusive entity definitions to match V2 register footprint.
- Deleted 20 entity definitions from the V3 definition lists (sensors, binary sensors, selects, buttons) that had no equivalent in V2.
- This reduces V3 Modbus-polled registers from ~38 to ~22, which should significantly cut options flow reload time for V3 users.
