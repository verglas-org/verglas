# Worklog

## 2026-08-08 — Issue #81

- Added the OpenFGA policy-engine adapter and tenant-qualified tuple translation.
- Added resource inheritance and action implication without storing canonical metadata in OpenFGA.

## 2026-08-09 — Issue #81

- Added the universal `connect` relation to the OpenFGA model and tuple translation. Database and service connection grants now evaluate through the same tenant-qualified policy path as all other actions.
