# Worklog

- #1: Added the Lakekeeper CloudEvent sink that delivers committed table mutations directly to every Verglas cache member. The deployment is required to provide tenant-network endpoints and a bearer, so the integration cannot silently fall back to polling.
