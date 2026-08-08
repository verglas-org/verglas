# PREFLIGHT — local server notes

Operator notes for running a self-hosted `verglas-server` on this machine. The
server ships as a Docker container (see `Dockerfile` / `docker-compose.yml`).
There is no launchd/systemd install path.

```
cd ~/code/verglas
# Edit deploy/docker/verglas.toml + credentials, then:
docker compose up -d --build
export VERGLAS_ENDPOINT=http://127.0.0.1:8334
verglas status
```
