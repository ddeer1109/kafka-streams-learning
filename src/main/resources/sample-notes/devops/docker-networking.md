# Docker Bridge Networking

tags: docker, networking, containers

Docker creates a virtual bridge network (docker0) by default. Containers on the same bridge can communicate by container name (DNS resolution).

Key commands:
- `docker network create mynet` — create custom bridge
- `docker run --network mynet` — attach container to network
- `docker network inspect mynet` — see connected containers

Custom bridges are preferred over the default bridge because they provide automatic DNS resolution between containers.
