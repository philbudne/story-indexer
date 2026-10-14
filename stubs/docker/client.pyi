"""
just enough for mypy checking indexer/scripts/docker-stats.py
"""

class Service:
    name: str
    status: str
    attrs: dict[str, dict[str, int]]

class ServiceCollection:
    def list(self, status: bool) -> list[Service]: ...

class DockerClient:
    @classmethod
    def from_env(cls) -> DockerClient: ...
    @property
    def services(self) -> ServiceCollection: ...

from_env = DockerClient.from_env
