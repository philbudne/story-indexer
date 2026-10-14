from typing import Any, TypedDict

class _Binding(TypedDict):
    source: str
    vhost: str
    destination: str
    destination_type: str
    routing_key: str
    arguments: dict

class _Defns(TypedDict):
    bindings: list[_Binding]

class AdminAPI:
    def __init__(self, url: str, auth: tuple[str, str]) -> None: ...
    def get_definitions(self) -> _Defns: ...
    def list_exchanges(self) -> list[Any]: ...
    def list_nodes(self) -> list[Any]: ...
    def _api_get(self, path: str) -> list[Any]: ...
