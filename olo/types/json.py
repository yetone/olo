from typing import Any, Dict, Union, Sequence, overload
from typing_extensions import Protocol


class _JSONArray(Protocol):
    def __getitem__(self, idx: int) -> 'JSONLike': ...

    # hack to enforce an actual list
    def sort(self) -> None: ...


class _JSONDict(Protocol):
    def __getitem__(self, key: str) -> 'JSONLike': ...

    # hack to enforce an actual dict
    @staticmethod
    @overload
    def fromkeys(seq: Sequence[Any]) -> Dict[Any, Any]: ...

    @staticmethod
    @overload
    def fromkeys(seq: Sequence[Any], value: Any) -> Dict[Any, Any]: ...  # pylint: disable=function-redefined


JSONLike = Union[str, int, float, bool, None, _JSONArray, _JSONDict]
