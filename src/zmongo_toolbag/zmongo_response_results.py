from typing import List, Optional, Union, Dict
from pymongo.cursor import Cursor

from zmongo_toolbag.safe_result import SafeResult


class ZMongoResponseResults:
    """Wraps Cursors/Lists to guarantee outputs are SafeResult objects."""
    def __init__(self, raw_data: Union[List[Dict], Cursor]):
        self._data = raw_data
        self._realized_list = None

    def _get_list(self) -> List[SafeResult]:
        if self._realized_list is None:
            self._realized_list = [SafeResult(doc) for doc in self._data]
        return self._realized_list

    def __iter__(self):
        return iter(self._get_list())

    def __len__(self):
        return len(self._get_list())

    def __getitem__(self, index: int) -> SafeResult:
        return self._get_list()[index]

    def first(self) -> Optional[SafeResult]:
        lst = self._get_list()
        return lst[0] if lst else None
