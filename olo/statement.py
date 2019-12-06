from __future__ import annotations
from typing import TYPE_CHECKING, TypeVar, List

if TYPE_CHECKING:
    from olo import Field
from olo.interfaces import SQLASTInterface

T = TypeVar('T')


class Assignment(SQLASTInterface):
    def get_sql_ast(self) -> List:
        if isinstance(self.right, SQLASTInterface):
            right_sql_ast = self.right.get_sql_ast()
        else:
            right_sql_ast = ['VALUE', self.right]
        return [
            'BINARY_OPERATE',
            '=',
            [
                'QUOTE',
                self.left.name
            ],
            right_sql_ast,
        ]

    def __init__(self, left: Field, right: T):
        self.left = left
        self.right = right
