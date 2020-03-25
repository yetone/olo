from typing import List, Dict, Tuple, Iterable, Optional

from olo.compat import iteritems
from olo.context import context, table_alias_mapping_context, in_sql_translation_context
from olo.errors import ORMError
from olo.utils import car, cdr, is_sql_ast, friendly_repr

AST = List


MODIFY_OPERATES = frozenset({'INSERT', 'DELETE', 'UPDATE'})


def _detect_table_alias(sql_ast: AST, rev_alias_mapping: Optional[Dict[str, str]] = None) -> AST:
    if not is_sql_ast(sql_ast):
        return sql_ast

    rev_alias_mapping = {} if rev_alias_mapping is None else rev_alias_mapping

    if sql_ast[0] == 'TABLE':
        alias = sql_ast[1][0].lower()
        orig_alias = alias
        n = 0
        while alias in rev_alias_mapping:
            if rev_alias_mapping[alias] == sql_ast[1]:
                break
            n += 1
            alias = orig_alias + str(n)
        rev_alias_mapping[alias] = sql_ast[1]
        return ['ALIAS', sql_ast, alias]

    if sql_ast[0] == 'ALIAS' and len(sql_ast) >= 3:
        if is_sql_ast(sql_ast[1]) and sql_ast[1][0] == 'TABLE':
            rev_alias_mapping[sql_ast[2]] = sql_ast[1][1]
        return sql_ast

    if sql_ast[0] in MODIFY_OPERATES and len(sql_ast) >= 2:
        return sql_ast[:2] + [_detect_table_alias(x, rev_alias_mapping=rev_alias_mapping) for x in sql_ast[2:]]

    return [_detect_table_alias(x, rev_alias_mapping=rev_alias_mapping)
            for x in sql_ast]


def detect_table_alias(sql_ast: AST) -> Tuple[AST, Dict[str, str]]:
    rev_alias_mapping = {}
    sql_ast = _detect_table_alias(
        sql_ast,
        rev_alias_mapping=rev_alias_mapping
    )
    alias_mapping = {v: k for k, v in sorted(iteritems(rev_alias_mapping))}
    return sql_ast, alias_mapping


class SQLASTTranslator(object):
    def translate(self, sql_ast: AST) -> Tuple[str, List]:
        if not is_sql_ast(sql_ast):
            raise ORMError(f'{friendly_repr(sql_ast)} is not a valid sql ast!')

        alias_mapping = None
        if not context.in_sql_translation:
            sql_ast, alias_mapping = detect_table_alias(sql_ast)

        head = car(sql_ast)
        tail = cdr(sql_ast)
        method_name = 'post_{}'.format('_'.join(head.split(' ')))
        method = getattr(self, method_name, None)
        if method is None:
            raise NotImplementedError(method_name)

        with in_sql_translation_context():
            if alias_mapping:
                with table_alias_mapping_context(alias_mapping):
                    return method(*tail)  # pylint: disable=not-callable

            return method(*tail)  # pylint: disable=not-callable

    def reduce(self, args: Iterable[AST]) -> Tuple[List[str], List]:
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return sql_pieces, params
