from olo.libs.compiler.translators.ast_translator import PythonTranslator


class QueryTranslator(PythonTranslator):

    def postGenExpr(self, node):
        return node.code.src

    def postGenExprInner(self, node):
        qual_src = [qual.src for qual in node.quals]
        if qual_src:
            qual_src[-1] = qual_src[-1].replace('.flat_map', '.map')
        return ''.join(qual_src) + node.expr.src + ')' * len(node.quals)

    def postGenExprFor(self, node):
        src = node.iter.src
        src += ''.join('.cq.filter(lambda %s: %s)' % (
            node.assign.src, if_.test.src) for if_ in node.ifs)
        src += '.cq.flat_map(lambda %s: ' % node.assign.src
        return src

    def postLambda(self, node):
        src = 'lambda %s: %s' % (','.join(node.argnames), node.code.src)
        return src


def ast2src(tree):
    QueryTranslator(tree)
    return tree.src
