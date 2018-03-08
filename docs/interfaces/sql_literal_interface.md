# SQLLiteralInterface

可转成 SQL 字符串的接口

目前如下类实现了此接口:

* [Field][field]
* [UnaryExpression][unary_expression]
* [BinaryExpression][binary_expression]
* [Query][query]

## 接口方法

### `get_sql_and_params`

获得 SQL 字符串和参数

函数签名: `def get_sql_and_params(self):`

返回值:

(str, list); 第一个元素是 SQL 字符串，第二个元素是 SQL 参数

用法:

```python
class Dummy(BaseModel):
    age = Field(str)

# Field 实现了 SQLLiteralInterface
assert Dummy.age.get_sql_and_params() == ('`age`', [])

exp = Dummy.age.desc()
# UnaryExpression 实现了 SQLLiteralInterface
assert exp.get_sql_and_params() == ('`age` DESC', [])

exp = Dummy.age > 1
# BinaryExpression 实现了 SQLLiteralInterface
assert exp.get_sql_and_params() == ('`age` > %s', [1])
```

  [field]: /fields/field.md
  [unary_expression]: /expressions/unary_expression.md
  [binary_expression]: /expressions/binary_expression.md
  [query]: /query.md
