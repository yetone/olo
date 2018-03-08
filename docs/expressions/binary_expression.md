# BinaryExpression

MySQL 中的二元表达式

实现了 [SQLLiteralInterface][SQLLiteralInterface]

## 实例方法

### `__init__`

函数签名: `def __init__(self, left, right, operator):`

参数:

* `left`: [SQLLiteralInterface][SQLLiteralInterface]; 左值
* `right`: object | [SQLLiteralInterface][SQLLiteralInterface]; 右值
* `operator`: str; 操作符

用法:

```python
exp = BinaryExpression(Dummy.age, 1, '>')
assert exp.get_sql_and_params() == ('`age` > %s', [1])

exp = BinaryExpression(Dummy.age, Dummy.id, '>')
assert exp.get_sql_and_params() == ('`age` > `id`', [])
```

  [SQLLiteralInterface]: /interfaces/sql_literal_interface.md
