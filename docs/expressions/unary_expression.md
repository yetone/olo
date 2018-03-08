# UnaryExpression

MySQL 中的一元表达式

实现了 [SQLLiteralInterface][SQLLiteralInterface]

## 实例方法

### `__init__`

函数签名: `def __init__(self, value, operator):`

参数:

* `value`: [SQLLiteralInterface][SQLLiteralInterface]; 代表值，
* `operator`: str; 代表操作符

用法:

```python
exp = UnaryExpression(Dummy.id, 'DESC')
assert exp.get_sql_and_params()  == ('`id` DESC', [])

exp = UnaryExpression(
    BinaryExpression(Dummy.age, Dummy.id, '-'),
    'DESC'
)
assert exp.get_sql_and_params() == ('(`age` - `id`) DESC', [])
```

  [SQLLiteralInterface]: /interfaces/sql_literal_interface.md
