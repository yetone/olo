# Query

数据请求类, 一切的数据库请求都是从这里发生的

实现了 [SQLLiteralInterface][SQLLiteralInterface]

这个类负责把 expressions 转成 SQL 并通过 `DataBase` 从数据库中取值，然后用 model class 包装成实例对象

## 实例方法

### filter

增加 `WHERE` 的查询条件

函数签名: `def filter(self, *expressions, **expression_dict):`

参数:

* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Dummy.query.filter(Dummy.id > 1, age=2)
assert q.get_sql_and_params() == ('SELECT * FROM `Dummy` WHERE `id` > %s AND `age` = %s', [1, 2])
```

### on

增加 `ON` 的查询条件, 用在 `join` 过的 `query` 下

函数签名: `def on(self, *expressions, **expression_dict):`

参数:

* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Foo.query.join(Bar).on(Foo.id == Bar.foo_id)
assert q.get_sql_and_params() == ('SELECT * FROM `Foo` ON `Foo`.`id` = `Bar`.`foo_id`', [])
```

### having

增加 `HAVING` 的查询条件

函数签名: `def having(self, *expressions, **expression_dict):`

参数:

* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    Query; 新的 `Query` 实例

用法:

```python
from olo import funcs

c = funcs.COUNT(1).alias('c')
q = Dummy.query(Dummy.age, c).group_by(Dummy.age).having(c > 2)
assert q.get_sql_and_params() == ('SELECT `age`, COUNT(1) AS c FROM `Dummy` GROUP BY `age` HAVING c > %s', [2])
```

### offset

增加 `OFFSET` 的查询条件

函数签名: `def offset(self, offset):`

参数:

* `offset`: int

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Dummy.query.offset(1)
```

### limit

增加 `LIMIT` 的查询条件

函数签名: `def limit(self, limit):`

参数:

* `limit`: int

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Dummy.query.limit(1)
```

### order_by

增加 `ORDER BY` 的查询条件

函数签名: `def order_by(self, *order_by):`

参数:

* `order_by`: [str | Field | UnaryExpression]

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Dummy.query.order_by('id')
q = Dummy.query.order_by(Dummy.id)
q = Dummy.query.order_by(Dummy.id.desc())
q = Dummy.query.order_by('id', '-age')
q = Dummy.query.order_by(Dummy.id, Dummy.age.desc())
```

### group_by

增加 `GROUP BY` 的查询条件

函数签名: `def group_by(self, *group_by):`

参数:

* `group_by`: [str | Field]

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Dummy.query.group_by('id')
q = Dummy.query.group_by(Dummy.id)
q = Dummy.query.group_by('id', 'age')
q = Dummy.query.group_by(Dummy.id, Dummy.age)
```

### join

增加 `JOIN` 的查询条件

函数签名: `def join(self, model_class):`

参数:

* `model_class`: Model; model class

返回值:

    Query; 新的 `Query` 实例

用法:

```python
q = Foo.query.join(Dummy).filter(Foo.age == Dummy.age)
```

### left_join

增加 `LEFT JOIN` 的查询条件

参数用法同 `join`

### right_join

增加 `RIGHT JOIN` 的查询条件

参数用法同 `join`

### first

获得一个 model class 对象

函数签名: `def first(self):`

返回值:

    Model; 此 model class 的实例


用法:

```python
dummy = Dummy.query.filter(age=1).first()
```

### all

获得所有 model class 对象

函数签名: `def all(self):`

返回值:

    [Model]; 此 model class 的实例的列表


用法:

```python
dummys = Dummy.query.filter(age=1).all()
```

  [SQLLiteralInterface]: /interfaces/sql_literal_interface.md
