# CachedQuery

数据请求类 [Query][Query] 的缓存实现，接口跟 [Query][Query] 完全一样

## 实例方法

### filter

同 [Query.filter][query_filter]

用法:

```python
q = Dummy.cq.filter(Dummy.id > 1, age=2)
assert q.get_sql_and_params() == ('SELECT * FROM `Dummy` WHERE `id` > %s AND `age` = %s', [1, 2])
```

### on

同 [Query.on][query_on]

用法:

```python
q = Foo.cq.join(Bar).on(Foo.id == Bar.foo_id)
assert q.get_sql_and_params() == ('SELECT * FROM `Foo` ON `Foo`.`id` = `Bar`.`foo_id`', [])
```

### having

同 [Query.having][query_having]

用法:

```python
from olo import funcs

c = funcs.COUNT(1).alias('c')
q = Dummy.cq(Dummy.age, c).group_by(Dummy.age).having(c > 2)
assert q.get_sql_and_params() == ('SELECT `age`, COUNT(1) AS c FROM `Dummy` GROUP BY `age` HAVING c > %s', [2])
```

### offset

同 [Query.offset][query_offset]

用法:

```python
q = Dummy.cq.offset(1)
```

### limit

同 [Query.limit][query_limit]

用法:

```python
q = Dummy.cq.limit(1)
```

### order_by

同 [Query.order_by][query_order_by]

用法:

```python
q = Dummy.cq.order_by('id')
q = Dummy.cq.order_by(Dummy.id)
q = Dummy.cq.order_by(Dummy.id.desc())
q = Dummy.cq.order_by('id', '-age')
q = Dummy.cq.order_by(Dummy.id, Dummy.age.desc())
```

### group_by

同 [Query.group_by][query_group_by]

用法:

```python
q = Dummy.cq.group_by('id')
q = Dummy.cq.group_by(Dummy.id)
q = Dummy.cq.group_by('id', 'age')
q = Dummy.cq.group_by(Dummy.id, Dummy.age)
```

### join

同 [Query.join][query_join]

用法:

```python
q = Foo.cq.join(Dummy).filter(Foo.age == Dummy.age)
```

### left_join

同 [Query.left_join][query_left_join]

### right_join

同 [Query.right_join][query_right_join]

### first

同 [Query.first][query_first]

用法:

```python
dummy = Dummy.cq.filter(age=1).first()
```

### all

同 [Query.all][query_all]

用法:

```python
dummys = Dummy.cq.filter(age=1).all()
```

  [query]: query.md
  [query_filter]: query.md#filter
  [query_on]: query.md#on
  [query_having]: query.md#having
  [query_offset]: query.md#offset
  [query_limit]: query.md#limit
  [query_order_by]: query.md#order_by
  [query_group_by]: query.md#group_by
  [query_join]: query.md#join
  [query_left_join]: query.md#left_join
  [query_right_join]: query.md#right_join
  [query_first]: query.md#first
  [query_all]: query.md#all
