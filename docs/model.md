# Model

`Model` 类是一切的开始，是一切 model class 的基类。

用法可参照「快速开始」里的 [例子][example]

## 类属性

### `__table_name__`

表名, 一般不需要用户自己指定, OLO 会根据 class_name 自动推导出来:

```
class FooBar(BaseModel):
    pass
    
assert FooBar.__table_name__ == 'foo_bar'
```

### query

返回 [Query][query] 对象, 只能通过 [Options.query_class][query_class] 设置

### cache

返回 [CacheWrapper][cache_wrapper] 对象, 只能通过 [Options.cache_class][cache_class] 设置

## 类方法

### create

创建一个对象

函数签名: `def create(cls, **attrs):`

参数:

* `attrs`: {str: object}; 字段名值映射

返回值:

    Model; 创建完的 model class 实例

用法:

```python
dummy = Dummy.create(name='foo', age=12)
```

### get

通过 `id` 获得一个对象

函数签名: `def get(cls, id=None, **kwargs):`

参数:

* `id`: object; `id`

返回值:

    Model; 此 model class 的实例

用法:

```python
dummy = Dummy.get(1)
```

### gets

通过 `id` 列表批量获得多个对象

函数签名: `def gets(cls, idents, filter_none=True):`

参数:

* `idents`: [object]; `id` 列表
* `filter_none`: bool; 是否过滤掉 `None` 值, 默认是 `True`

返回值:

    [Model]; 此 model class 的实例的列表

用法:

```python
dummys = Dummy.gets([1, 2, 3])
```

### get_by

通过查询条件获得一个对象

函数签名: `def get_by(cls, *expressions, **expression_dict):`

参数:

* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    Model; 此 model class 的实例

用法:

```python
dummy = Dummy.get_by(age=12)
```

参数可以传 `expression` 列表来进行复杂的查询:

```python
dummy = Dummy.get_by(Dummy.age > 12, name='foo')
```

### gets_by

通过查询条件批量获得多个对象

函数签名: `def gets_by(cls, *expressions, **expression_dict):`

参数:

* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    [Model]; 此 model class 的实例的列表

用法:

```python
dummys = Dummy.gets_by(age=12)
```

参数可以传 `expression` 列表来进行复杂的查询:

```python
dummys = Dummy.gets_by(Dummy.age > 12, name='foo')
```

### get_entities_by

通过查询条件批量获得某些字段

函数签名: `def get_entities_by(cls, entities, *expressions, **expression_dict)`

参数:

* `entities`: [str]; 字段名列表
* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    [object]; 字段列表

用法:

```python
res = Dummy.get_entities_by(['id', 'name'], age=3)  # [(1, 'foo'), (2, 'bar')]
```

### count_by

获得符合此查询条件的数量

函数签名: `def count_by(cls, *expressions, **expression_dict):`

参数:

* `expressions`: [UnaryExpression | BinaryExpression]; `expression` 列表, 可为空
* `expression_dict`: {str: object}; 查询条件的键值对

返回值:

    int; 符合此查询条件的数量

用法:

```python
count = Dummy.count_by(age=12)
```

## 实例方法

### update

更新实例

函数签名: `def update(self, **attrs):`

参数:

* `attrs`: {str: object}; 字段名值映射

返回值:

    bool; `True` 为更新成功, `False` 为更新失败

用法:

```python
dummy.update(name='foo', age=1)
```

### save

保存未入库的更新

model 实例的赋值操作在 save 之前都不会入库, 只会标记为 `dirty fields`

函数签名: `def save(self):`

返回值:

    bool; `True` 为更新成功, `False` 为更新失败

用法:

```python
dummy.name = 'foo'
dummy.age = 12
# 赋值未入库, dummy 的 dirty fields 为 {'name', 'age'}
dummy.save()
# 赋值已入库, dummy 的 dirty fields 为 {}
```

### delete

删除 model 实例

函数签名: `def delete(self):`

返回值:

    bool; `True` 为删除成功, `False` 为删除失败

用法:

```python
dummy.delete()
```

### to_dict

导出 `dict`

函数签名:

```python
def to_dict(self, excludes=None, parsers=None,
            type_parsers=None, jsonize=False):
```

参数:

* `excludes`: set | list | tuple | None; 需要忽略的属性名
* `parsers`: {str: function} | None; 属性名到转换函数的映射

```python
dummy = Dummy(name='foo', age=1)
dct = dummy.to_dict(parsers={'age': lambda x: x + 1})
assert dct == {'name': 'foo', 'age': 2}
```

* `type_parsers`: {type: function} | None; 属性类型到转换函数的映射

```python
dummy = Dummy(id=1, name='foo', age=2)
dct = dummy.to_dict(type_parsers={int: str})
assert dct == {'id': '1', 'name': 'foo', 'age': '2'}
```

* `jsonize`: bool; 是否要转成可被 `json.dumps` 的 `dict`

    默认会把 `datetime` 转成格式为 `%Y-%m-%d %H:%M:%S` 的字符串

返回值:

    `dict`

用法:

```python
dct = dummy.to_dict()
```

## Hooks

hooks 的用法已经在 [快速入门][hooks] 介绍完毕了

  [hooks]: quickstart.md#hooks
  [query]: query.md
  [cache_wrapper]: cache_wrapper.md
  [query_class]: model_options.md#query_class
  [cache_class]: model_options.md#cache_class
  [example]: quickstart.md#_2
