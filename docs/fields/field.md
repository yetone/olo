# Field

字段类, 用来定义 MySQL 字段的类型等属性

实现了 [SQLLiteralInterface][SQLLiteralInterface]

用法:

```python
class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    name = Field(str)
```

## 实例方法

### `__init__`

函数签名: 

```python
def __init__(
    self, type,
    default=None, name=None,
    parser=None, deparser=None,
    primary_key=False, on_update=None,
    choices=None, encrypt=False,
    input=None, output=None,
    noneable=True, attr_name=None,
    version=None
):
```

参数:

1. `type`: object; 字段类型

    OLO 的字段是「强类型」的, 详情请看 [FieldType][field_type]

    也就意味着你在这里指定的字段类型一定是 model 实例的相应的属性类型,

    例如:

        class Foo(BaseModel):
            name = Field([str])  # 代表 name 字段是字符串列表类型的

        dummy = Dummy.create(name=['a', 'b'])

        dummy.name == ['a', 'b']

2. `default`: object; 字段的默认值, 默认为 `None`
3. `name`: str | None; 字段的名字

    对应数据库中字段的名字

    通常不用自己指定, 会自动生成

4. `parser`: function | None; 字段数据的 parser

    由于字段是「强类型」的且可以指定所有的类型, 

    有一些用户的自定义的类型需要用户自己指定 `parser`,

    例如: 

        def user_parser(v):
            if isinstance(v, User):
                return v
            if isinstance(v, int):
                return User.get(v)

        class Foo(BaseModel):
            user = Field(User, parser=user_parser)

        user = User.get(1)
        foo = Foo.create(user=user)
        foo = Foo.create(user=user.id)

5. `deparser`: function | None; 字段数据的 deparser

    由于字段是「强类型」的且可以指定所有的类型, 

    有一些用户的自定义的类型需要用户自己指定 `deparser`,

    例如: 

        def user_deparser(user):
            return user.id

        class Foo(BaseModel):
            user = Field(User, parser=user_parser,
                         deparser=user_deparser)

        user = User.get(1)
        foo = Foo.create(user=user)
        foo = Foo.create(user=user.id)

6. `primary_key`: bool; 是否是主键, 默认为 `False`
7. `on_update`: function | None; 实例更新时的回调函数

    此函数的返回值为此字段的值
    
    例如:
    
        class Foo(BaseModel):
            age = Field(int, on_update=lambda x: x.age + 1)
            name = Field(str)
        
        foo = Foo.create(age=1, name='foo')
        foo.update(name='bar')  # Foo.age.on_update called
        assert foo.age == 2

8. `choices`: [object] | None; 此字段的取值范围

    若此字段的值不在其范围, 将报错: `ValidationError`
    
    例如:
    
        class Foo(BaseModel):
            age = Field(int, choices=[1, 2, 3])
        
        Foo.create(age=4)  # will raise ValidationError
        foo = Foo.create(age=1)
        foo.update(age=4)  # will raise ValidationError

9. `encrypt`: bool; 是否加密, 默认为 `False`

    OLO 支持字段加密。有一些涉及用户隐私的字段比如: `password`, `phone_number` 等等可能需要在数据库里加密。

10. `input`: function | None; 入库时需要调用的函数

    数据库中最终储存的值是此函数的返回值。与 `deparser` 不同的是, `deparser` 进行的是类型的转换, 而此函数仅仅是数据的转换
    
    `类型跟数据是有区别的`

11. `output`: function | None; 绑定到实例对象时需要调用的函数

    实例对象上的相应属性值是此函数的返回值。与 `parser` 不同的是, `parser` 进行的是类型的转换, 而此函数仅仅是数据的转换
    
    `类型跟数据是有区别的`

12. `noneable`: bool; 是否可为 NULL, 默认为 `True`

13. `attr_name`: str | None; 在 model class 上的属性名
    
    通常情况下用户不需要指定, 会自动生成

14. `version`: int | None; 版本, 默认为 `None`

    在 `Field` 中, 此参数暂时无意义

### asc

返回值:
    
  [UnaryExpression][unary_expression]

用法:

```python
exp = Dummy.id.asc()  # `id` ASC
```

### desc

返回值:
    
  [UnaryExpression][unary_expression]

用法:

```python
exp = Dummy.id.desc()  # `id` DESC
```

### `__add__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id + Dummy.age  # `id` + `age`
exp = Dummy.id + 1  # `id` + 1
```

### `__sub__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id - Dummy.age  # `id` - `age`
exp = Dummy.id - 1  # `id` - 1
```

### `__mul__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id * Dummy.age  # `id` * `age`
exp = Dummy.id * 2  # `id` * 2
```

### `__div__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id / Dummy.age  # `id` / `age`
exp = Dummy.id / 2  # `id` / 2
```

### `__mod__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id % Dummy.age  # `id` % `age`
exp = Dummy.id % 2  # `id` % 2
```

### `__eq__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id == Dummy.age  # `id` = `age`
exp = Dummy.id == None  # `id` IS NULL
```

### `__ne__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id != Dummy.age  # `id` != `age`
exp = Dummy.id != None  # `id` IS NOT NULL
```

### `__gt__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id > Dummy.age  # `id` > `age`
exp = Dummy.id > 2  # `id` > 2
```

### `__ge__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id >= Dummy.age  # `id` >= `age`
exp = Dummy.id >= 2  # `id` >= 2
```

### `__lt__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id < Dummy.age  # `id` < `age`
exp = Dummy.id < 2  # `id` < 2
```

### `__le__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id <= Dummy.age  # `id` <= `age`
exp = Dummy.id <= 2  # `id` <= 2
```

### in_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id.in_([1, 2, 3])  # `id` IN (1, 2, 3)
```

### `__lshift__`

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id << [1, 2, 3]  # `id` IN (1, 2, 3)
```

### not_in_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id.not_in_([1, 2, 3])  # `id` NOT IN (1, 2, 3)
```

### like_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.name.like_('%foo%')  # `name` LIKE '%foo%'
```

### ilike_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.name.ilike_('%foo%')  # `name` ILIKE '%foo%'
```

### regexp_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.name.regexp_('\w+')  # `name` REGEXP '\w+'
```

### between_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.id.between_([1, 3])  # `id` BETWEEN 1, 3
```

### concat_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.name.concat_(Dummy.id)  # `name` || `id`
```

### is_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.name.is_(Dummy.id)  # `name` IS `id`
```

### is_not_

返回值:
    
  [BinaryExpression][binary_expression]

用法:

```python
exp = Dummy.name.is_not_(Dummy.id)  # `name` IS NOT `id`
```

  [SQLLiteralInterface]: /interfaces/sql_literal_interface.md
  [unary_expression]: /expressions/unary_expression.md
  [binary_expression]: /expressions/binary_expression.md
  [field_type]: /fields/field_type.md
