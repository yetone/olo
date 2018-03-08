# 快速入门

## 一个小例子

一个标准的模型(Model)看起来会是这样的：

```python
from olo import DataBase, Model, Field, DbField


db = DataBase(store, beansdb=beansdb.Client())


class BaseModel(Model):
    class Options:
        db = db
        cache_client = mc.Client()


class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    name = Field(str)
    age = Field(int, default=12, on_update=lambda x: x.age + 1)
    tags = Field([str], default=[])
    payload = Field(dict, default={})
    map = Field({int: str}, default={})
    flag = DbField(int, default=0)  # save in beansdb

    # DbField need get_uuid method!
    def get_uuid(self):
        return '/{}/{}'.format(
            self.__table_name__,
            self.id
        )
```

如上所示，通过 OLO 的声明式的 API 来定义一个模型是非常简洁的（起码我这样认为）

那么，这段代码做了什么？

1. 首先，我们创建了一个 `DataBase` 实例。

    `db` 对象是用来执行 SQL 的中间件。主要依靠 `sqlstore` 来进行数据库的操作。

2. 接下来，我们创建了一个类 `BaseModel` 继承了 [Model][model]，用来作为其他模型的基类。

    定义一个模型的基类，这算是定义模型的最佳实践。

3. 我们在 `BaseModel` 里创建了一个 [Options][options] 类。

    这个 [Options][options] 类是用来为模型提供配置参数的,
    [Options][options] 是可被继承的, 子类的 [Options][options] 定义是继承自夫类的 [Options][options]
    在这个例子里我们制定了 `db` 为我们刚刚实例化的 `DataBase`,
    `cache_client` 为 `mc.Client()`。
    像 `db` 是执行 `SQL` 操作的中间件一样，`cache_client` 是用来执行缓存操作的。

4. 然后，我们定义了 `Dummy` 模型类，让其继承自 `BaseModel`。
5. 最后我们通过 [Field][field] 类来定义其模型中的各种字段。

    其中 `DbField` 是指储存在 `beansdb` 里的字段，类似于豆瓣一直使用的 `PropsItem`，类似的，需要用这种类型的字段就需要提前定义好 `get_uuid` 的方法来方便生成独一无二的 `key` 。


## 增加

向数据库插入一条记录：

```python
dummy = Dummy.create(name='foo', age=11)
```

## 更改

更新数据库里的某一条记录：

```python
dummy.update(name='foo2')
```

或者

```python
dummy.name = 'foo2'
dummy.save()
```

更新后:

```python
assert dummy.name == 'foo2'
assert dummy.age == 12  # because of: on_update=lambda x: x.age + 1
```

## 删除

```python
dummy.delete()
```

## 查询

### SQLAlchemy 形式的查询

#### query base

获得一个:

```python
dummy = Dummy.query.filter(id=1).first()
```

获得全部:

```python
dummys = Dummy.query.filter(name='foo').all()
```

获得数量:
```python
dummys = Dummy.query.filter(name='foo').count()
```

#### 链式调用

```python
dummys = (
  Dummy.query
    .filter(Dummy.name.like_('%foo%'))
    .filter(Dummy.age >= 12)
    .offset(12)
    .limit(20)
    .all()
)
```

#### 复用 query

```python
query = Dummy.query.filter(age=12)

query1 = query.filter(Dummy.name.like_('%foo%'))
query2 = query.filter(Dummy.name.like_('%bar%'))

dummy1 = query1.first()
dummy2 = query2.first()
```

常用的情况可能是这样的:

```python
start = request.args.get('start', 0, type=int)
count = request.args.get('count', 20, type=int)
age = request.args.get('age', type=int)
name = request.args.get('name')

q = Dummy.query

if age is not None:
    q = q.filter(age=age)

if name is not None:
    q = q.filter(Dummy.name.like('%{}%'.format(name)))

total = q.count()
dummys = q.offset(start).limit(count).all()
```

#### 仅请求某几个字段

```python
Dummy.query('id').filter(age=12).all()
Dummy.query('id', 'age').filter(Dummy.age >= 12).all()
```

or

```python
Dummy.query(Dummy.id).filter(age=12).all()
Dummy.query(Dummy.id, Dummy.age).filter(Dummy.age >= 12).all()
```

#### order_by

```python
Dummy.query.order_by(Dummy.id.desc()).all()
```

multiple order expressions:

```python
Dummy.query.order_by(Dummy.age.asc(), Dummy.id.desc()).all()
```

#### group_by

```python
Dummy.query('age', 'count(1)').group_by('age').all()
```

multiple group expressions:

```python
Dummy.query('name', 'age').group_by('name', 'age').all()
```

#### join

```python
Dummy.query.join(Foo).filter(Dummy.id == Foo.age).all()
Dummy.query.left_join(Foo).filter(Dummy.id == Foo.age).all()
```

### Douban style


```python
dummy = Dummy.get(1)
dummys = Dummy.gets([1, 2, 3])

dummy = Dummy.get_by(id=1)
dummys = Dummy.gets_by(Dummy.age >= 12)
```

#### 从缓存中查询

```python
dummy = Dummy.cache.get(1)
dummys = Dummy.cache.gets([1, 2, 3])
```

## Hooks

在日常的产品需求中，经常有一些针对增删改查的前后状态的 hook 需求。比如在新建完某篇文章后发一条广播，在文章内容更新后给用户发送消息提醒等等。
针对这些需求 OLO 提供了一系列方便的 hook 方法：

### creation hook:

```python
@classmethod
def before_create(cls, **kwargs):
    """是个类方法, 在对象创建（入库）前调用
    参数为你调用 `create` 方法时传入的参数, 
    返回值是个 `boolean` 值, `True` 代表可以创建, `False` 代表不可创建,
    返回 `False` 时, 将无法创建
    """
    return True

@classmethod
def after_create(cls, instance):
    """是个类方法, 在对象创建（入库）后调用
    instance 是指创建完成的对象
    """
    pass
```

### update hook:

```python
def will_update(self, next_inst):
    """在对象更新前调用
    参数是即将更新为的对象,
    返回值是个 `boolean` 值，`True` 代表可以更新, `False` 代表不可更新
    返回 `False` 时，将不更新
    """
    return True

def age_will_update(self, next_age):
    """在 `age` 字段更新前调用
    参数是即将 `age` 字段即将更新为的值,
    返回值是个 `boolean` 值，`True` 代表可以更新, `False` 代表不可更新
    返回 `False` 时，将不更新
    聪明的你可能猜出来了，这种方法名的格式为 '{field_name}_will_update'
    """
    return True

def did_update(self, pre_inst):
    """在对象更新后调用
    参数是更新前的对象
    """
    pass

def age_did_update(self, old_age):
    """在 `age` 字段更新后调用
    参数是更新前的 `age` 字段
    聪明的你可能猜出来了，这种方法名的格式为 '{field_name}_did_update'
    """
    pass
```

### delete hook:

```python
def before_delete(self):
    """在对象删除前调用
    返回值是个 `boolean` 值，`True` 代表可以删除, `False` 代表不可删除
    返回 `False` 时，将不删除
    """
    return True

def after_delete(self):
    """在对象删除后调用
    """
    pass
```

## 事务

OLO 提供了比较全面的事务管理：

```python
with db.transaction():
    for age in range(10, 20):
        Dummy.create(name='foo', age=age)
```

支持嵌套的事务:

```python
with db.transaction():
    Dummy.create(name='foo', age=11)
    with db.transaction():
        ...
```

  [model]: /model.md
  [options]: /model_options.md
  [field]: /fields/field.md
