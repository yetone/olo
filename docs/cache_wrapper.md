# CacheWrapper

[Model][model] 类中的数据查询接口的缓存版实现

## 如何命中缓存

### 声明 primary_key

单个查询(get, get_by)和批量查询(gets, gets_by), 查询条件是 primary_key

```python
class Dummy(BaseModel):
    # 声明 primary key
    id = Field(int, primary_key=True)

# 以下都命中缓存
Dummy.cache.get(1)
Dummy.cache.gets([1,2, 3])
Dummy.cache.get_by(id=1)
Dummy.cache.gets_by(id=1)
```

### 声明 unique_key

单个查询(get, get_by), 查询条件是 unique_key

```python
class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    key = Field(str)
    age = Field(int)
    name = Field(str)
    
    # 声明 unique keys
    __unique_keys__ = (
        ('key',),
        ('age', 'name'),
    )

# 以下都命中缓存
Dummy.cache.get_by(key='test')
Dummy.cache.get_by(age=12, name='foo')
```

### 声明 index_key

批量查询(gets_by)和数量查询(count_by), 查询条件是 index_key

注: `gets_by` 需要指定 `limit`
因为 OLO 的批量缓存的实现是缓存了前 200 条数据，
所以不指定 `limit` 或者 `offset` + `limit` > 200 将无法命中缓存

```python
class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    key = Field(str)
    age = Field(int)
    name = Field(str)
    
    # 声明 index keys
    __index_keys__ = (
        ('key',),
        ('age', 'name'),
    )

# 以下都命中缓存
Dummy.cache.gets_by(key='test', limit=20)
Dummy.cache.gets_by(age=12, name='foo', limit=20)
Dummy.cache.gets_by(age=12, limit=20)
Dummy.cache.count_by(age=12, name='foo')
Dummy.cache.count_by(age=12)
```

如上所示, OLO 的缓存对索引的处理和 MySQL 是一致的：

    `gets_by(age=12)` 命中索引 `('age', 'name')`

#### 声明 order_by

如果需要在查询中排序，还需声明 `__order_bys__`:

```python
class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    key = Field(str)
    age = Field(int)
    name = Field(str)
    
    # 声明 index keys
    __index_keys__ = (
        ('key',),
        ('age', 'name'),
    )
    
    # 声明 order bys
    __order_bys__ = (
        'age', '-age',
        ('name', '-age')
    )

# 以下都命中缓存
Dummy.cache.gets_by(key='test', order_by='age', limit=20)
Dummy.cache.gets_by(age=12, name='foo', order_by='-age', limit=20)
Dummy.cache.gets_by(age=12, order_by=('name', '-age'), limit=20)
```

其实 OLO 会根据 `__primary_key__` 自动生成 `__order_bys__`:

```python
__order_bys__ = (
    'id', '-id'
)
```

不根据 `__index_keys__` 自动生成 `__order_bys__` 的原因是会生成指数级的 `__order_bys__`，
导致清缓存时清除大量无用的 `key`，造成极度的资源浪费

## 实例方法

### get

同 [Model.get][model_get]

### gets

同 [Model.gets][model_gets]

### get_by

同 [Model.get_by][model_get_by]

### gets_by

同 [Model.gets_by][model_gets_by]

### count_by

同 [Model.count_by][model_count_by]

  [model]: model.md
  [model_get]: model.md#get
  [model_gets]: model.md#gets
  [model_get_by]: model.md#get_by
  [model_gets_by]: model.md#gets_by
  [model_count_by]: model.md#count_by
