# ModelOptions

model class 的配置项。model class 的基本配置都在此类中定义

用法:

```python
new_db = Database(new_store, beansdb=beansdb.Client())

class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    
    class Options:
        db = new_db
```

就是在 model class 下定义一个名为 `Options` 类, 其类属性就是此 model class 的配置项

## 配置项

### db

DataBase; `DataBase` 实例, 参与所有的数据库操作

### cache_client

Client; `memcached Client` 实例, 参与缓存的增删改查操作

### cache_key_prefix

str; 缓存 `key` 的前缀, 默认是 `olo`

### cache_expire

int; 缓存过期时间, 默认是 `60 * 60 * 24`, 一天

### enable_log

bool; 是否启用 db log, 默认为 `False`

### db_field_version

int; `DbField` 的 `version`, 默认为 `1`.

此 model class 下的所有的 `DbField` 的 `version` 如果没有单独指定, 则为此 `version`

目前 `version` 有以下值:

* `0`: 代表此 model_class 中的所有的 `DbField` 的值以 `dict` 的形式储存在一个 `beansdb key` 中，类似于 `PropsItem`
* `1`: 代表每一个 `DbField` 储存在不同的 `beansdb key` 中

### cache_key_version

str; 缓存 `key` 的版本, 通常情况下你需要在新增或更改了某些字段的情况下来更新这个版本

### query_class

[Query][query]; [Query][query] 类, `Model.query` 会返回此类的实例, 默认为 [Query][query]

### cache_class

[CacheWrapper][cache_wrapper]; [CacheWrapper][cache_wrapper] 类, `Model.cache` 会返回此类的实例, 默认为 [CacheWrapper][cache_wrapper]

### auto_use_cache

bool; 是否自动使用缓存, 默认为 `False`

如果为 `True`:

* `Model.get` 等同于 `Model.cache.get`
* `Model.gets` 等同于 `Model.cache.gets`
* `Model.get_by` 等同于 `Model.cache.get_by`
* `Model.gets_by` 等同于 `Model.cache.gets_by`
* `Model.count_by` 等同于 `Model.cache.count_by`

  [query]: /query.md
  [cache_wrapper]: /cache_wrapper.md
