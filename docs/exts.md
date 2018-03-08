# 插件

## declared_attr

声明属性，类似于 `property` 用来定义实例的动态属性，
`declared_attr` 用来定义类的动态属性

主要用在动态生成 [Field][field], 或动态生成 [`Model.__table_name__`][table_name]:

```python
from olo.ext.declared import declared_attr


class BaseModel(object):

    @declared_attr
    def id(cls):
        if cls.__name__ == 'Foo':
            return Field(str)
        return Field(int)

    @declared_attr
    def __table_name__(cls):
        return cls.__name__.lower()
```

## hybrid_property

混合属性，某些情况先需要一些特殊的属性来作为查询条件:

```python
from olo.ext.hybrid import hybrid_property


class Line(BaseModel):
    start = Field(int)
    end = Field(int)

    @hybrid_property
    def length(self):
        return self.end - self.start


line = Line.query.filter(Line.length == 2).first()
assert line.length == 2
```

可爱的是 `hybrid_property` 可以作为 `index_key` 的一部分，
从而可以作为被缓存对象的请求条件，详情请看 [CacheWrapper][index_key]：

```python
from olo.ext.hybrid import hybrid_property


class Line(BaseModel):
    start = Field(int)
    end = Field(int)

    __index_keys__ = (
        ('length',),
    )

    @hybrid_property
    def length(self):
        return self.end - self.start


# 命中缓存
Line.cache.get_by(length=2)
```

## exported_property

某些情况下需要通过 [Model.to_dict][to_dict] 方法来导出其他的属性:

```python
from olo.ext.exported import exported_property


class Line(BaseModel):
    start = Field(int)
    end = Field(int)

    @exported_property
    def length(self):
        return self.end - self.start

line = Line.create(start=1, end=3)
assert line.to_dict() == {'start': 1, 'end': 3, 'length': 2}
```

  [field]: /fields/field.md
  [table_name]: /model.md#__table_name__
  [to_dict]: /model.md#to_dict
  [index_key]: /cache_wrapper.md#index_key
