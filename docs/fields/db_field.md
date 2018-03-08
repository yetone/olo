# DbField

字段类, 用来定义 beansdb 字段的类型等属性

用法:

```python
class Dummy(BaseModel):
    id = Field(int, primary_key=True)
    name = DbField(str)
    
    def get_uuid(self):
        return '/{}/{}'.format(
            self.__table_name__,
            self.id
        )
```

如上所示, 存在 `DbField` 的类需要用户提供一个 `get_uuid` 实例方法来生成一个唯一的 `key`

## 实例方法

### `__init__`

函数签名: 同 [`Field.__init__`][1]

参数:

1. `version`: int | None; 版本, 默认为 `None`

    为 `None` 时, 此 `DbField` 的版本就是 `Model._options.db_field_version`
    
    具体意义请前往 [Options.db_field_version][2]

2. 其他同 [`Field.__init__`][1]


  [1]: /fields/field.md#__init__
  [2]: /model_options.md#db_field_version
