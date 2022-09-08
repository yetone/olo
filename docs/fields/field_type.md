# FieldType

由于 [Field][field] 的类型是「静态」的，所以这里单独说明一下其类型注释

> 不要吐槽我这样搞类型标注！！！因为我写 OLO 的年代还是 Python 2.X 盛行的年代，那时候 Python 3.x 的 type annotation 还是一个触不可及的传说！请珍惜每一个跟随着 Python 从 2.x 一路走来的劳动人民！

## 基本准则

用 `type(value)` 作为 [Field][field] 的类型

## 样例

### `int` / `float` / `Decimal` / `str` / `list` / `dict` / `tuple`

单元素类型（可以勉强叫做原始类型），这种情况很直观，分别代表 int, float, Decimal, str, list, dict, tuple 类型

### `[str]`

表示元素都为字符串类型的 `list`，同理：

* `[int]`: 元素都为整数类型的 `list`
* `[float]`: 元素都为浮点数类型的 `list`
* 等等

### `{str: int}`

表示 `key` 为字符串类型 `value` 为整数类型的 `dict`, 同理：

* `{int: str}`: `key` 为整数类型 `value` 为字符串类型的 `dict`
* 等等

### `(int, str)`

表示两个元素的 `tuple`, 且第一个元素类型是 `int`, 第二个元素类型是 `str`, 同理：

* `(int, int)`
* `(int, str, float)`
* 等等

### 自定义类型

厉害的地方来了，上面所有情况都适用于自定义的类型哦！

例如:

```python
class User(object):
    pass
```

那么 `[User]` 代表的就是所有元素类型都为 `User` 的 `list`

  [field]: /fields/field.md
