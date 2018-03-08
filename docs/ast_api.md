# AST API

一组神奇的 API 🎉

借鉴（抄袭）[Pony](https://ponyorm.com/) 用 Pythonic 的方式来请求数据

## 函数

### select

参数是一个 generator

用法:

```python
q = select(
    d for d in Dummy 
    if d.age > 1 and d.id < 10 or d.name in ['a', 'b']
)

# 完全等同于：

q = Dummy.cq.filter(
    (Dummy.age > 1) & (Dummy.id < 10) | (
        Dummy.name.in_(['a', 'b'])
    )
)

# 你没看错，用的是 `Dummy.cq`，所以用 `select` 函数默认是走缓存的，嘻嘻
```

类似的：


```python
q = select(
    (d.id, d.name) for d in Dummy 
    if d.id == 10 or d.age > 10
)

# 完全等同于:

q = Dummy.cq('id', 'name').filter(
    (Dummy.id == 10) | (Dummy.age > 10)
)
```


是不是很神奇呢，代码也直观了许多，嘻嘻

还有更神奇的：

#### join


```python
q = select(
    (f.id, b.id)
    for f in Foo 
    for b in Bar
    if f.age == b.age and f.id > 10
)

# 上面会生成下列 SQL：

'''
SELECT `foo`.`id`, `bar`.`id` FROM `foo` JOIN `bar`
WHERE `foo`.`age` == `bar`.`age` AND `foo`.`id` > 10
'''
```
