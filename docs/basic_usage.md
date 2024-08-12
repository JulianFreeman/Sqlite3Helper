# 建立连接

该库主要的功能类叫 `Sqlite3Worker` ，我们通过它来建立与 sqlite3 数据库的连接，并进行后续操作。

```python
from Sqlite3Helper import Sqlite3Worker

# 不加参数会在内存中创建一个临时的数据库
# 关闭即销毁
sqh1 = Sqlite3Worker()

# 提供一个路径参数则会打开该路径对应的数据库
sqh2 = Sqlite3Worker("test.db")
```

# 创建表

在创建表之前需要先定义列，定义列要用到数据类 `Column` 和枚举类型 `DataType` 。

```python
from Sqlite3Helper import Column, DataType

stu_id = Column(name="stu_id", data_type=DataType.INTEGER, primary_key=True)
name = Column(name="name", data_type=DataType.TEXT, nullable=False)
grade = Column(name="grade", data_type=DataType.REAL)
address = Column(name="address", data_type=DataType.TEXT, has_default=True, default="Earth")
```

然后创建表：

```python
sqh.create_table("students", [stu_id, name, grade, address])
```

# 删除表

```python
sqh.drop_table("students")
```

# 重命名表

```python
sqh.rename_table("students", "pupils")
```

# 添加列

```python
email = Column(name="email", data_type=DataType.TEXT)
sqh.add_column("students", email)
```

# 重命名列

> 该功能仅在 sqlite 3.25.0 以上有效。

```python
sqh.rename_column("students", "name", "fullname")
```

> 该功能还是少用，虽然数据库操作是正常的，但之前定义的 `Column` 对象的名称不会同步变更，如果之后还用到这个 `Column` 对象会产生名称不匹配的问题。

# 插入数据

```python
sqh.insert_into("students", [name, grade, email], [
    ["John Doe", 97.0, "johndoe@gmail.com"],
    ["Karl Blue", 76.0, "bkarl123@outlook.com"],
    ["Liz Brown", 82.0, "lizbr7abc@tuta.com"],
])
```

# 查询数据

## 无条件查询

```python
_, rows = sqh.select("students", [stu_id, name, grade])
print(rows)
# [(1, 'John Doe', 97.0), (2, 'Karl Blue', 76.0), (3, 'Liz Brown', 82.0)]
```

## 有条件查询

条件查询需要使用该库提供的另外一个功能类 `Operand` 实现。

```python
from Sqlite3Helper import Operand

_, rows = sqh.select("students", [stu_id, name, grade],
                     where=Operand(grade).greater_than(80))
print(rows)
# [(1, 'John Doe', 97.0), (3, 'Liz Brown', 82.0)]
```

## 排序

排序使用该库提供的函数 `order` 、枚举 `SortOption` 等实现。

```python
from Sqlite3Helper import order, SortOption

_, rows = sqh.select("students", [stu_id, name, grade],
                     where=Operand(grade).greater_than(80),
                     order_by=[order(name, SortOption.DESC)])
print(rows)
# [(3, 'Liz Brown', 82.0), (1, 'John Doe', 97.0)]
```

## 限制个数和偏移

```python
_, rows = sqh.select("students", [stu_id, name, grade],
                     where=Operand(grade).greater_than(80),
                     order_by=[order(name, SortOption.DESC)],
                     limit=1, offset=1)
print(rows)
# [(1, 'John Doe', 97.0)]
```

# 删除数据

```python
sqh.delete_from("students", where=Operand(grade).less_than(80))
```

# 更新数据

```python
sqh.update("students", [(name, "John Smith"), (grade, 100.0)],
           where=Operand(name).equal_to("John Doe"))
```
