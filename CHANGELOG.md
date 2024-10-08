# 版本日志

## v2.3.0

- 适配 python 3.10 版本
- 增加 `Table`

## v2.2.6

- 将加密功能作为可选功能

## v2.2.5

- 分割代码，无功能变化

## v2.2.4

- 增加创建密钥信息的函数

## v2.2.3

- 完善类型处理和转换等
- 微调部分函数结构

## v2.2.2

- 如果内部的 sqlite3 语句执行失败，在报错信息中会展示出错的语句
- 将对 `BlobType` 的加密函数合并到该类之内
- 增加更合理的类型检查和隐式转换系统
- 支持转义字符串中的单引号
- 其他微调

## v2.2.1

- `Expression` 的 `or_` 方法支持提升优先级

## v2.2.0

- 修复一些 bug
- 使用不那么随机的 Fernet 实现加密
- 支持加密内容查询

## v2.1.1

- 在密钥错误或不合法的情况下不报错，而是直接返回密文信息

## v2.1.0

- 移除未使用的解密函数
- 在未提供密钥的情况下，查询加密的 BLOB 不会报错，但插入或更新会报错

## v2.0.0

- 移除 `AbsHeadRow`
- 支持 BLOB 类型加密

## v1.2.0

- 将 `insert_into` 等函数的 `execute` 作为 `commit` 的前提
- 允许在更新数据时将 NULL 填入到非 NULL 类型的列，但是排除有 NOT NULL 限制的列
- 当 `Operand` 是 BLOB 类型时，其 `equal_to` 接收的参数为 `str` 时自动转为 `BlobType` 

## v1.1.0

- 增加列举表名的功能。
- 修改 `order_by` 的类型提示，使之支持字符串。
- 插入数据前的类型检查中，支持 `int` 转 `float` 。
- 给 `like` 函数添加 `not_` 参数。
