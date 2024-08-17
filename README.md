# Sqlite3Helper

一个不安全的 sqlite3 包装库

## 介绍

因本人总是记不住 SQLite 的那些 SQL 语法，在 Python 里使用 sqlite3 这个库的时候每次都得去查语法，甚是烦躁，于是就编写了这个包装库，把常用的数据库操作都封装成了函数。

说这个库不安全，是因为这个库没有处理 SQL 注入攻击的问题。本来也就是自己用一用，注入攻击这么复杂的问题，还是先放一放吧。

## 安装

```sh
pip install Sqlite3Helper
```

### 安装可选的加密功能

> 需 2.2.6 版本以上，2.2.6 版本之前是强制安装的

```sh
pip install Sqlite3Helper[crypto]
```


## 示例

点击 [这里](docs/basic_usage.md) 查看基本使用。
