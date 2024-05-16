# go-archiver

## 背景

该项目是 [`pt-archiver`](https://docs.percona.com/percona-toolkit/pt-archiver.html) 的 go 语言实现版本，其优点有：

* 不使用 `LOAD DATA`，云数据库产品或无法开启 `local_infile` 的实例均可使用
* 支持 MySQL 协议的目标端，理论上都能进行写入
* 性能较高，详见下方数据比对
* 支持无索引的表
* 不同字符集的兼容性较好

目前核心功能已完成，更多额外特性在逐步迭代中。

## 编译

```shell
git clone https://github.com/dbadylan/go-archiver.git
cd go-archiver/cmd/archiver/
go build
./archiver -h
```

## 示例

```shell
./archiver \
--src-address 127.0.0.1:3306 \
--src-username xxxx \
--src-password xxxx \
--src-database db1 \
--src-table tb1 \
--src-where "ts < '2024-01-01 00:00:00'" \
--src-limit 2000 \
--tgt-address 127.0.0.1:3308 \
--tgt-username xxxx \
--tgt-password xxxx \
--tgt-database db2 \
--tgt-table tb2 \
--progress 5s \
--statistics
```

## 性能比对
