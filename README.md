# go-archiver

用于解决 `pt-archiver` 使用中遇到的问题：

* 字符集的兼容性较差
* 批量操作依赖于 `LOAD DATA`
* 不支持无索引的表

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
-src.host 127.0.0.1 \
-src.port 3306 \
-src.username xxxx \
-src.password xxxx \
-src.database db1 \
-src.table tb1 \
-src.where "ts < '2024-01-01 00:00:00'" \
-src.limit 2000 \
-tgt.host 127.0.0.1 \
-tgt.port 3308 \
-tgt.username xxxx \
-tgt.password xxxx \
-tgt.database db2 \
-tgt.table tb2 \
-progress 5s \
-sleep 0 \
-statistics
```

## 性能比对

`pt-archiver`

```shell

```

`go-archiver`

```shell

```