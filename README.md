# go-archiver

## 背景

该项目是 [`pt-archiver`](https://docs.percona.com/percona-toolkit/pt-archiver.html) 的 go 语言实现版本，其优点有：

* 不使用 `LOAD DATA`，云数据库产品或无法开启 `local_infile` 的实例均可使用
* 支持 MySQL 协议的目标端，理论上都能进行写入
* 性能有所提高，详见下方数据比对
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

参数

|       | pt-archiver                                                                   | go-archiver                                     |
|-------|-------------------------------------------------------------------------------|-------------------------------------------------|
| 全表归档  | --limit 2000<br />--bulk-insert<br />--bulk-delete<br />--where '1 = 1'       | --src-limit 2000                                |
| 按条件归档 | --limit 2000<br />--bulk-insert<br />--bulk-delete<br />--where 'k < 2510000' | --src-limit 2000<br />--src-where 'k < 2510000' |

### 全表归档

pt-archiver

```
Started at 2024-05-16T17:14:02, ended at 2024-05-16T17:19:24
Source: A=utf8,D=sysbench,P=3306,h=172.16.0.1,p=...,t=sbtest1,u=root
Dest:   A=utf8,D=sysbench,P=3306,h=172.16.0.2,p=...,t=sbtest1,u=root
SELECT 5000000
INSERT 5000000
DELETE 5000000
Action              Count       Time        Pct
bulk_inserting       2500    85.6934      26.68
bulk_deleting        2500    51.6852      16.09
commit               5000    16.0724       5.00
select               2501    11.5496       3.60
print_bulkfile    5000000   -15.4340      -4.80
other                   0   171.6598      53.44
```

go-archiver

```
{
    "time": {
        "begin": "2024-05-16 17:28:37",
        "finish": "2024-05-16 17:31:13",
        "duration": "2m35s"
    },
    "instance": {
        "source": {
            "address": "172.16.0.2:3306",
            "database": "sysbench",
            "table": "sbtest1",
            "charset": "utf8"
        },
        "target": {
            "address": "172.16.0.1:3306",
            "database": "sysbench",
            "table": "sbtest1",
            "charset": "utf8"
        }
    },
    "action": {
        "select": 5000000,
        "insert": 5000000,
        "delete": 5000000
    }
}
```

### 按条件归档

pt-archiver

```
Started at 2024-05-16T18:57:13, ended at 2024-05-16T19:00:22
Source: A=utf8,D=sysbench,P=3306,h=172.16.0.1,p=...,t=sbtest1,u=root
Dest:   A=utf8,D=sysbench,P=3306,h=172.16.0.2,p=...,t=sbtest1,u=root
SELECT 2885879
INSERT 2885879
DELETE 2885879
Action              Count       Time        Pct
bulk_inserting       1443    46.4956      24.60
bulk_deleting        1443    33.1145      17.52
commit               2886     9.3321       4.94
select               1444     8.0295       4.25
print_bulkfile    2885879    -8.5681      -4.53
other                   0   100.6093      53.23
```

go-archiver

```
{
    "time": {
        "begin": "2024-05-16 19:14:43",
        "finish": "2024-05-16 19:16:42",
        "duration": "1m58s"
    },
    "instance": {
        "source": {
            "address": "172.16.0.2:3306",
            "database": "sysbench",
            "table": "sbtest1",
            "charset": "utf8"
        },
        "target": {
            "address": "172.16.0.1:3306",
            "database": "sysbench",
            "table": "sbtest1",
            "charset": "utf8"
        }
    },
    "action": {
        "select": 2885879,
        "insert": 2885879,
        "delete": 2885879
    }
}
```