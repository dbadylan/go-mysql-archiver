# go-mysql-archiver

## 背景

该项目是 [`pt-archiver`](https://docs.percona.com/percona-toolkit/pt-archiver.html) 的 go 语言实现版本，其优点有：

* 不依赖于 `LOAD DATA`
  - 云数据库产品或无法开启 `local_infile` 的实例均可使用
  - 支持 MySQL 协议的目标端，理论上都能写入
  - 不再有字符集兼容性问题
* 性能有所提高（详见下方数据比对）
* 支持无索引的表
* 可手动暂停与恢复

目前核心功能已完成，更多额外特性在逐步迭代中。

## 编译

```shell
git clone https://github.com/dbadylan/go-mysql-archiver.git
cd go-mysql-archiver/cmd/archiver/
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

工具参数：

| pt-archiver                                                             | go-mysql-archiver |
|-------------------------------------------------------------------------|-------------------|
| --limit 2000<br />--bulk-insert<br />--bulk-delete<br />--where '1 = 1' | --src-limit 2000  |

表和数据：

使用 `sysbench` 创建测试表，总记录数 500 万。

### 有主键或非空唯一索引

pt-archiver

```
Started at 2024-05-20T14:10:17, ended at 2024-05-20T14:15:40
Source: A=utf8,D=sysbench,P=3306,h=172.16.0.1,p=...,t=sbtest1,u=root
Dest:   A=utf8,D=sysbench,P=3306,h=172.16.0.2,p=...,t=sbtest1,u=root
SELECT 5000000
INSERT 5000000
DELETE 5000000
Action              Count       Time        Pct
bulk_inserting       2500    84.7472      26.22
bulk_deleting        2500    51.8502      16.04
commit               5002    16.2668       5.03
select               2501    11.7352       3.63
print_bulkfile    5000000   -15.4795      -4.79
other                   0   174.1063      53.87
```

go-mysql-archiver

```
{
    "time": {
        "begin": "2024-05-20 14:17:59",
        "finish": "2024-05-20 14:20:34",
        "duration": "2m34s"
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

### 只有普通索引

```sql
ALTER TABLE `sbtest1` MODIFY `id` INT NOT NULL DEFAULT 0, DROP PRIMARY KEY; -- 删除主键
```

pt-archiver

```
Started at 2024-05-20T14:24:35, ended at 2024-05-20T14:30:45
Source: A=utf8,D=sysbench,P=3306,h=172.16.0.1,p=...,t=sbtest1,u=root
Dest:   A=utf8,D=sysbench,P=3306,h=172.16.0.2,p=...,t=sbtest1,u=root
SELECT 5000000
INSERT 5000000
DELETE 5000000
Action              Count       Time        Pct
bulk_deleting        2500    86.6232      23.36
bulk_inserting       2500    71.7427      19.35
select               2501    34.6001       9.33
commit               5002    17.1650       4.63
print_bulkfile    5000000   -15.3862      -4.15
other                   0   176.0144      47.47
```

go-mysql-archiver

```
{
    "time": {
        "begin": "2024-05-20 14:32:19",
        "finish": "2024-05-20 14:35:04",
        "duration": "2m44s"
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

### 无任何索引

```sql
ALTER TABLE `sbtest1` DROP KEY `k_1`; -- 删除普通索引
```

pt-archiver

```
Cannot find an ascendable index in table at /usr/local/bin/pt-archiver line 3261.
```

go-mysql-archiver

```
{
    "time": {
        "begin": "2024-05-20 14:37:30",
        "finish": "2024-05-20 14:52:30",
        "duration": "14m59s"
    },
    "instance": {
        "source": {
            "address": "172.16.0.1:3306",
            "database": "sysbench",
            "table": "sbtest1",
            "charset": "utf8"
        },
        "target": {
            "address": "172.16.0.2:3306",
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

## 任务控制

> socket 文件名与路径可由 `socket` 参数自定义，默认为 /tmp/${src-address}-${src-database}-${src-table}.sock

### 暂停

```shell
echo pause | nc -U /tmp/172.16.0.1:3306-sysbench-sbtest1.sock
```

### 恢复

```shell
echo resume | nc -U /tmp/172.16.0.1:3306-sysbench-sbtest1.sock
```
