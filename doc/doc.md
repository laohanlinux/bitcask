# BitCask Design

本项目基于`basho`的`bitcask`论文所设计的`键/值`存储系统。

## 磁盘源数据域

![源数据域](http://pic.yupoo.com/iammutex/BwqvSyJo/qQIps.jpg)


## HashMap结构

存储于内存中。

![](http://pic.yupoo.com/iammutex/BwqvSLXE/F43A2.jpg)

## Hint File

用于重建`HashMap`数据结构以及`HashMap`的持久化。

![](http://pic.yupoo.com/iammutex/BwqvTat7/o6LeV.jpg)
 
## 其他参数说明

文件的删除标志由ksz 和 valuesz 决定

如果ksz 和 valuesz 都为0，则表示该记录的操作是删除的操作。

- sync.open_timeout  

```
none — lets the operating system manage syncing writes (default)
o_sync — uses the O_SYNC flag, which forces syncs on every write
Time interval — Riak will force Bitcask to sync at specified intervals
```

- merge Policy

```
always — No restrictions on when merge operations can occur (default)
never — Merge will never be attempted
window — Merge operations occur during specified hours
```

```
windows.start = 3
windows.end = 7
```

- Merge Interval

```
merge_check_interval = 3m
```

- Fold Keys Threshold

```
max_age = 0.5s
max_puts = 1000
```

- Automatic Expiration

`not impleted`
