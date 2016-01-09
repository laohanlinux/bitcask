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
