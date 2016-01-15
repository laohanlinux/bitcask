# bitcask
this is storage backend  for riot

[Design Doc](https://github.com/laohanlinux/bitcask/blob/master/doc/doc.md)

[riot](https://github.com/laohanlinux/riot)



# Example

```
package main

import (
	"github.com/laohanlinux/bitcask"
	"github.com/laohanlinux/go-logger/logger"
)

func main() {
	bc, err := bitcask.Open("exampleBitcaskDir", nil)
	if err != nil {
		logger.Fatal(err)
	}
	defer bc.Close()
	k1 := []byte("xiaoMing")
	v1 := []byte("毕业于新东方推土机学院")

	k2 := []byte("zhanSan")
	v2 := []byte("毕业于新东方厨师学院")

	bc.Put(k1, v1)
	bc.Put(k2, v2)

	v1, _ = bc.Get(k1)
	v2, _ = bc.Get(k2)
	logger.Info(string(k1), string(v1))
	logger.Info(string(k2), string(v2))
	// time.Sleep(time.Second * 10)
	// override
	v2 = []byte("毕业于新东方美容美发学院")
	bc.Put(k2, v2)
	v2, _ = bc.Get(k2)
	logger.Info(string(k2), string(v2))

}

```

`go run example/bitcask_main.go`

```
2016/01/15 18:31:15 bitcask_main.go:25 [info [xiaoMing 毕业于新东方推土机学院]]
2016/01/15 18:31:15 bitcask_main.go:26 [info [zhanSan 毕业于新东方厨师学院]]
2016/01/15 18:31:15 bitcask_main.go:32 [info [zhanSan 毕业于新东方美容美发学院]]
```

other Example: find it in `xxxx_test.go`

# TODO

- 优化数据结构，减少内存占用
- 增加delete和merge功能
