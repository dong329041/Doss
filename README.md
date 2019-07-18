# Doss

该程序实现了一个分布式对象存储系统的底层引擎，为上层客户端应用提供底层存储能力。整体架构由 apiServer 和 dataServer 组成，apiServer 之间、dataServer 之间完全对等，可以无限扩展，避免单点故障。apiServer 对外提供 Restful HTTP 接口接收客户端请求，并和 dataServer 之间进行交互完成数据访问（可以部署多台 apiServer，上层使用 nginx 做负载均衡）。下面对该系统特性进行简单介绍：

### 数据定位

根据标准的一致性哈希算法实现了 hashRing，并加入 dataServer 存储空间大小不同等权重来平衡各节点的虚拟 cube 复制因子，对象读写请求均根据对象名进行哈希运算映射至虚拟 cube 上，再由虚拟 cube 映射到真实物理节点上；哈希环结构的生成是由每个 dataServer 程序启动时会将自己监听的 ip 地址注册到 mongodb 的 node 表中，apiServer 通过 MongoDB 的 watch 机制实时监测 node 表的变化，从而动态维护哈希环。

### 数据冗余策略

1. 相比于多副本策略，纠删码更节省空间，并且纠删码丢失数据的风险更低，故冗余策略采用纠删码实现；
2. 写过程：stream 对 HTTP 进行了流式封装，封装了一个纠删码编码器和一个哈希计算器，此编码器实现了 io.Writer 接口，该编码器包含（数据分片数+修复分片数）个上传数据流 writer（默认为 4 + 2 = 6）， apiServer 开辟 buffer 缓冲区，将数据一批一批吃到内存中，并在内存中完成纠删码的编码，之后纠删码编码器将计算好的结果分为 6 份送入 6 个 writer，这 6 个 writer 分别请求对应数据节点的 temp 接口，将数据流式上传，在上传的过程中数据同样会送入哈希计算器（通过 io.TeeReader 实现，类似于 Linux 的 Tee 命令），待所有的数据都计算并上传完毕，此时哈希计算器也算出了对象的 hash 值，若与客户端请求头中的 hash 值一致，则将上传到所有数据节点 /temp 接口的临时对象转正为正式对象并在数据库中添加元数据，若不一致则删除临时对象；
3. 读过程：同样生成纠删码编码器，在哈希环中计算出该对象所位于的所有数据节点，生成 6 个 Reader 分别向这些数据节点发起 GET 请求获取对象 6 个分片的数据，同样 apiServer 在 buffer 中一批数据一批数据进行编码，编码完成后的正确数据一批批地发送给客户端，同时将正确的数据 PUT 到发生数据损坏的数据节点上，完成分片数据的修复。

### 数据存储策略

1. 若文件 size 小于 64MB，则访问小对象接口，将小对象写到大的聚合对象内部（聚合对象可类比于 GFS 或淘宝 TFS 中的 chunk 概念）：

> 为什么要有小文件合并：在进行 1 亿海量小文件压测时，发现写到 6000 万个小文件时性能下降剧烈，原因在于：每写一个小文件，xfs 文件系统都要调用 xfsaild 系统调用写日志，从而占据了磁盘的 I/O，导致小文件的读写 I/O 受到影响，HDD盘的性能波动特别大，而且小文件数量过多也会过多占据文件系统的 inode；

> 实现过程：聚合对象的长度固定为 64MB，在上传小对象时，先获取未满的聚合对象，若不存在未满的聚合对象或者获取到的聚合对象剩余容量不够，则生成新的聚合对象，然后将小对象写到其附着的所有聚合对象上（按照 offset 和 size 界定其在聚合对象上占据的字节区间）；那么小对象的读取则将它所附着的聚合对象上根据 offset 和 size 来获取数据；

> 如何解决多 apiServer 同时写，访问同一个聚合对象产生的数据区间冲突：每个 apiServer 在上传数据之前先更新数据库 aggregate_object 表，发现可用的聚合对象后将空间预定抢占，其他的 apiServer 则预定后面的空间，该操作通过 MongoDB 的FindOneAndUpdate 来保证写操作的原子性，抢占完成后自己慢慢将数据推送到所占据的空间；若上传过程了发生了数据损坏，后期的纠删码实时修复会保证数据的正确性；

2. 若文件 size 大于 64MB，则访问大对象接口：大对象上传时可向 apiServer 的 /object 接口发送 POST 请求得到一个加密的 token ，该 token 可用于断点续传，从而抵御不良的网络环境，该 token 中包含了对象 name、size、hash 值等信息，当发生网络中断时，可从该 token 中恢复数据流继续上传；
3. 小文件聚合的概念对于 apiServer 是无感知的，由 dataServer 全权负责。

### 数据修复：数据的自我治愈

##### 文件系统层面的实时监控与修复

1. dataServer 程序中会启动专门的协程监控大对象目录和聚合对象目录下文件的变化，通过  Linux 系统的 inotify 机制和 windows 的 ReadDirectoryChangesW 系统调用监控目录，如果发生 write 事件和 delete 事件，则得到发生损坏的文件名，将该文件信息序列化成 MongoDB 文档添加到 repair_object 集合中，apiServer 通过 MongoDB 的 watch 机制生成 changeStream 监听到该 collection 发生了 insert，则生成纠删码编码器将数据编码计算并将计算完成的结果覆盖损坏的分片；
2. 如何保证损坏的对象只被一个 apiServer 修复，并发处理的问题？

> 分布式乐观锁：apiServer 通过 MongoDB 的 watch 监听到 repair_object 发生改变后，立刻将该文档的 Locker 字段设置为自己的 ip:port（若查看该文档 Locker 属性已经存在，则说明该损坏的对象被别的 apiServer 捷足先登了），抢占分布式乐观锁的过程通过 FindOneAndUpdate 来保证操作的原子性；抢占完成后开始慢慢修复，待修复完成后，dataServer 的目录监听会再次监听到该对象的变化（因为损坏的数据被正确的数据覆盖），此时 dataServer 将验证该对象的 hash 值是否变得正确了，若正确则将之前插入到 repair_object 集合中的文档删除，表示修复过程结束。

> 死锁问题分析：由于 MongoDB 中没有租约机制，所以当 apiServer 抢到锁之后，在未完成修复工作之前宕机，则会造成该损坏的对象一直被这个宕机的 apiServer 占用，此谓“占着茅坑不拉屎”式的死锁，这种情况如何处理的：此时该损坏的对象得不到实时的修复，但在正常的业务 IO 中，生成的纠删码编码器访问此对象时会检测到数据错误并修复的，修复完成后由 dataServer 感知到并将 repair_object 中该文档删除，也就是剥夺了该锁，所以对于数据正确性没有任何影响，这里只是提供实时修复，避免长时间没有读取此对象而造成的错误累积）（apiServer 再次启动时检查 repair_object 表中是否存在 Locker 为自己的 ip:port 的，若存在则执行上次未完成的修复任务）；

##### 硬件产生的分片损坏
以上分析是在文件系统层面提供实时修复，避免错误累积从而增大丢失数据的风险，但是硬件层面（如磁盘磁性退化等）造成的数据损坏，同样在业务 IO 的数据访问时经过纠删码编码进行修复；

综合以上两方面，数据可以做到自我治愈，正确性是可以得到严格保证的。

### 数据去重

由于不同的客户端可能会上传同一份数据，所以在 dataServer 中以对象 hash 值为对象名来进行保存，若有相同 hash 值则只在 mongodb 中添加元数据记录，而不会再 dataServer 中保存相同的数据。

### 断点续传

之前的分析中已经提到。

### 数据维护

在 dataServer 包中的 check 子包中，定义了 cron 表达式，每天凌晨 4 点会定期检查系统数据：

1. 元数据：将早期的版本删除，只留下 5 个版本，类似队列结构，先入先出；
2. 对象数据：由于客户端发送 DELETE 请求时，只是将元数据中的 hash 值置为空字符串（系统的约定，此为删除的标记），所以在维护阶段将对象移到 /garbage 目录，若小文件对象的 hash 为空，则将聚合对象的引用数减1，并将该分片元数据删除，之后将未被引用的聚合对象放入 /garbage 目录，最后将 /garbage 回收站中存在时间超过 10 天的对象删除；
3. 凌晨 4 点的洛杉矶绝大部分人在睡觉，所以数据维护占用的磁盘 IO 不会对正常的业务 IO 造成大的影响。


----


# 程序结构说明

### apiServer 包
此包对客户端提供了 Restful HTTP 接口：

1. **heartbeat 子包**：监听数据节点发送的心跳消息；
2. **locate 子包**：在哈希环中定位对象应存放在哪些数据节点上；
3.** objects 子包**：对于客户端请求的 /objects 接口进行处理，包括：GET、POST、PUT、DELETE 方法；repair.go：监听数据节点的对象损坏并通过构造经纠删码编码的数据流对其进行修复；
4. **temp 子包**：对于客户端请求的 /temp 接口进行处理，包括：PUT、HEAD 方法；
5. **version 子包**：对于客户端请求的 /version 接口进行处理，获取对象所有的版本并返回给客户端版本信息。
6. **apiServer.go**：apiServer 程序的主入口，包括初始化设置线程数量、监听数据节点心跳协程、实时监测数据节点的变动从而动态维护哈希环、监听数据节点的对象损坏情况并立即修复等。

### dataServer 包

1. **heartbeat 子包**：向 apiServer 汇报心跳消息；
2. **locate 子包**：在内存中维护对象的信息（分片属于哪个对象、分片 id 是多少以及每个聚合对象当前可用容量等信息）；监控大对象和聚合对象的目录，感知文件损坏并实时修复；
3. **objects 子包**：对外提供 /objects 接口的处理，包括：GET 方法；
4. **temp 子包**：此包是真正对数据流进行处理的包，对外提供 /temp 接口的处理，包括：GET、PATCH、POST、PUT、HEAD、DELETE 方法；

### stream 包

此包是对 HTTP 的流式处理封装，此包为整个程序读写流程处理的灵魂。此包将纠删码的编码过程、数据校验、断点续传等流程封装为流式，大致流程为：buffer 缓冲区的管理、纠删码在缓冲区中进行计算编码并流式推送到数据节点、数据发生损坏时进行数据重构修复并将重构完成的正确数据一边发送给客户端一边推送到发生数据损坏的数据节点。


### hashRing 包

此包是按照一致性哈希论文中阐述的算法流程实现的，并加入了节点权重等因素，数据的定位将对象名映射到虚拟 cube 上，再由虚拟 cube 映射到真实物理节点，从而完成数据请求的负载均衡，并且按比例将更多的数据喂给存储能力越大的数据节点，以实现数据均衡；用一致性哈希进行数据定位的好处还在于数据的定位是在内存中完成，没有系统角色之间的网络交互，所有的节点看到的一致性哈希视图是一致的。该包采用单例模式设计，包内的哈希环在包内唯一，通过 GHashRing 指针提供给外部使用。

### meta 包

此包是对 MongoDB 数据库操作的封装，所有表的增删改查、查询条件和查询结果的序列化与反序列化均在此包内完成，包外只需生成相应的数据库操作结构体变量并访问该变量的方法即可完成数据库的访问。

### rbmq 包

对 github.com/streadway/amqp 的封装：采用 publish/subscribe 模式，封装为 producer、consumer 两种结构体角色，外部只需创建所需的结构体，并调用该结构体的相应方法即可完成操作；以收发心跳的场景为例：每个 DataServer 节点将自己的 ip 地址和当前汇报的心跳时间戳作为消息主体 publish 到队列上（所有 DataServer 的队列绑定在同一交换机上），每个 apiServer 以订阅的方式通过该交换机从每个 DataServer 的队列中取出心跳消息。

### utils 包

此包主要定义系统中所用到的工具类函数：

1. addr.go：获取 rabbitMq、MongoDB 的 url 地址，获取本机网卡地址等；
2. nullWriter.go：实现一个黑洞设备文件（类似于 Linux 的 /dev/null 设备），实现过程大致为：定义 NullWriter 结构体，为该结构体实现 io.Writer 接口，在 Write 方法中开辟 buffer 缓冲区，将数据一批一批读入内存并丢弃；
3. parseHeader.go：对 HTTP 请求中解析出 hash、size、offset 等信息；
4. watchFilePath.go：实现了监控指定目录文件的变化函数：

> 实现原理：使用的是 Linux 系统的 inotify 机制和 windows 的 ReadDirectoryChangesW；

> 函数防抖处理：当目录下文件发生改变时，将该事件收集到数组中，启动定时器5秒，若5秒内再次发生变化，则继续将该事件 append 到数组中，并将定时器重置……直至5秒内没有再发生变化，将发生的所有事件传递给外部的回调函数；

> 对外提供 GetStopWatchSignal 函数用于获取停止监控信号的通道，外部获取到的是该包内 stopWatchSignal 的地址，向此通道中放入一个 bool 值，则可结束监听；
5. utils.go：其他工具函数，如：SeekWrite 和 SeekCopy（可以指定偏移量和读取量来读写文件）、流式计算哈希值、判断 slice 中是否包含指定元素等等。

### common 包

1. **apiFlag 子包**：定义了 apiServer 程序的命令行参数及其默认值；
2. **dataFlag 子包**：定义了 dataServer 程序的命令行参数及其默认值；
3. **constants.go**：定义了系统中使用到的常量；
4. **Errors.go**：定义了系统中使用到的不同种类的错误码.

### config 包

定义了系统用到的所有配置参数，在 config.json 中定义了配置项的值并且增加了详细的注释说明（默认需将该配置文件拷贝至 /etc/doss/config.json 中），config.go 在该包 init 中对 config.json 进行解析并将所有的配置项赋值到 Config 结构体中（此结构体变量通过 GConfig 指针将地址提供给包外访问，采用单例模式设计）.

### 默认参数的封装

go 语言是不支持函数的默认参数和函数重载的，所以代码中好几处使用了 Functional Options Pattern 的方式实现了默认参数的优雅封装，例如 meta 包和 rbmq 包中的 funcParams 子包。

------

# api接口说明和系统交互流程

### GET /locate/<object_name>：
此时 apiServer 根据对象名查询数据库从而得到 hash 值，然后对对象 hash 值进行一致性哈希计算得到该对象位于的数据节点，apiServer 向这些数据节点的 /locate 接口发送 GET 请求，探测对象是否存在，最后将定位信息返回给客户端；

### GET /versions/<object_name>：
apiServer 将查询数据库中该对象的所有版本，返回给客户端；

### PUT /objects/<object_name>：

1. 客户端需提供两个请求头（size：指定对象的字节长度；digest：SHA-256=<object_hash>：提供 hash 值用于 apiServer 的数据校验）；
2. apiServer 会创建用于纠删码读写的数据流，生成纠删码编码器，此编码器包括 (4+2) 个 writer，分别向 dataServer 的 /temp 接口发起 POST 请求，dataServer 生成 uuid，并将本次上传的相关信息（uuid、name、size、hash）保存在 /temp/uuid 文件中，最后将 uuid 作为响应返回给 apiServer；
3. 纠删码编码器向 6 个 dataServer 的 /temp 接口发送 PATCH 请求，将数据计算编码分成 6 份推送到数据节点，一边推送一边计算 hash，用于上传完成后的校验；
4. 若 hash 校验一致：向 dataServer 的 /temp 接口发送 PUT 请求，dataServer 将 /temp 目录下的临时文件重命名为 /objects/<object_hash.shard_index.shard_hash>；若 hash 校验不一致，则向 dataServer 的 /temp 接口发送 DELETE 请求，将临时文件删除.

### GET /objects/<object_name>(?version=1)
apiServer 根据对象名和版本号查询数据库得到对象 hash 值，一致性哈希计算得到该对象的所有在线数据节点，向这些数据节点的 /objects/<object_hash.shard_index> 发送 GET 请求，dataServer 验证对象 hash 值，若散列值一致的话将文件流拷贝到响应 writer，若不一致则返回错误，apiServer 则会生成响应的 TempWriter 将错误的分片数据修复。

### DELETE /objects/<object_name>
apiServer 将 MongoDB 的 object 集合中该对象的 hash 字段置为空字符串，dataServer 的数据维护协程会定期检查 hash 值为空的对象并将其进行删除。

### POST /objects/<object_name>
1. 客户端需提供两个请求头（size：指定对象的字节长度；digest：SHA-256=<object_hash>：提供 hash 值用于 apiServer 的数据校验）；
2. apiServer 创建可恢复的纠删码编码器，并将数据节点等信息生成一个加密的 token，向客户端返回 201，并设置响应头 location 为：/temp/<token>，客户端得到该地址后可以向该 url 上传数据。

### PUT /temp/<object_name>
1. 客户端给出两个请求头：（Authorization: <token> 用于验证 token 并从 token 中恢复上传流；range: byte=<first>-<last> 用于告诉 apiServer 上传数据的区间）；
2. apiServer 解析 token，得到上一次的上传 stream 信息，并向数据节点的 /temp 接口发送 HEAD 请求，得到已经上传的大小，和客户端的 range 字段进行比对，若一致，则开始断点续传流程。

### HEAD /temp/<object_name>
apiServer 向数据节点的 /temp 接口发送 HEAD 请求，得到已经上传的进度并返回给客户端。
