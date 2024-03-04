# Codewave-Async分布式任务队列中间件

## 引入使用
Async模块分为`master`和`slave`，目前需要本地打包成jar包引入。

Async是一个仅依赖于Redis，支持心跳检测与宕机恢复，水平扩展，消费者组自动负载均衡，按任务优先级执行，具备高可用性的轻量级分布式任务队列中间件。

### 配置Config
参照async-test中的simpletest进行配置，其中prefix相当于topic，master和slave必须使用相同的prefix创建

queueNum指定创建队列的数目，最好与slave节点数目一致，不指定此项参数则默认使用一个队列

rebalanceStrategy指定要启用的消费者组均衡策略，默认为轮询

heartbeatInterval和expirationCount的乘积决定了心跳超时判定时间
```
private TaskConfig TaskConfig(RedissonClient redissonClient) {
        config = TaskConfig.builder()
                .prefix(prefix)
                .redissonClient(redissonClient)
                .queueNums(3)
                .rebalanceStrategy(ROUND_ROBIN)
                .heartbeatInterval(heartbeatInterval)
                .expirationCount(expirationCount)
                .build();
        return config;
    }
```

## 实现原理

![Async-Master-Slave](https://github.com/Rajahn/Codewave2/assets/39303094/d79fe180-9bd5-4594-8de2-5c9e4692c1ca)

### 1. 就绪队列

- 就绪队列用于让slave获取任务，其中的任务由master提交，当创建了多个队列时，会使用负载均衡算法为每个slave分配要负责消费的队列并将映射关系保存在redis中
- 应尽量保证slave节点数小于等于队列数量，当slave数量大于队列数量时，会出现一个队列对应多个消费者的情况，
  此时会启用分布式锁从队列中消费，这种设计是因为本框架的使用场景是尽量充分利用slave的计算资源
- 就绪队列采用`zset`结构，配合优先级字段实现任务按优先级调度。

### 2. 执行队列

- 每个master/slave节点都有独立的执行队列，存储正在执行中的任务信息，用于宕机后恢复。
- 执行队列采用Hash结构，key与master/slave节点的心跳key相同

### 3. 结果队列

- 用于存放已经结束的任务，由worker提交，master进行消费。
- 结果队列采用Hash结构，key与master/slave节点的心跳key相同

### master职责

- master负责将任务提交到就绪/重试队列。
- master从结果队列左侧消费已完成的任务。
- master每隔一段时间向心跳队列发送心跳时间戳。

### slave职责

- slave负责从就绪/重试队列获取任务。
- slave具体业务执行完毕后，向结果队列提交任务。
- slave每隔一段时间向心跳队列发送心跳时间戳。

## 心跳检测机制实现 
![Async-heartbeat](https://github.com/Rajahn/Codewave2/assets/39303094/58b059d5-ed82-4bd1-bb88-39918cf4e6dd)

- master/slave在启动时便会不断向心跳队列发送心跳，结构为hash，key为节点信息，value为当前时间戳。
- master/slave 启动时，会抢占一个"leader"分布式锁，确定一个节点为leader。
- 具有leader身份的节点负责每隔一段时间获取心跳队列中的数据，当发现某个节点的hash value时间戳与当前时间差距大于阈值，即认定此节点宕机，执行宕机处理。

## 宕机处理

- leader节点通过宕机节点的hashKey判断是master节点还是slave节点。
- 对于宕机的master节点，将其执行队列中的任务放入结果队列。
- 对于宕机的slave节点，将其执行队列中的任务放入就绪队列。
- 以上操作完成后删除执行队列的hashkey，并删除心跳队列的hashkey。
- 如果具有leader身份的节点宕机，分布式锁过期释放，其他节点会重新开始抢占成为leader。

## 消费者组负载均衡

- 作为leader职责的一部分，在有新的slave节点加入时，为其分配要负责消费的队列，目的是在获取任务时避免使用锁
- 当slave获取任务时，会调用getQueuesForWorker方法从redis中获取其负责消费的队列，如果没找到则说明当前slave是新加入节点，触发均衡调整
- 当有节点宕机，leader在执行宕机处理时，触发均衡调整

## 优先级调度
- 指定优先级由业务方实现，只需要在master添加任务时包含score即可，以下为一种使用案例
- 每个task在加入就绪队列时，以其orderTime作为zset的score，orderTime是一个长整型。
- 在任务创建时，orderTime = 当前时间 - priority，priority代表优先priority秒进行调度。
- 当任务重试时，orderTime = 当前时间 + delay 降低优先级。

## 设计演进过程

本项目的使用场景来自VT CS5704课程项目，经过多轮重构。调研常见分布式任务组件后，发现我们场景的主要需求在于分布式的任务执行，且需要为多阶段任务，任务重试，优先级调度功能做预留设计，
xxl-job等组件主要定位在执行定时任务，消息队列主要在于消息流转，其主要功能并不是我们想要的，且引入较重，购买服务增加成本。我们场景的主要制约在于：

1. 租不起GPU服务器跑项目，只能利用几台个人pc作为worker节点。
2. 我们的通信使用内网穿透工具ngrok，经常掉线断联。
3. yash同学的深度学习模型过于复杂，且依赖其他在本地运行的ai音频增强底座服务，多次尝试打包docker失败，遂只能本地运行，无法构建集群或使用运维工具。

**开发阶段1**：任务到达后直接拆分存入数据库，只支持使用一个worker，ip地址写死在配置文件，使用异步线程任务向worker推送，实现了基本功能。

缺点：每来一个任务就要启用一个线程处理，即便自定义了线程池，请求处理能力仍十分有限；只能使用一个worker，效率非常低下，且worker阻塞/任务失败后任务信息丢失。
                [codewave1实现](https://github.com/Rajahn/CodewaveService)

**开发阶段2**：尝试在worker侧用docker-compose实现一个简单的 [集群](https://github.com/Rajahn/TTS_XunFei_Service_Deployment_With_Docker_Compose)，采用轮询分配请求，提升处理能力，已验证demo；但经yash同学多次努力后其模型打包docker失败，此方案作废。

**开发阶段3**：使用Redis的List做消息队列，任务到达后先放入redis缓存（有可能丢失），然后逐步写入数据库，尝试使用Nacos等服务注册中心实现服务注册，权衡后放弃；把向worker推送改为worker主动拉取任务，至此支持多个worker同时工作，效率有所提升。

缺点：任务失败/worker节点宕机后任务丢失。

**开发阶段4**：将任务处理模块单独拆分成Codewave-Async模块，完善了心跳，宕机处理等设计，并为每步功能添加测试

**开发阶段5**：优化任务处理，支持多队列处理，避免在抢占任务时使用锁
