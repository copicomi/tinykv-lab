# 2b

在这一part，我们需要实现与 RaftLog 交互的状态机，并为上层提供封装接口

该状态机是一个实现了 `Storage interface` 的 KV-server

一个 `Store` 通过下层的若干个 `peer` 进行同步，这些 `peer` 组成一个 `Raft` 集群

而一个对外服务，可能会包含多个 `Store`，每个`Store server` 单独对应一个 `peer` 集群，所有这些 `peer` 聚合起来构成一个 `region`

总结来说：
- `peer` 代表 `RaftNode`
- `Store` 对应 `Raft` 的上层应用
- `region` 包含若干个 `Store-KV-server`，是 **peer组成的集合**

# 框架

首先，我们先参考 `RaftStorage` 的实现，上层 KV 应该与 `RaftNode` 进行读写操作， `commit` 后才认为完成

与下层 Raft 的状态机实现不同，这里的交互应该是**异步**的，故采用 `go channel` 实现

我们传递不同类型的 `RaftStoreCommandMessage` 与 `RaftNode` 进行通信

传递 `Context` 参数，携带上层 `Region` 的信息

> `store` 有什么必要得知 `region` 的信息呢？
> 我有以下推断：
> 一、`region context` 可能保存了 `peer` 的具体信息，宕机或者领导者信息，方便 `store` 得知环境变化
> 二、`store` 内部的 `peer` 成员很可能是动态流动的，当负载发生变化时，`region` 内部的 `peer` 会在 `store` 之间进行转移

在这两层之间，还有传递 RaftMessage、更新 Ready、更新状态机等脏活，我们建立 `RaftWorker` 来担任这一角色

多个 `RaftWorker` 从 `RaftStore` 的 `channel` 中获取 `commandMessage` 进行处理，并进行回答

# 实现内容

## peer storage

`peer` 就是我们认为的 `RaftNode` 角色，因此实现这一层，就是完成对 `RaftRawNode` 的包装

> 为何要多此一层包装呢？
> 我认为，`RawNode` 只维护 `peer` 下层的信息，但是 `peer` 作为 `Region` 内的个体，必须得知外部环境、状态机等信息，因此这一层维护的是与上层 `Storage-KV-server` 相关的信息
> 
> 而 `RawNode` 负责封装好 `Raft` 接口，向 `peer` 隐藏底层细节，使其只需要关注与上层交互的部分

因此这些 `State` 我们分两个 `badger` 存储：`raftdb` 与 `kvdb`，目的就是将 `peer` 上下层的状态进行区分

> 有关 `state` 与 `db` 的存储，在 `kv/raftstore/meta` 中有格式描述与辅助函数，key和value都要通过辅助函数来获取

> 创建 `peerStorage` 时，会从执行对象读取初始化数据，注意**a`RAFT_INIT_LOG_TERM` 和 `RAFT_INIT_LOG_INDEX` 初始化为 5**，这么做的目的是与中途加入的 `peer` 做区分，这暂时不是我们关注的部分，但要记住这个约定

我们需要实现 `PeerStorage.SaveReadyState()` ，该函数从 `raft.Ready` 中获取数据，将其写入 `badger raftdb`

> 这里需要澄清 `Ready` 的角色，这其实是 `Raft` 原子写入的内容，为保证安全，`Raft` 分批次检查变更，并通过 `Ready` 传递给上层 

在这里我们只需要关心与 `RaftLocalState` 相关的部分，不关注上层的 `kv/region`

> 注意使用 `WriteBatch` 与 `badger` 进行交互

