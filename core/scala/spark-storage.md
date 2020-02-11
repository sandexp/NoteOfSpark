## **spark-storage**

---

1.  [MemoryStore.scala](# MemoryStore)
2.  [BlockException.scala](# BlockException)
3.  [BlockId.scala](# BlockId)
4.  [BlockInfoManager.scala](# BlockInfoManager)
5.  [BlockManager.scala](# BlockManager)
6.  [BlockManagerId.scala](# BlockManagerId)
7.  [BlockManagerManagedBuffer.scala](# BlockManagerManagedBuffer)
8.  [BlockManagerMaster.scala](# BlockManagerMaster)
9.  [BlockManagerMasterEndpoint.scala](# BlockManagerMasterEndpoint)
10.  [BlockManagerMasterHeartbeatEndpoint.scala](# BlockManagerMasterHeartbeatEndpoint)
11.  [BlockManagerMessages.scala](# BlockManagerMessages)
12.  [BlockManagerSlaveEndpoint.scala](# BlockManagerSlaveEndpoint)
13.  [BlockManagerSource.scala](# BlockManagerSource)
14.  [BlockNotFoundException.scala](# BlockNotFoundException)
15.  [BlockReplicationPolicy.scala](# BlockReplicationPolicy)
16.  [BlockUpdatedInfo.scala](# BlockUpdatedInfo)
17.  [DiskBlockManager.scala](# DiskBlockManager)
18.  [DiskBlockObjectWriter.scala](# DiskBlockObjectWriter)
19.  [DiskStore.scala](# DiskStore)
20.  [FileSegment.scala](# FileSegment)
21.  [RDDInfo.scala](# RDDInfo)
22.  [ShuffleBlockFetcherIterator.scala](# ShuffleBlockFetcherIterator)
23.  [StorageLevel.scala](# StorageLevel)
24.  [StorageStatus.scala](# StorageStatus)
25.  [TopologyMapper.scala](# TopologyMapper)
26.  [基础拓展](# 基础拓展)

---

#### MemoryStore

#### BlockException

```scala
private[spark]
case class BlockException(blockId: BlockId, message: String) extends Exception(message)
介绍: 存储块异常信息
```

#### BlockId

#### BlockInfoManager

#### BlockManager

#### BlockManagerId

#### BlockManagerManagedBuffer

#### BlockManagerMaster

#### BlockManagerMasterEndpoint

#### BlockManagerMessages

#### BlockManagerSlaveEndpoint

#### BlockManagerSource

#### BlockNotFoundException

#### BlockReplicationPolicy

#### BlockUpdatedInfo

#### DiskBlockManager

#### DiskBlockObjectWriter

#### DiskStore

#### FileSegment

#### RDDInfo

#### ShuffleBlockFetcherIterator

#### StorageLevel

#### TopologyMapper

#### 基础拓展