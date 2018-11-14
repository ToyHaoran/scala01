介绍spark常用知识

practice文件夹是存放**练习文件代码**的

testfile文件夹是用来存放**测试文件**的, 注意在编译的时候Exclude，
否则jar包太大，但是注意所有指向这个文件的路径都会xxxxNotFound异常。


[介绍共享变量](SharedVariables.scala)




# 其他
运行期间查看spark UI 可以在代码的后面加
Thread.sleep(10 * 60 * 1000) // 挂住 10 分钟

关掉json检查：Setting--Editor--inspections--Json


# 一些bug
- org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.ipc.StandbyException): Operation category READ is not supported in state standby
  - IP地址没写对，连接到了错误的节点服务器上
  - 检查你连接HDFS的地址
-
