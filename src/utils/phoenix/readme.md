# 注意

此文件夹使用phoenix来读取和写入Hbase

    一般是前台使用java+JDBC查询。
    存储是spark处理，然后直接写入。

注意：

    如果想在集群上跑：
    如果phoenix中没有hbase-site.xml的文件，会导致连接不上，所以必须手动添加，
    需要把集群中的/etc/hbase/2.6.0.3-8/0中的hbase-site.xml文件拷贝本地resource(自己新建的)里面，
    然后右键此文件夹--Mark Directory as--Resources Root.

    如果想再次在本地跑，需要把上面的文件Mark Directory as--Excluded.


[scala版用spark操作phoenix](ReadUseSpark.scala)

常用的是下面这个：

[java版用jdbc操作phoenix](ReadUseJdbc.java)


常见错误：
- 错误1：ERROR 2007 (INT09) :**Outdated jars**. Newer Phoenix clients can't communicate with older Phoenix servers.
  - Jar包问题，是因为你用的jar包和集群上的jar包版本不一致。换成集群上的jar包版本。
- 错误2：**INFO** query.ConnectionQueryServicesImpl: **HConnection established**. Stacktrace for informational
  purposes:hconnection-0x43c1b556 java.lang.Thread.getStackTrace.
  - 这个错误不用管，直接跳过就行，对程序没影响。其实如果你只用一个client包的话，这个错误是不显示的。
- 错误3：Exception in thread "main" ：java.sql.SQLException: org.apache.hadoop.hbase.client.RetriesExhaustedException:
  **Can't get the locations**
  - 是因为你没有连接上集群，解决方式是把集群上的hbase.xml文件拿过来。参考“在集群上运行phoenix”那一节。
- 错误4：Exception in thread "main" java.sql.SQLException: org.apache.hadoop.hbase.client.RetriesExhaustedException:
  Failed after attempts=36, exceptions: Tue Sep 18 14:51:46 CST 2018, null, **java.net.SocketTimeoutException:**
  callTimeout=60000, callDuration=68223: row 'SYSTEM:CATALOG,,' on table 'hbase:meta' at
  region=hbase:meta,,1.1588230740, **hostname=bg-demo-03.haiyi.com**,16020,1537237055320, seqNum=0
  - 没有配置hosts，解决方法：修改C:\Windows\System32\drivers\etc\hosts，
    - 配置 172.20.32.211 bg-demo-01.haiyi.com
    - 172.20.32.212 bg-demo-02.haiyi.com
    - 172.20.32.213 bg-demo-03.haiyi.com
  - 如果换集群测试：需要换hbase-site.xml，以及修改hosts文件
  - 驱动包只用phoenix-4.10.0.2.6.0.3-8-client.jar这一个就行，其他的用不到。
- 错误5：与前端的jar包冲突
  - 将jar包换掉：将phoenix-4.10.0.2.6.0.3-8-client.jar（91M，集成了特别多的jar包）改为phoenix-4.10.0.2.6.0.3-8-thin-client.jar（6M）
  - 将驱动改掉：org.apache.phoenix.queryserver.client.Driver

