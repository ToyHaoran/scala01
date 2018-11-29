# 注意

参考：[wiz笔记](http://4d3810a1.wiz03.com/share/s/1de12x0ssAHg2vQr4b1tgYE_0M05VM0rIAQ82PQw8k0eEr21)


此文件夹使用phoenix来读取和写入Hbase

一般是前台使用java+JDBC查询。 存储是spark处理，然后直接写入。

---

[HbaseUtil](HbaseUtil.java) ， [TestHbaseUtil](TestHbaseUtil.java)

Hbase详细工具类，用来直接操作Hbase以及使用Phoenix操作数据库，使用
phoenix-4.10.0.2.6.0.3-8-client.jar包。

[HbaseUtilThin](HbaseUtilThin.java)

全部是通过Phoenix来操作Hbase（删除了直接操作Hbase）

[PhoenixUtil](PhoenixUtil.java)， [TestPhoenixUtil](TestPhoenixUtil.java)

只是用来查询，使用的phoenix-4.10.0.2.6.0.3-8-thin-client.jar包，避免了与前端的jar包冲突。

[TestSpark01.scala](TestSpark01.scala)， [TestSpark02.java](TestSpark02.java)

使用spark通过phoenix直接操作Hbase。

---










