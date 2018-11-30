# 注意

参考：[wiz笔记](http://4d3810a1.wiz03.com/share/s/1de12x0ssAHg2vQr4b1tgYE_0M05VM0rIAQ82PQw8k0eEr21)


此文件夹使用phoenix来读取和写入Hbase

一般是前台使用java+JDBC查询。 存储是spark处理，然后直接写入。

## Phoenix数据类型
https://blog.csdn.net/jiangshouzhuang/article/details/52400722



---

[HbaseUtil](HbaseUtil.java) ， [TestHbaseUtil](TestHbaseUtil.java)

Hbase详细工具类，用来直接操作Hbase以及使用Phoenix操作数据库，使用
phoenix-4.10.0.2.6.0.3-8-client.jar包。

[PhoenixUtil](PhoenixUtil.java)

全部是通过Phoenix来操作Hbase（删除了直接操作Hbase）

[PhoenixUtil](PhoenixUtil.java)，[TestPhoenixQueryUtil](TestPhoenixQueryUtil.java)

只是用来查询，使用的phoenix-4.10.0.2.6.0.3-8-thin-client.jar包，避免了与前端的jar包冲突。

[TestSpark01.scala](TestSpark01.scala)， [TestSpark02.java](TestSpark02.java)

使用spark通过phoenix直接操作Hbase。

---

## 总结
前端只是用来查询使用[TestPhoenixQueryUtil](TestPhoenixQueryUtil.java)

后端用来保存DF使用[Save2Hbase](Save2Hbase.scala)








