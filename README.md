#ScuBigData

四川大学拓思爱诺购物数据分析项目

## 需求1
通过指定taskid，从数据库查询任务相关信息，包括starttime，taskParam等，要求`通过数据库连接池的方式获取连接`，连接池在整个程序的运行过程中只有一份，且链接数量固定（Jdbc+Mysql）

## 需求2
在指定日期范围内，按照session粒度进行数据聚合。要求聚合后的pair RDD的元素是<k:String,v:String>,	其中k=sessionid，v的格式如下：

`sessionid=value|searchword=value|clickcaterory=value|age=value|professional=value|city=value|sex=value`

使用(Spark RDD + Sql)

## 需求3
根据用户的查询条件，一个 或者多个：年龄范围，职业（多选），城市（多选），搜索词（多选），点击品类（多选）进行数据过滤

`注意`：session时间范围是必选的。返回的结果RDD元素格式同上，使用(Spark RDD + Sql)

## 需求4

实现自定义累加器完成多个聚合统计业务的计算，统计业务包括访问时长：1~3秒，4~6秒，7~9秒，10~30秒，30~60秒的session访问量统计，
访问步长：1~3个页面，4~6个页面等步长的访问统计

` 注意`：业务较为复杂,需要使用多个广播变量时，就会使得程序变得非常复杂，不便于扩展维护（Spark Accumulator）

## 需求5
对通过筛选条件的session，按照各个品类的点击、下单和支付次数，降序排列，获取前10个热门品类。

优先级：点击，下单，支付。二次排序（Spark）
