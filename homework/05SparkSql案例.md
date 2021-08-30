# SparkSql项目实战

## 数据准备

首先在 Hive 中创建表,，并导入数据。一共有 3 张表： 1 张用户行为表，1 张城市表，1 张产品表

```sql
CREATE TABLE `user_visit_action`(
`date` string,
`user_id` bigint,
`session_id` string,
`page_id` bigint,
`action_time` string,
`search_keyword` string,
`click_category_id` bigint,
`click_product_id` bigint,
`order_category_ids` string,
`order_product_ids` string,
`pay_category_ids` string,
`pay_product_ids` string,
`city_id` bigint)
row format delimited fields terminated by '\t';
load data local inpath '/root/spark-sql/user_visit_action.txt' into table user_visit_action;

CREATE TABLE `product_info`(
`product_id` bigint,
`product_name` string,
`extend_info` string)
row format delimited fields terminated by '\t';
load data local inpath '/root/spark-sql/product_info.txt' into table product_info;

CREATE TABLE `city_info`(
`city_id` bigint,
`city_name` string,
`area` string)
row format delimited fields terminated by '\t';
load data local inpath '/root/spark-sql/city_info.txt' into table city_info;
```

## 需求

### 个区域热门商品Top3

#### 需求简介

这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。

例如：

| **地区** | **商品名称** | **点击次数** | **城市备注**                        |
| -------- | ------------ | ------------ | ----------------------------------- |
| **华北** | 商品 A       | 100000       | 北京 21.2%，天津  13.2%，其他 65.6% |
| **华北** | 商品 P       | 80200        | 北京 63.0%，太原  10%，其他 27.0%   |
| **华北** | 商品 M       | 40000        | 北京 63.0%，太原  10%，其他 27.0%   |
| **东北** | 商品 J       | 92000        | 大连 28%，辽宁  17.0%，其他 55.0%   |

#### 需求分析

- 查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与Product_info 表连接得到产品名称
- 按照地区和商品 id 分组，统计出每个商品在每个地区的总点击次数
- 每个地区内按照点击次数降序排列
- 只取前三名
- 城市备注需要自定义 UDAF 函数

#### 功能实现

- 连接三张表的数据，获取完整的数据（只有点击）
- 将数据根据地区，商品名称分组
- 统计商品点击次数总和,取Top3
- 实现自定义聚合函数显示备注