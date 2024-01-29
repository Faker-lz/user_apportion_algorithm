#  坐席分配接口



## apportion 函数

1、参数

- user_policy_sr(非必须，提供表示刷新配额)：Dataframe类型

  | key              | description                 | type |
  | ---------------- | --------------------------- | ---- |
  | project_id       | 项目id                      | int  |
  | user_number      | 项目待分配用户              | int  |
  | apportion_number | 项目已分配用户，直接全部填0 | int  |

- user_online_state（非必须，提供表示用户上下线的刷新更新）：Dataframe类型

  | key     | description  | type |
  | ------- | ------------ | ---- |
  | user_id | 在线的用户id | int  |

2、返回值类型

返回此次分配修改过的用户，具体格式如下：

| key        | description        | type   |
| ---------- | ------------------ | ------ |
| user_id    | 修改后的用户id     | int    |
| project_id | 分配该用户的项目id | int    |
| ctime      | 分配时间           | string |
| state      | 最新状态           | int    |

