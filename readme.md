#  坐席分配接口



## apportion 函数

1、参数

- task_user_quota(必须，表示任务需要的额度)：Dataframe类型

  | key              | description                 | type |
  | ---------------- | --------------------------- | ---- |
  | task_id       | 对应任务id                      | int  |
  | user_number      | 项目待分配用户              | int  |
  | apportion_number | 项目已分配用户，直接全部填0 | int  |

- user_task_value(必须，表示用户在任务下的价值)： Dataframe类型
  | key              | description                 | type |
  | ---------------- | --------------------------- | ---- |
  | user_id          | 用户id                      | int  |
  | task_id          | 对应的任务id                | int  |
  | value            | 用户在该任务下的价值        | float|

- user_must_do_task（非必须，表示某用户必须要参加某项任务）：Dataframe类型

  | key     | description  | type |
  | ------- | ------------ | ---- |
  | user_id | 用户id       | int  |
  | task_id | 对应的任务id  | int  |

2、返回值

- finished_user_pd(表示用户分配的记录)：Dataframe类型
  | key     | description  | type |
  | ------- | ------------ | ---- |
  | user_id | 用户id       | int  |
  | task_id | 分配到的任务id  | int  |

- task_user_quota(表示不同任务的分配情况)：Dataframe类型
  | key     | description  | type |
  | ------- | ------------ | ---- |
  | task_id | 任务id        | int  |
  | user_number | 该任务需要的用户数量  | int  |
  | apportion_number | 该任务目前分配到的用户数量 | int  |
  | description | 算法运行完成后该任务分配状态的描述 | string |

- all_value（所有用户分配完毕后的总价值）：int
