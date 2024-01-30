import numpy as np
import pandas as pd
    
def apportion_description(data):
    """
    @param data: 任务用户分配记录中的一行数据

    @return: description: 任务分配描述
    """
    if data['apportion_number'] == data['user_number']:
        description = "项目恰好分配完成"
    elif data['apportion_number'] < data['user_number']:
        description = "项目未分配完成"
    else:
        description = "项目分配超额"
    return description
    

def apportion_task(user_task_value: pd.DataFrame,
                    task_user_quota: pd.DataFrame=None,
                    user_must_do_task: pd.DataFrame=None,
                )-> (pd.DataFrame, pd.DataFrame):
    """
    @param user_task_value: 所有可分配的用户对任务的价值，根据用户对任务的价值对用户进行排序分配。字段为：user_id, task_id, value
    @param task_user_quota: 任务用户配额，该任务需要分配的用户数量。字段为：task_id，user_number，apportion_number
    @param user_must_do_task: 用户必须完成的任务，该任务必须分配给用户。字段为：user_id, task_id

    @return: finished_user_pd: 分配任务后，变动的用户

    此外，坐席可以执行哪些任务由user_task_value决定，如果user_task_value中没有该任务，则坐席不能执行该任务
    """
    # 任务分配记录
    task_user_quota['apportion_number'] = 0
    # 分配过程中分配满的任务
    filled_tasks = list()
    # 必须分配给规定任务的用户，即再分配中不变的用户
    fixed_user = list()
    # 返回结果，变动的用户
    finished_user_pd = pd.DataFrame(columns=['user_id', 'task_id'])

    # 将只擅长一种任务的用户看作是必须分配到该任务里的用户
    user_fit_task_number = user_task_value.groupby('user_id')['task_id'].count().reset_index()
    user_fit_task_number = user_fit_task_number.rename(columns={'task_id': 'user_fit_number'})
    user_only_one_fit_task = user_fit_task_number.loc[user_fit_task_number['user_fit_number']==1, 'user_id'].unique().tolist()
    user_only_one_fit_task = user_task_value.loc[user_task_value['user_id'].isin(user_only_one_fit_task), ['user_id', 'task_id']].copy()    

    if user_must_do_task is None:
        user_must_do_task = user_only_one_fit_task
    else:
        user_must_do_task = pd.concat([user_must_do_task, user_only_one_fit_task], ignore_index=True)

    # 先获取可用的用户，之后根据能参加的项目的数量对用户进行排序且分配用户
    orial_user_task_value = user_task_value.copy()
    user_task_value = user_task_value.loc[~user_task_value['user_id'].isin(finished_user_pd['user_id'].unique().tolist())]
    # TODO 为什么去掉0之后总价值反倒还会变小
    # user_task_value = user_task_value.loc[user_task_value['value'] > 0]
    user_task_value = user_task_value.sort_values('value', ascending=False)

    # 获取用户参加项目的数量
    user_fit_task_number = user_task_value.groupby('user_id')['task_id'].count().reset_index()
    user_fit_task_number = user_fit_task_number.rename(columns={'task_id': 'user_fit_number'})
    user_fit_task_number = user_fit_task_number.sort_values('user_fit_number',ascending=True)

    # 第一轮按照适合任务多少进行分配，优先分配用户到最擅长的任务中，所以对于不擅长任何任务的用户不进行分配
    for index in range(user_fit_task_number.shape[0]):
        user_id = user_fit_task_number.loc[index, 'user_id']
        task_score_row = user_task_value.loc[user_task_value['user_id']==user_id]
        # task_score_row = task_score_row.sort_values('value', ascending=False)
        if user_fit_task_number.loc[index, 'user_fit_number'] == 0:
            continue
        task_score_row = task_score_row.head(1)[['user_id', 'task_id']].reset_index(drop=True)
        task_id = task_score_row['task_id'].values[0]
        finished_user_pd = pd.concat([finished_user_pd, task_score_row], ignore_index=True)
        task_user_quota.loc[task_user_quota['task_id'] == task_id, 'apportion_number'] += 1
        # 更新用户适配项目数量
        task_need_user_number = task_user_quota.loc[task_user_quota['task_id']==task_id, 'user_number'].values[0]
        task_apportion_number = task_user_quota.loc[task_user_quota['task_id']==task_id, 'apportion_number'].values[0]
        if task_apportion_number >= task_need_user_number and task_id not in filled_tasks:
            need_adjusted_user = user_task_value.loc[user_task_value['task_id']==task_id, 'user_id'].unique().tolist()
            user_fit_task_number.loc[user_fit_task_number['user_id'].isin(need_adjusted_user), 'user_fit_number'] = \
            user_fit_task_number.loc[user_fit_task_number['user_id'].isin(need_adjusted_user),'user_fit_number'].apply(lambda x: x-1 if x>0 else 0)
            filled_tasks.append(task_id)
    
    # 第二轮分配，将分配超额的任务末尾的用户分配给其他未分配满的任务
    excess_tasks = task_user_quota.loc[task_user_quota['apportion_number'] > task_user_quota['user_number'], 'task_id'].unique().tolist()
    for excess_task in excess_tasks:
        # 获取超额分配项目的超出额度数量且不是必须分配到该项目的用户
        excess_number = task_user_quota.loc[task_user_quota['task_id']==excess_task, 'apportion_number'].values[0] - \
                        task_user_quota.loc[task_user_quota['task_id']==excess_task, 'user_number'].values[0]
        # 获取超额且不是必须分配到该项目的用户
        excess_users = finished_user_pd.loc[((finished_user_pd['task_id']==excess_task) & (~finished_user_pd['user_id'].isin(fixed_user))), 'user_id'].unique().tolist()
        # 获取相关用户在该项目的价值
        excess_users_value = user_task_value.loc[(user_task_value['user_id'].isin(excess_users) & (user_task_value['task_id']==excess_task))]
        excess_users_value = excess_users_value.sort_values('value', ascending=False)
        excess_users = excess_users_value.iloc[-excess_number:]['user_id'].unique().tolist()
        for excess_user in excess_users:
            # 获取该用户擅长的且未分配满的项目
            excess_user_unfilled_task_value = user_task_value.loc[((user_task_value['user_id'] == excess_user) & (~user_task_value['task_id'].isin(filled_tasks)))]
            if excess_user_unfilled_task_value.empty:
                # baseline里先剔除这个用户的分配
                # 替换最擅长任务里最差且成功率低于自己的用户
                excess_user_task_value = user_task_value.loc[user_task_value['user_id'] == excess_user]
                best_proference_task = excess_user_task_value.head(1)[['user_id', 'task_id']].reset_index(drop=True)
                # 拿到该用户最擅长任务里的最不擅长该任务的用户
                exchange_target_user_record = finished_user_pd.loc[((finished_user_pd['task_id'] == best_proference_task['task_id'].values[0]) & \
                                                                    (~finished_user_pd['user_id'].isin(fixed_user)))].copy()
                exchange_target_user_record = pd.merge(exchange_target_user_record, user_task_value,on=['user_id', 'task_id'] ,how='left')
                exchange_target_user_record = exchange_target_user_record.sort_values('value', ascending=True)
                exchange_target_user_record = exchange_target_user_record.head(1)
                finished_user_pd.loc[finished_user_pd['user_id']==excess_user, 'task_id'] = exchange_target_user_record['task_id'].values[0]
                # 删除被强占用户的分配记录
                indices_to_drop = finished_user_pd[finished_user_pd['user_id'] == exchange_target_user_record['user_id'].values[0]].index
                finished_user_pd = finished_user_pd.drop(indices_to_drop)
                task_user_quota.loc[task_user_quota['task_id']==excess_task, 'apportion_number'] -= 1
            else:
                task_score_row = excess_user_unfilled_task_value.head(1)[['user_id', 'task_id']].reset_index(drop=True)
                new_task_id = task_score_row['task_id'].values[0]
                # 重新分配用户
                finished_user_pd.loc[finished_user_pd['user_id']==excess_user, 'task_id'] = new_task_id
                # 更新分配额度
                task_user_quota.loc[task_user_quota['task_id']==new_task_id, 'apportion_number'] += 1
                task_user_quota.loc[task_user_quota['task_id']==excess_task, 'apportion_number'] -= 1
                # 判断新分配的任务是否分配满
                task_need_user_number = task_user_quota.loc[task_user_quota['task_id']==new_task_id, 'user_number'].values[0]
                task_apportion_number = task_user_quota.loc[task_user_quota['task_id']==new_task_id, 'apportion_number'].values[0]
                if task_apportion_number == task_need_user_number:
                    filled_tasks.append(new_task_id)

    # 第三轮分配，将前两轮未分配的用户分配给未分配满的任务, 优先分配未
    free_users = user_task_value.loc[~user_task_value['user_id'].isin(finished_user_pd['user_id'].tolist()), 'user_id'].unique().tolist()
    # 计算未分配满的任务，以及差的人数
    # unfilled_tasks = task_user_quota.loc[~task_user_quota['task_id'].isin(filled_tasks), 'task_id'].tolist()
    unfilled_tasks = task_user_quota.loc[~task_user_quota['task_id'].isin(filled_tasks)].copy()
    unfilled_tasks['need_number'] = unfilled_tasks['user_number'] - unfilled_tasks['apportion_number']
    unfilled_tasks = unfilled_tasks.sort_values('need_number', ascending=False) 

    for _, unfilled_task_row in unfilled_tasks.iterrows():
        unfilled_task_id = unfilled_task_row['task_id']
        unfilled_task_need_number = unfilled_task_row['need_number']
        free_user_numbers = len(free_users)
        if free_user_numbers == 0:
            break
        apporotion_numbers = min(free_user_numbers, unfilled_task_need_number)
        apportion_users = np.random.choice(free_users, apporotion_numbers, replace=False)
        # 将选择用户排除
        free_users = list(set(free_users) - set(apportion_users))
        # 获取该任务的用户
        apportion_users_pd = pd.DataFrame(
            {
                'user_id': apportion_users,
                'task_id': [unfilled_task_id] *  apporotion_numbers 
            }
        )
        # 更新分配记录
        finished_user_pd = pd.concat([finished_user_pd, apportion_users_pd], ignore_index=True)
        # 更新任务分配数据
        task_user_quota.loc[task_user_quota['task_id']==unfilled_task_id, 'apportion_number'] += apporotion_numbers
        # 更新分配满的任务
        if unfilled_task_need_number == apporotion_numbers:
            filled_tasks.append(unfilled_task_id)
        else:
            break

    # 第四轮分配，将剩余的用户随机分配给自己擅长的任务
    for free_user in free_users:
        # 获取用户擅长的任务
        final_user_task_value = user_task_value.loc[user_task_value['user_id']==free_user]
        final_user_task_value = final_user_task_value.sort_values('value', ascending=False)
        final_user_task_value = final_user_task_value.head(1)[['user_id', 'task_id']].reset_index(drop=True)
        task_id = final_user_task_value['task_id'].values[0]
        finished_user_pd = pd.concat([finished_user_pd, final_user_task_value], ignore_index=True)
        task_user_quota.loc[task_user_quota['task_id']==task_id, 'apportion_number'] += 1
    task_user_quota['description'] = task_user_quota.apply(apportion_description, axis=1)   
    all_value = pd.merge(finished_user_pd, orial_user_task_value, on=['user_id', 'task_id'], how='left')
    # all_value.to_excel('./no_zero_all_value.xlsx')
    value = all_value['value'].sum() 
    return finished_user_pd, task_user_quota, value

if __name__ == "__main__":
    user_task_quota = pd.read_excel('./task_user_quota.xlsx')
    user_task_value = pd.read_excel('./user_task_value.xlsx')
    finished_user_pd, user_task_quota, all_value = apportion_task(user_task_value, user_task_quota, None)
    print(finished_user_pd.sort_values('user_id', ascending=False))
    print(user_task_quota)
    print(all_value * 100)



