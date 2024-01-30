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
                ):
    """
    @param user_task_value: 所有可分配的用户对任务的价值，根据用户对任务的价值对用户进行排序分配。字段为：user_id, task_id, value
    @param task_user_quota: 任务用户配额，该任务需要分配的用户数量。字段为：task_id，user_number，apportion_number
    @param user_must_do_task: 用户必须完成的任务，该任务必须分配给用户。字段为：user_id, task_id

    @return: finished_user_pd: 分配任务后用户分配到的对应的任务id
    @return: task_user_quota: 分配的额度和描述
    @return: all_value: 分配后总的价值

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

    # 优先将任务分配给必须完成任务的用户
    if not user_must_do_task.empty:
        # 分配任务给必须完成任务的用户
        finished_user_pd = user_must_do_task
        # 更新任务分配数据
        task_apportion_number = finished_user_pd.groupby('task_id')['user_id'].count()
        task_user_quota['apportion_number'] = task_user_quota['task_id'].map(task_apportion_number).fillna(0).astype(int)
        fixed_user = user_must_do_task['user_id'].unique().tolist()
        # 更新分配满的任务
        filled_tasks = task_user_quota.loc[task_user_quota['apportion_number'] >= task_user_quota['user_number'], 'task_id'].unique().tolist()

    # 先获取可用的用户，之后根据能参加的项目的数量对用户进行排序且分配用户
    orial_user_task_value = user_task_value.copy()
    user_task_value = user_task_value.loc[~user_task_value['user_id'].isin(finished_user_pd['user_id'].unique().tolist())]
    user_task_value = user_task_value.sort_values('value', ascending=False)

    # 第一轮优先分配用户到最擅长的任务中，所以对于不擅长任何任务的用户不进行分配
    # 对于超额分配的用户并没有真正分配到finished_user_pd中，而是将其记录在超额分配的用户列表里
    excess_users = list()
    unassigned_user_list = user_task_value['user_id'].unique().tolist()
    for user_id in unassigned_user_list:
        task_score_row = user_task_value.loc[user_task_value['user_id']==user_id]
        task_score_row = task_score_row.sort_values('value', ascending=False)
        task_score_row = task_score_row.head(1)[['user_id', 'task_id']].reset_index(drop=True)
        task_id = task_score_row['task_id'].values[0]
        # 获取最擅长的任务分配情况
        task_need_user_number = task_user_quota.loc[task_user_quota['task_id']==task_id, 'user_number'].values[0]
        task_apportion_number = task_user_quota.loc[task_user_quota['task_id']==task_id, 'apportion_number'].values[0]
        if task_need_user_number == task_apportion_number:
            excess_users.append(user_id)
        else:
            finished_user_pd = pd.concat([finished_user_pd, task_score_row], ignore_index=True)
            task_user_quota.loc[task_user_quota['task_id'] == task_id, 'apportion_number'] += 1
            # 如果这次分配导致任务被分配满，将任务更新为已满状态
            if task_apportion_number + 1 == task_need_user_number:
                filled_tasks.append(task_id)

    # 第一轮分配完成后用户分配状态：
    # 1 必须分配且只擅长一种任务的用户被分配到了指定的任务
    # 2 如果在遍历到用户时，该用户最擅长任务没分配满则，用户分配到自己擅长的任务下，否则暂存到excess_users中，表示超额，并未真实分配
    
    # 第二轮分配，将超额分配的用户进行再分配，具体逻辑：
    # 如果用户最擅长的任务没有分配完，那么用户直接分配到该任务中
    # 如果用户最擅长的任务分配完成，同时最擅长的任务里提供价值最低的用户所提供的价值低于该用户的价值，则替换，否则不进行替换等待低三四轮分配
    # 如果有用户被替换下来则加入到未分配的用户队列里，等待重新分配
    while len(excess_users) > 0:
        excess_user = excess_users.pop(0)
        # 获取该用户擅长的且未分配满的项目
        excess_user_unfilled_task_value = user_task_value.loc[((user_task_value['user_id'] == excess_user) & (~user_task_value['task_id'].isin(filled_tasks)))]
        # 如果所有擅长的任务都分配满了，则查看最擅长的任务里最差的用户提供的价值是否高于该用户，如果是的话就替换，否则这一轮不再分配该用户
        if excess_user_unfilled_task_value.empty:
            # 替换最擅长任务里最差且成功率低于自己的用户
            excess_user_task_value = user_task_value.loc[user_task_value['user_id'] == excess_user]
            best_proference_task = excess_user_task_value.head(1)
            # 拿到该用户最擅长任务里的最不擅长该任务且不在必须分配名单里的用户
            # TODO 下一步优化目标，用户擅长的全部的任务，目前在第二轮分配时只考虑了用户最擅长的任务
            exchange_target_user_record = finished_user_pd.loc[((finished_user_pd['task_id'] == best_proference_task['task_id'].values[0]) & \
                                                                (~finished_user_pd['user_id'].isin(fixed_user)))].copy()
            exchange_target_user_record = pd.merge(exchange_target_user_record, user_task_value,on=['user_id', 'task_id'] ,how='left')
            exchange_target_user_record = exchange_target_user_record.sort_values('value', ascending=True)
            exchange_target_user_record = exchange_target_user_record.head(1)
            # 如果优于目标用户，则进行抢占，并将被抢占的目标用户加入到excess_users列表中
            if exchange_target_user_record['value'].values[0] < best_proference_task['value'].values[0]:
                exchange_user_id = exchange_target_user_record['user_id'].values[0]
                finished_user_pd.loc[finished_user_pd['user_id']==exchange_user_id, 'user_id'] = excess_user
                # 将被替换的用户加入到excess_users队列中
                excess_users.append(exchange_user_id)
            # TODO 未来可能的优化方向，如果没有目标怎么办？
        else:
            # TODO 是否妥当 如果擅长的价值为0 则在下一轮将其随机分配
            if excess_user_unfilled_task_value['value'].values[0] != 0:
                task_score_row = excess_user_unfilled_task_value.head(1)[['user_id', 'task_id']].reset_index(drop=True)
                new_task_id = task_score_row['task_id'].values[0]
                # 重新分配用户
                finished_user_pd = pd.concat([finished_user_pd, task_score_row], ignore_index=True)
                # 更新分配额度
                task_user_quota.loc[task_user_quota['task_id']==new_task_id, 'apportion_number'] += 1
                # 判断新分配的任务是否分配满
                task_need_user_number = task_user_quota.loc[task_user_quota['task_id']==new_task_id, 'user_number'].values[0]
                task_apportion_number = task_user_quota.loc[task_user_quota['task_id']==new_task_id, 'apportion_number'].values[0]
                if task_apportion_number == task_need_user_number:
                    filled_tasks.append(new_task_id)
        
    # 第二轮分配完用户的状态：
    # 1. 必须分配的用户分配到了相应的任务里
    # 2. 对于第一轮所有超额分配的用户，如果他擅长的任务没有分配满，那么他被分配到了没有分配满的
    # 3. 如果他擅长的任务都分配满了，而且他最擅长任务里的表现最差的用户没有比他还差，那么他将被分配到该任务，并且会将踢出用户重新调整，否则该用户将处于未分配状态

    # 第三轮分配，将前两轮未分配的用户分配给未分配满的任务, 优先分配未
    free_users = user_task_value.loc[~user_task_value['user_id'].isin(finished_user_pd['user_id'].tolist()), 'user_id'].unique().tolist()
    # 计算未分配满的任务，以及差的人数
    unfilled_tasks = task_user_quota.loc[~task_user_quota['task_id'].isin(filled_tasks), 'task_id'].tolist()
    unfilled_tasks = task_user_quota.loc[task_user_quota['task_id'].isin(unfilled_tasks)].copy()
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

    # 第四轮分配，将剩余的用户分配给自己擅长的任务
    for free_user in free_users:
        # 获取用户擅长的任务
        final_user_task_value = user_task_value.loc[user_task_value['user_id']==free_user]
        final_user_task_value = final_user_task_value.head(1)[['user_id', 'task_id']].reset_index(drop=True)
        task_id = final_user_task_value['task_id'].values[0]
        finished_user_pd = pd.concat([finished_user_pd, final_user_task_value], ignore_index=True)
        task_user_quota.loc[task_user_quota['task_id']==task_id, 'apportion_number'] += 1
    task_user_quota['description'] = task_user_quota.apply(apportion_description, axis=1)     
    all_value = pd.merge(finished_user_pd, orial_user_task_value, on=['user_id', 'task_id'], how='left')
    all_value = all_value['value'].sum()
    return finished_user_pd, task_user_quota, all_value

if __name__ == "__main__":
    user_task_quota = pd.read_excel('./task_user_quota.xlsx')
    user_task_value = pd.read_excel('./user_task_value.xlsx')
    finished_user_pd, user_task_quota, all_value = apportion_task(user_task_value, user_task_quota, None)
    print(finished_user_pd.sort_values('user_id'))
    print(user_task_quota)
    print(all_value * 100)



