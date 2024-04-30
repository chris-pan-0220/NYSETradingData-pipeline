# 导入必要的库和测试对象
import pytest
import pandas as pd
import saspy
from sqlalchemy.orm import sessionmaker
from nyse_data_pipeline.analysis import (
    get_bbo_task_for_register_by_orm,
    get_except_bbo_task_for_register_by_orm,
    connect_to_mysql_by_orm,
    register_task_by_orm,
    unregister_task_by_orm,
    delete_task_by_orm,
    load_task
)
from nyse_data_pipeline.schema import Task, TaskCategory
from nyse_data_pipeline.config import Config
import datetime
import uuid

CONFIG = Config()
config = CONFIG.get_config()
sas_config = config['sas']
mysql_config = config['mysql']

# 编写测试函数
def test_get_bbo_task_and_register():
    # 连接到测试数据库
    
    engine = connect_to_mysql_by_orm(host=mysql_config['host'],
                                        user=mysql_config['user'],
                                        password=mysql_config['password'],
                                        database=mysql_config['database']['test'])
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 清理数据库中的数据
        session.query(Task).delete()
        session.commit()

        # 添加测试用例数据
        c1 = session.query(TaskCategory.id) \
            .filter(TaskCategory.name == 'SPLITS_US_ALL_BBO') \
            .scalar()
        c2 = session.query(TaskCategory.id) \
            .filter(TaskCategory.name == 'EQY_US_ALL_BBO') \
            .scalar()
        
        groupuuid = str(uuid.uuid4())

        task_list = [
            Task(taskCategoryId=c1,
                filename='file1.gz',
                alphabet='A',
                date=datetime.date(2024, 5,20),
                groupuuid=None),
            Task(taskCategoryId=c2,
                filename='file2.gz',
                alphabet=None,
                date=datetime.date(2024, 5, 20),
                groupuuid=None),
            Task(taskCategoryId=c1,
                filename='file3.gz',
                alphabet='A',
                date=datetime.date(2024, 5,20),
                groupuuid=groupuuid),
        ]

        session.add_all(task_list)

        # 调用被测试函数
        result = get_bbo_task_for_register_by_orm(session)

        # 进行断言检查
        assert len(result) == 1  # 这里的 expected_length 是你期望的结果长度
        assert result[0] == task_list[0]

        new_groupuuid, n = register_task_by_orm(session=session,
                                                rows=result)
        print('group uuid: ', new_groupuuid)

        assert result[0].groupuuid == new_groupuuid
        assert n == 1 # 1

        # TODO: why there is a bug ?
        # rows = session.query(Task) \
        #     .filter(Task.groupuuid.is_(new_groupuuid)) \
        #     .all()
        
        # assert len(rows) == 1

    finally:
        session.query(Task).delete()
        session.commit()
        session.close()

def test_get_except_bbo_task_and_register():
    # 连接到测试数据库
    
    engine = connect_to_mysql_by_orm(host=mysql_config['host'],
                                        user=mysql_config['user'],
                                        password=mysql_config['password'],
                                        database=mysql_config['database']['test'])
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 清理数据库中的数据
        session.query(Task).delete()
        session.commit()

        # 添加测试用例数据
        c1 = session.query(TaskCategory.id) \
            .filter(TaskCategory.name == 'SPLITS_US_ALL_BBO') \
            .scalar()
        
        category = [
            'EQY_US_ALL_REF_MASTER',
            'EQY_US_ALL_BBO_ADMIN',
            'EQY_US_ALL_TRADE_ADMIN',
            'EQY_US_ALL_ADMIN',
            'EQY_US_ALL_TRADE',
            'EQY_US_ALL_NBBO'
        ]

        categoryId = [
            session.query(TaskCategory) \
                .filter(TaskCategory.name == c) \
                .first() \
                .id \
                for c in category
        ]
        
        d1 = datetime.date(2024, 1, 20)
        t1 = [
            Task(taskCategoryId=c1,
                filename='file1.gz',
                alphabet='A',
                date=d1,
                groupuuid=None)
        ]

        d2 = d1
        t2 = [ # target 
            Task(taskCategoryId=int(c),
                filename=f'file2-{c}.gz',
                alphabet=None,
                date=d2,
                groupuuid=None)
            for c in categoryId
        ]

        # print(t2)

        d3 = datetime.date(2024, 3, 20)
        t3 = [
            Task(taskCategoryId=c,
                filename=f'file3-{c}.gz',
                alphabet=None,
                date=d3,
                groupuuid=None)
            for c in categoryId
        ]
        
        groupuuid4 = str(uuid.uuid4())
        d4 = datetime.date(2024, 4, 20)
        t4 = [
            Task(taskCategoryId=c,
                filename=f'file4-{c}.gz',
                alphabet='A',
                date=d4,
                groupuuid=groupuuid4)
            for c in categoryId
        ]

        d5 = datetime.date(2024, 5, 20)
        groupuuid5 = str(uuid.uuid4())
        t5 = [
           Task(taskCategoryId=categoryId[0],
                filename=f'file5-{categoryId[0]}.gz',
                alphabet='A',
                date=d5,
                groupuuid=groupuuid5) 
        ]

        session.add_all(t1)
        session.add_all(t2)
        session.add_all(t3)
        session.add_all(t4)
        session.add_all(t5)

        # 调用被测试函数
        result = get_except_bbo_task_for_register_by_orm(session=session,
                                                         category=category,
                                                         date=d2)
        
        assert len(result) == len(t2)
        for r in result:
            ok = False
            for t in t2:
                if r == t:
                    ok = True 
            assert ok

        groupuuid2, n = register_task_by_orm(session=session,
                                             rows=result)
        assert len(result) == n 
        for r in result: 
            assert r.groupuuid == groupuuid2

        unregister_ids = [r.id for r in result]
        unregister_n = unregister_task_by_orm(session=session,
                               ids=unregister_ids)
        
        assert len(result) == unregister_n
        for r in result:
            assert r.groupuuid is None

        groupuuid2, n = register_task_by_orm(session=session,
                                             rows=result)
        
        # skip assert 
        delete_ids = unregister_ids
        delete_n = delete_task_by_orm(session=session,
                           ids=delete_ids)
        
        assert len(result) == delete_n

        deleted_result = session.query(Task) \
            .filter(Task.id.in_(delete_ids)) \
            .all()
        assert len(deleted_result) == 0

    finally:
        session.query(Task).delete()
        session.commit()
        session.close()

# TODO: test from t1~t5的所有可能性

def test_load_task():
    sas = saspy.SASsession()
    hr_df = pd.DataFrame({'date': ['20240210', '20240310']})
    hr = load_task(sas, df=hr_df) # load n1 tasks
    print(hr.head())
    sas.disconnect() 

def test_sas_error():
    sas = saspy.SASsession()
    sas.check_error_log = False
    sas_code = """
        options errorabend;
        data ErrorExample;
            length numVar 8;
            numVar = ; /* 將字符值賦給數值變量 */
        run;
    """
    # 執行 SAS 程式碼
    error = False 
    try:
        sas.submit(sas_code)
    except Exception:
        error = True 

    assert error == True
    sas.disconnect() 



if __name__ == "__main__":
   pytest.main([__file__, '-v'])
