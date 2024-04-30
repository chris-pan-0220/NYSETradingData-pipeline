import uuid
import saspy
import pandas as pd
import logging
import os
import sys
import py7zr
from datetime import date as DATE
from datetime import datetime

from urllib.parse import quote_plus
from sqlalchemy import create_engine, distinct, inspect
from sqlalchemy.orm import sessionmaker, Session

from nyse_data_pipeline.config import Config
from nyse_data_pipeline.logger import Logger
from nyse_data_pipeline.schema import Task, TaskCategory
from nyse_data_pipeline.exception import (
    MYSQLConnectionError,
    GetTaskError,
    RegisterTaskError,
    UnRegisterTaskError,
    DeleteTaskError,
    LoadTaskError,
    BBOTaskFailError,
    ExceptBBOTaskFailError,
    SasProgramExecuteError
)

# TODO: 改為獲取共同的logger
# logger = Logger().get_logger()

# def connect_to_mysql():
#     # TODO: 改為傳入host, user, password, database
#     CONFIG = Config()
#     config = CONFIG.get_config() 
#     mysql_config = config['mysql']
#     try:
#         connection = mysql.connector.connect(
#             host = mysql_config['host'],
#             user = mysql_config['user'],
#             password = mysql_config['password'],
#             database = mysql_config['database'],
#             auth_plugin = 'mysql_native_password'
#         )
#         return connection
#     except Exception as e:
#         print("exception occured !!!")
#         raise MYSQLConnectionError("connect to mysql error") from e
    
# def connect_to_mysql(host, user, password, database):
#     try:
#         connection = mysql.connector.connect(
#             host = host,
#             user = user,
#             password = password,
#             database = database,
#             auth_plugin = 'mysql_native_password'
#         )
#         return connection
#     except Exception as e:
#         # TODO: exception如何記錄
#         print("exception occured !!!")
#         raise MYSQLConnectionError("connect to mysql error") from e

def connect_to_mysql_by_orm(host, user, password, database):
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{user}:{quote_plus(password)}@{host}/{database}")
        return engine
    except Exception as e:
        raise MYSQLConnectionError("error connect to mysql") from e

# def get_bbo_task(cursor: MySQLCursorAbstract):
#     try:
#         # TODO: 改為ORM
#         cursor.execute("""
#             SELECT t.*
#             FROM task t
#             JOIN taskCategory tc ON t.taskCategoryId = tc.id
#             WHERE tc.name = 'SPLITS_US_ALL_BBO' AND t.groupuuid is NULL
#             FOR UPDATE;
#         """)
#         rows = cursor.fetchall()
#         return rows, cursor.rowcount
#     except Exception as e:
#         raise GetTaskError("get bbo task error") from e

# TODO: doc當中get for update必須要寫出來，因為`with_for_update()`會lock db  
def get_bbo_task_for_register_by_orm(session: Session):
    try: 
        rows = session.query(Task). \
            join(TaskCategory, Task.taskCategoryId == TaskCategory.id). \
            filter(TaskCategory.name == 'SPLITS_US_ALL_BBO').\
            filter(Task.groupuuid.is_(None)).\
            with_for_update().\
            all()
        return rows
    except Exception as e:
        raise GetTaskError("get bbo task error") from e
    
# def get_except_bbo_task_by_date(cursor: MySQLCursorAbstract, date: str):
#     # TODO: exception
#     # TODO: 從config當中的sas區塊，傳入category (source的區塊)
#     category = (
#         'EQY_US_ALL_REF_MASTER',
#         'EQY_US_ALL_BBO_ADMIN',
#         'EQY_US_ALL_TRADE_ADMIN',
#         'EQY_US_ALL_ADMIN',
#         'EQY_US_ALL_TRADE',
#         'EQY_US_ALL_NBBO'
#     )
#     # TODO: 改為ORM
#     cursor.execute("""
#         SELECT t.*
#         FROM task t
#         JOIN taskCategory tc ON t.taskCategoryId = tc.id
#         WHERE tc.name IN %s AND t.groupuuid is NULL AND t.date = %s
#     """, (category, date, ))
#     rows = cursor.fetchall()
#     return rows, cursor.rowcount

def get_except_bbo_task_for_register_by_orm(session: Session, date: DATE, category: list[str]):
    try:
        rows = session.query(Task).\
            join(TaskCategory, Task.taskCategoryId == TaskCategory.id). \
            filter(TaskCategory.name.in_(category)). \
            filter(Task.groupuuid.is_(None)). \
            filter(Task.date == date). \
            all()
        return rows
    except Exception as e:
        raise GetTaskError("get except_bbo task error") from e

# def get_unique_date(cursor: MySQLCursorAbstract):
#     # TODO: 直接使用ORM
#     cursor.execute("""
#         SELECT DISTINCT date
#         FROM task
#         ORDER BY date ASC
#     """)
#     rows = cursor.fetchall()
#     return rows

def get_unique_date_by_orm(session: Session):
    rows = session.query(distinct(Task.date)). \
        order_by(Task.date.asc()). \
        all()
    return rows

def load_task(sas: saspy.SASsession, df: pd.DataFrame):
    try:
        # TODO: 這裡或許可以用來測試sas產生的error
        hr = sas.df2sd(df, 'filelist')
        return hr
    except Exception as e:
        raise LoadTaskError("load task to sas error") from e

# register by ids
# unregister by ids

# def register_task(cursor: MySQLCursorAbstract, ids: tuple[str]):
#     group_uuid = str(uuid.uuid4()) # 生成新的 UUID
#     ids = tuple(ids)
#     # TODO: 檢查IN的使用方式
#     # TODO: 改為ORM就不用記憶這些語法
#     # TODO: 確保ids都有被更新到: NULL空的
#     cursor.execute("""
#         UPDATE task 
#         SET groupuuid = %s
#         WHERE id IN (%s)
#     """, (group_uuid, ids, ))
#     return group_uuid, cursor.rowcount

# TODO: 在外面需要使用 session.commit / rollback
# require Task.groupuuid is NULL
# bbo允許部分註冊失敗 ? 這個是不可能的，因為get_bbo_for_register已經把數據lock起來了
# except_bbo對於每一個date只能同時成功/失敗
def register_task_by_orm(session: Session, rows: list[Task]):
    try: 
        group_uuid = str(uuid.uuid4()) # 生成新的 UUID
        rowcount = 0
        for row in rows:
            if row.groupuuid is None:
                row.groupuuid = group_uuid
                rowcount += 1

        assert(rowcount == len(rows))

        session.commit()
        return group_uuid, rowcount
    except Exception as e:
        raise RegisterTaskError("register tasks error") from e

# TODO: 是否透過groupuuid統一 unregister / delete ?
# 否，因為同一組groupuuid可能會有部分的task失敗，需要指定ids

# def unregister_task(cursor: MySQLCursorAbstract, ids: tuple[str]):
    ids = tuple(ids)
    # TODO: 檢查使用方式
    # TODO: 改為ORM就不用記憶這些語法
    cursor.execute("""
        UPDATE task 
        SET groupuuid = NULL 
        WHERE id IN (%s)
    """, (ids, ))
    return cursor.rowcount 

# TODO: 必須unregister not NULL的物件，不是全部成功就是全部失敗
def unregister_task_by_orm(session: Session, ids: list[str]):
    try:
        rowcount = session.query(Task). \
            filter(Task.id.in_(ids)). \
            filter(Task.groupuuid.is_not(None)). \
            update({'groupuuid': None}, synchronize_session=False)
        
        # It must be equal !!! 由開發的時候決定
        assert(rowcount == len(ids))

        session.commit()
        return rowcount
    except Exception as e:
        raise UnRegisterTaskError("unregister task error") from e

# TODO: 改為ids ?
# def delete_task(cursor: MySQLCursorAbstract, group_uuid: str):
#     cursor.execute("""
#         DELETE FROM task
#         WHERE task.groupuuid = %s
#     """, (group_uuid, ))
#     return cursor.rowcount 

# TODO: 說明delete的task groupuuid必須非None
def delete_task_by_orm(session: Session, ids: list[str]):
    try: 
        rowcount = session.query(Task). \
            filter(Task.id.in_(ids)). \
            filter(Task.groupuuid.is_not(None)). \
            delete() 
        
        assert(rowcount == len(ids))
        
        session.commit()
        return rowcount
    except Exception as e:
        raise DeleteTaskError("get bbo task error") from e
  
def check_7z_integrity(archive_path):
    try:
        with py7zr.SevenZipFile(archive_path, mode='r') as archive:
            archive.testzip()
            """
            Read all the files in the archive and check their CRCs.
            Return the name of the first bad file, or else return None.
            When the archive don’t have a CRC record, it return None.

            ref: https://py7zr.readthedocs.io/en/latest/api.html
            """
            # print(f"{archive_path} 的完整性检查通过，没有损坏的文件。")
        return True
    except py7zr.Bad7zFile as e:
        # print(f"检测到损坏的压缩包：{archive_path}，错误信息：{e}")
        return False 

# TODO: 把df改為object
# TODO: 進一步只傳入df的一個row => check_bbo_task
# def get_bbo_fail_task(df: pd.DataFrame):

#     CONFIG = Config()
#     config = CONFIG.get_config() 
#     # TODO: 把zip_base傳入
#     sas_config = config['sas']
#     # TODO: 檢查文件是否刪除會太麻煩，這個邏輯刪掉
#     sas_data_base = sas_config['result']['bbo']['sas_data']
#     zip_base = sas_config['result']['bbo']['7z']
#     """
#     df: hr_df from rows ( cursor.fetchall() )
#     - cqX_yyyymmdd.7z
#         - cqX_yyyymmdd.sas7bdat
#         - cqX_yyyymmddend.sas7bdat
#     - cqX_yyyymmdd.sas7bdat
#     - cqX_yyyymmddend.sas7bdat
#     """
#     fail_task_id = []
#     # columns: id

#     for _, row in df.iterrows():
#         date = row['date'].replace('-', '')
#         f = [
#             f"{sas_data_base}\cq{row['alphabet']}_{date}.sas7bdat", 
#             f"{sas_data_base}\cq{row['alphabet']}_{date}end.sas7bdat",
#             f"{zip_base}\cq{row['alphabet']}_{date}.7z"
#         ]
#         # print(f)
#         # print(os.path.exists(f[0]))
#         # print(os.path.exists(f[1]))
#         # print(os.path.exists(f[2]))
#         # print(check_7z_integrity(f[2]))
#         if not os.path.exists(f[0]) and not os.path.exists(f[1]) and os.path.exists(f[2]) and check_7z_integrity(f[2]):
#             continue
#         # fail task
#         fail_task_id.append(row['id'])
#         for file in f:
#             if os.path.exists(file):
#                 os.remove(file)
#     return fail_task_id

# TODO: write test
def check_bbo_task(zip_base: str, task: Task):
    path = f"{zip_base}\cq{task.alphabet}_{datetime.strftime(task.date, '%Y%m%d')}.7z"
    if not os.path.exists(path):
        msg = f"Required file {path} does not exist"
        raise BBOTaskFailError(msg) # TODO: 增加exception類別
        # logger.error(f"Required file {path} does not exist")
        # return False 
    if not check_7z_integrity(path):
        msg = f"Required file {path} is not complete. Remove"
        # logger.error(f"Required file {path} is not complete. Remove")
        os.remove(path)
        raise BBOTaskFailError(msg) # TODO: 增加exception類別
        # return False 
    return True   

# TODO: 修改df ? 變成 object
# TODO: 進一步只傳入df的一個row => check_except_bbo_task
# def get_except_bbo_fail_task_by_date(df: pd.DataFrame) -> list[str]:
#     # TODO: 傳入zip_base (應該是result_dir)
#     CONFIG = Config()
#     config = CONFIG.get_config() 
#     sas_config = config['sas']
#     # TODO: tmp的資料檢查是否刪掉會很麻煩，這個不用了
#     tmp1 = sas_config['result']['except_bbo']['tmp1']
#     tmp2 = sas_config['result']['except_bbo']['tmp2']
#     zip_base = sas_config['result']['except_bbo']['7z']
#     """
#     df: hr_df from rows ( cursor.fetchall() ) with column 

#     :EQY_US_ALL_REF_MASTER
#     taq.masterYYYYMMDD.sas7bdat

#     :EQY_US_ALL_BBO_ADMIN
#     - taq.bboadYYYYMMDD.7z
#         - taq.bboadYYYYMMDDend.sas7bdat
#         - taq.bboadYYYYMMDD.sas7bdat

#     :EQY_US_ALL_TRADE_ADMIN
#     - taq.tradeadYYYYMMDD.7z
#         - taq.tradeadYYYYMMDDend.sas7bdat
#         - taq.tradeadYYYYMMDD.sas7bdat

#     :EQY_US_ALL_ADMIN
#     - taq.ctsadYYYYMMDD.7z
#         - taq.ctsadYYYYMMDDend.sas7bdat
#         - taq.ctsadYYYYMMDD.sas7bdat
#         - taq.ctsadmtxYYYYMMDD.sas7bdat 
#     - taq.utpadYYYYMMDD.7z
#         - taq.utpadYYYYMMDDend.sas7bdat
#         - taq.utpadYYYYMMDD.sas7bdat
#         - taq.utpadmtxYYYYMMDD.sas7bdat

#     :EQY_US_ALL_trade, EQY_US_ALL_NBBO
#     - taq.ctqYYYYMMDD.sas7bdat
#     - taq.tspYYYYMMDD.sas7bdat
#     - taq.volYYYYMMDD.sas7bdat
#     - taq.bsYYYYMMDD.sas7bdat
#     """
#     fail_task_date = []

#     for _, row in df.iterrows():
#         date = row['date'].replace('-', '')
#         fs = [
#             f"{zip_base}\\master{date}.sas7bdat",
#             f"{zip_base}\\bboad{date}.7z",
#             f"{zip_base}\\tradead{date}.7z",
#             f"{zip_base}\\ctsad{date}.7z",
#             f"{zip_base}\\utpad{date}.7z",
#             f"{zip_base}\\ctq{date}.sas7bdat",
#             f"{zip_base}\\tsp{date}.sas7bdat",
#             f"{zip_base}\\vol{date}.sas7bdat",
#             f"{zip_base}\\bs{date}.sas7bdat",
#         ]
#         for f in fs:
#             if not os.path.exists(f):
#                 fail_task_date.append(row['date'])
#                 break
#             if '.7z' in f and not check_7z_integrity(f):
#                 fail_task_date.append(row['date'])
#                 break

#     return fail_task_date

def check_except_bbo_task_by_date(zip_base: str, date: DATE):
    date = datetime.strftime(date, "%Y%m%d")
    path_list = [
        f"{zip_base}\\master{date}.sas7bdat",
        f"{zip_base}\\bboad{date}.7z",
        f"{zip_base}\\tradead{date}.7z",
        f"{zip_base}\\ctsad{date}.7z",
        f"{zip_base}\\utpad{date}.7z",
        f"{zip_base}\\ctq{date}.sas7bdat",
        f"{zip_base}\\tsp{date}.sas7bdat",
        f"{zip_base}\\vol{date}.sas7bdat",
        f"{zip_base}\\bs{date}.sas7bdat",
        f"{zip_base}\\ct{date}.7z",
        f"{zip_base}\\nbbo{date}.7z"
    ]
    ok = True
    msg_list = []
    for path in path_list:
        if not os.path.exists(path):
            msg = f"Required file {path} does not exist"
            msg_list.append(msg)
            # logger.error(f"Required file {path} does not exist")
            ok = False
            # raise ExceptBBOTaskFailError(msg)
        if '.7z' in path and not check_7z_integrity(path):
            msg = f"Required file {path} is not complete. Remove"
            msg_list.append(msg)
            # raise ExceptBBOTaskFailError(msg)
            # logger.error(f"Required file {path} is not complete. Remove")
            ok = False
    if not ok:
        for path in path_list:
            if os.path.exists(path):
                os.remove(path)
        err_msg = '\n'
        for msg in msg_list:
            err_msg += f'{msg}\n'
        err_msg += '\n'
        raise ExceptBBOTaskFailError(err_msg)

    return ok

def proc_bbo():
    CONFIG = Config()
    config = CONFIG.get_config() 
    sas_config = config['sas']
    mysql_config = config['mysql']

    # 獲取logger
    logger = logging.getLogger('proc_bbo')
    logger.setLevel(logging.INFO)
    # 創建formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # 创建流处理程序（输出到控制台）
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    # 创建文件处理程序（输出到文件）
    file_handler = logging.FileHandler('bbo.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # 添加处理程序
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    # TODO: 修改job queue註冊/復原/刪除的方式
    # how to ?

    engine = None 
    session = None
    sas = None 
    hr_df = None 
    group_uuid = None
    rows = None

    # sas = saspy.SASsession()
    # sas.disconnect()

    try:
        engine = connect_to_mysql_by_orm(host=mysql_config['host'],
                                         user=mysql_config['user'],
                                         password=mysql_config['password'],
                                         database=mysql_config['database']['prod'])
        Session = sessionmaker(bind=engine)
        session = Session()

        rows = get_bbo_task_for_register_by_orm(session=session) # lock data for register
        
        if len(rows) == 0:
            logger.info(f"Get 0 bbo tasks. Exit")
            return

        group_uuid, n1 = register_task_by_orm(session=session,
                                              rows=rows) # register n1 task
        logger.info(f"Get and register {n1} tasks. groupuuid: {group_uuid}")
        
        inspector = inspect(session.bind)
        task_columns = inspector.get_columns(Task.__tablename__)
        data = {}
        for col in task_columns:
            col_name = col['name']
            data[col_name] = [getattr(task, col_name) for task in session.query(Task).all()]
        
        hr_df = pd.DataFrame(data)

        hr_df['date'] = hr_df['date'].apply(lambda x: datetime.strftime(x, "%Y%m%d"))
        hr_df = hr_df[['date', 'filename', 'alphabet']]

        sas = saspy.SASsession()
        # useless...上次不正常關閉留下來的temporary檔案，work.filelist不能被刪除...
        # r = sas.file_delete('work.filelist')
        # logger.info(f'Delete work.filelist. State: {r["Success"]}. \n\n{r["LOG"]}\n')
        hr = load_task(sas, df=hr_df) # load n1 tasks 
        print(hr.head())
        # sas.disconnect()
        # return
        logger.info(f"Load {n1} tasks to sas session")
        
        program_path = sas_config['task']['bbo']['program']
        with open(program_path, 'r') as file:
            sas_code = file.read()
        logger.info(f"Read sas program {program_path}")

        # 執行sas program
        # TODO: 注意! 要使用options errorabend;才有效果
        try:
            sas.submit(sas_code)
            logger.info("Sas program execute msg: ")
            logger.info("\n\n"+sas.lastlog()+"\n")
        except Exception as e:
            msg = sas.lastlog()
            raise SasProgramExecuteError(f"Fail to execete sas program {program_path}. Log message: \n\n{msg}\n") from e
        
        logger.info(f"Successfully execute sas program {program_path}")
    except:
        logger.error("Error in analysis bbo task", exc_info=True)
        raise
    finally:
        # unregistered fail tasks ; delete finished tasks
        if rows:
            fail_task_ids = []
            ok_task_ids = []
            for row in rows: 
                logger.info(f"Check bbo task. file: {row.filename}")
                try:
                    check_bbo_task(zip_base=sas_config['task']['bbo']['result']['7z'],
                                task=row)
                    logger.info(f"Task {row.filename} is ok")
                    ok_task_ids.append(row.id)
                except BBOTaskFailError as e:
                    logger.error(f"Task {row.filename} fail.")
                    logger.error(e)
                    fail_task_ids.append(row.id)

                # if ok:
                #     logger.info(f"Task {row.filename} is ok")
                #     ok_task_ids.append(row.id)
                # else: 
                #     logger.info(f"Task {row.filename} fail.")
                #     fail_task_ids.append(row.id)

            n1 = 0
            if len(fail_task_ids):
                n1 = unregister_task_by_orm(session=session,
                                            ids=fail_task_ids)
            logger.error(f'Unregister {n1} fail bbo tasks')

            n2 = 0 
            if len(ok_task_ids):
                n2 = delete_task_by_orm(session=session,
                                        ids=ok_task_ids)
            logger.info(f'Delete {n2} finished bbo tasks')
        if sas: 
            sas.disconnect()
        if session:
            session.close()
        if engine:
            engine.dispose()

def proc_except_bbo():
    CONFIG = Config()
    config = CONFIG.get_config() 
    sas_config = config['sas']
    mysql_config = config['mysql']

        # 獲取logger
    logger = logging.getLogger('proc_except_bbo')
    logger.setLevel(logging.INFO)
    # 創建formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # 创建流处理程序（输出到控制台）
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    # 创建文件处理程序（输出到文件）
    file_handler = logging.FileHandler('except_bbo.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # 添加处理程序
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    # TODO: 修改job queue註冊/復原/刪除的方式
    # how to ?

    engine = None 
    session = None
    sas = None 
    hr_df = None 
    group_uuid = None
    rows = None
    rows_list = []
    date_rows = None
    result = None

    try:
        engine = connect_to_mysql_by_orm(host=mysql_config['host'],
                                         user=mysql_config['user'],
                                         password=mysql_config['password'],
                                         database=mysql_config['database']['prod'])
        Session = sessionmaker(bind=engine)
        session = Session()

        date_rows = get_unique_date_by_orm(session=session)

        if len(date_rows) == 0:
            logger.info(f"Get 0 except_bbo tasks. Exit")
            return 
        
        category = sas_config['task']['except_bbo']['source']
        # get categoryId
        categoryId = [
            session.query(TaskCategory) \
                .filter(TaskCategory.name == c) \
                .first() \
                .id
            for c in category
        ]



        sas = saspy.SASsession()
        for date_row in date_rows: 
            date = date_row[0] # yyyy-mm-dd 
            # if date != '2024-04-02': continue
            date = DATE(date.year, date.month, date.day)
            rows = get_except_bbo_task_for_register_by_orm(session=session,
                                                    date=date,
                                                    category=category)
            # print(date)
            # for row in rows:
            #     if row.taskCategoryId
            categoryCnt = {
                'EQY_US_ALL_ADMIN_CTS': 0,
                'EQY_US_ALL_ADMIN_UTP': 0,
                'EQY_US_ALL_BBO_ADMIN': 0,
                'EQY_US_ALL_NBBO': 0,
                'EQY_US_ALL_REF_MASTER': 0,
                'EQY_US_ALL_TRADE': 0,
                'EQY_US_ALL_TRADE_ADMIN': 0
            }
            categorySet = list(categoryCnt.keys())
            ok = True
            for row in rows: 
                cOK = False   
                for c in categorySet:
                    if c == str(row.filename[:-12]):
                        cOK = True
                        categoryCnt[c] += 1
                        break
                if not cOK:
                    ok = False 
                    break
            for c in categorySet:
                if categoryCnt[c] != 1:
                    ok = False
                    break
            # if str(date) == '2024-04-02':
            #     print(date_row)
            #     print(rows)
            #     print(categoryCnt)
            if not ok:
                continue

            
                
            group_uuid, n1 = register_task_by_orm(session=session,
                                 rows=rows)
            logger.info(f"Get and register except_bbo task. category count: {len(categoryId)}, date: {date}, groupuuid: {group_uuid}")
            rows_list.append(rows) # TODO: 處理 unregister / delete task
            
            r = sas.file_delete('work.filelist')
            logger.info(f'Delete work.filelist. State: {r["Success"]}. \n\n{r["LOG"]}\n')
            hr_df = pd.DataFrame({'date': [datetime.strftime(date, "%Y%m%d")]})
            hr = load_task(sas, df=hr_df) # load n1 tasks
            print(hr.head())
            logger.info(f"Load {n1} tasks, {1} except_bbo task to sas session")

            program_path = sas_config['task']['except_bbo']['program']
            with open(program_path, 'r') as file:
                sas_code = file.read()
            logger.info(f"Read sas program {program_path}")

            # 執行sas program
            # TODO: 注意! 要使用options errorabend;才有效果
            try:
                sas.submit(sas_code)
                logger.info("Sas program execute msg: ")
                logger.info("\n\n"+sas.lastlog()+"\n")
            except Exception as e:
                msg = sas.lastlog()
                raise SasProgramExecuteError(f"Fail to execete sas program {program_path}. Log message: \n\n{msg}\n") from e
            
            logger.info(f"Successfully execute sas program {program_path}")
    except:
        logger.error("Error in analysis except_bbo task", exc_info=True)
        raise   
    finally:
        n1 = 0
        n2 = 0
        for rows in rows_list:
            logger.info(f"Check except_bbo task. date: {rows[0].date}")
            
            ok = True 
            try: 
                check_except_bbo_task_by_date(zip_base=sas_config['task']['except_bbo']['result']['7z'],
                                                    date=rows[0].date)
            except ExceptBBOTaskFailError as e:
                ok = False
                logger.error(e)

            ids = [row.id for row in rows]
            
            if not ok:
                n = unregister_task_by_orm(session=session,
                                            ids=ids)
                n1 += 1
                logger.error(f"Task {rows[0].date} fail. Unregistered tasks' groupuuid: {rows[0].groupuuid}, total {len(rows)} tasks.")
            else: 
                n = delete_task_by_orm(session=session,
                                        ids=ids)
                n2 += 1
                logger.info(f"Task {rows[0].date} is ok. Deleted tasks' groupuuid: {rows[0].groupuuid}, total {len(rows)} tasks.")
            logger.info(f'Unregister {n1} fail except_bbo tasks')
            logger.info(f'Delete {n2} finished except_bbo tasks')
        if sas: 
            sas.disconnect()
        if session:
            session.close()
        if engine:
            engine.dispose()

if __name__ == '__main__':
    # proc_bbo()
    proc_except_bbo()
    """test"""
    # ok = check_except_bbo_task_by_date(zip_base=r'k:\master2024',
    #                                    date=DATE(2024, 1, 2))
    