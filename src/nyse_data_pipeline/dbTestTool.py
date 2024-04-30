from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import MetaData

from nyse_data_pipeline.analysis import connect_to_mysql_by_orm
from nyse_data_pipeline.config import Config
from nyse_data_pipeline.schema import Task, TaskCategory

# 创建一个基类
Base = declarative_base()

# def connect_to_mysql_testdb():
#     CONFIG = Config()
#     config = CONFIG.get_config() 
#     mysql_config = config['mysql']

#     host = mysql_config['host']
#     user = mysql_config['user']
#     password = mysql_config['password']
#     database = mysql_config['database']['test']

#     engine = create_engine(
#             f"mysql+mysqlconnector://{user}:{quote_plus(password)}@{host}/{database}")
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     return engine, session

def init_test_db():
    CONFIG = Config()
    config = CONFIG.get_config() 
    mysql_config = config['mysql']

    host = mysql_config['host']
    user = mysql_config['user']
    password = mysql_config['password']
    database = mysql_config['database']['test']

    engine = create_engine(
            f"mysql+mysqlconnector://{user}:{quote_plus(password)}@{host}/{database}")
    Session = sessionmaker(bind=engine)
    session = Session()

    # metadata = MetaData(engine)
    # metadata.create_all()

    Base.metadata.create_all(engine)  # 创建所有定义的表格
    # print(r)
    session.commit()

    return engine, session

# def insert_test_case(session: Session, tasks: list[Task]):
#     for task in tasks:
#         session.add(task)
#     session.commit()

# def delete_test_case(session: Session):
#     session.query(Task).delete()
#     session.commit()

if __name__ == '__main__':
    engine, session = init_test_db()