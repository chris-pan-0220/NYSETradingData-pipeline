from urllib.parse import quote_plus
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from nyse_data_pipeline.config import Config

# 創建一個 Base 類來定義 ORM 模型
Base = declarative_base()

# 定義一個 TaskCategory ORM 模型
class TaskCategory(Base):
    __tablename__ = 'taskCategory'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

    # tasks = relationship('Task')

class Task(Base):
    __tablename__ = 'task'  # 將 'your_table_name' 替換為實際的表格名稱

    id = Column(Integer, primary_key=True, autoincrement=True)
    taskCategoryId = Column(Integer, ForeignKey('taskCategory.id'), nullable=False)  # 外鍵設置
    filename = Column(String(255), nullable=False)
    alphabet = Column(String(1))
    date = Column(Date, nullable=False)
    groupuuid = Column(String(36), nullable=True)  # NULL 許可

# 創建表格
# Base.metadata.create_all(engine)
    
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

    Base.metadata.create_all(engine)  # 创建所有定义的表格

    categories = [
        TaskCategory(id=1, name='EQY_US_ALL_ADMIN'),
        TaskCategory(id=2, name='EQY_US_ALL_BBO'),
        TaskCategory(id=3, name='EQY_US_ALL_BBO_ADMIN'),
        TaskCategory(id=4, name='EQY_US_ALL_NBBO'),
        TaskCategory(id=5, name='EQY_US_ALL_REF_MASTER'),
        TaskCategory(id=6, name='EQY_US_ALL_REF_MASTER_PD'),
        TaskCategory(id=7, name='EQY_US_ALL_TRADE'),
        TaskCategory(id=8, name='EQY_US_ALL_TRADE_ADMIN'),
        TaskCategory(id=9, name='SPLITS_US_ALL_BBO')
    ]

    # print(r)
    session.add_all(categories)
    session.commit()

    return engine, session

if __name__ == '__main__':
    # 建立到 MySQL 資料庫的連接
    # engine = create_engine(f"mysql+mysqlconnector://root:{quote_plus('aaAA@999888')}@localhost/nyse")
    # # 創建一個 session
    # Session = sessionmaker(bind=engine)
    # session = Session()

    # # 查詢所有名為 'SPLITS_US_ALL_BBO' 的 TaskCategory
    # task_categories = session.query(TaskCategory).filter(TaskCategory.name == 'SPLITS_US_ALL_BBO').all()

    # # 輸出結果
    # for task_category in task_categories:
    #     print(task_category.id)

    # # 關閉 session
    # session.close()

    engine, session = init_test_db()
    session.close()
    engine.dispose()
