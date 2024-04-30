from nyse_data_pipeline.download import proc_download
from nyse_data_pipeline.analysis import proc_bbo, proc_except_bbo
from nyse_data_pipeline.logger import Logger
import multiprocessing
import time

logger = Logger().get_logger()

def d_proc_download():
    while True:
        st = time.time()
        proc_download()
        end = time.time()
        duration = end - st 
        if duration < 86400:
            logger.info('Execute proc_download complete. Sleep for a period')
            time.sleep(86400 - duration)

def d_proc_bbo():
    while True:
        st = time.time()
        proc_bbo()
        end = time.time()
        duration = end - st 
        if duration < 86400:
            logger.info(f'Execute proc_bbo complete. Sleep for {86400 - duration}')
            time.sleep(86400 - duration)

def d_proc_except_bbo():
    while True:
        st = time.time()
        proc_except_bbo()
        end = time.time()
        duration = end - st 
        if duration < 86400:
            logger.info(f'Execute proc_download complete. Sleep for {86400 - duration}')
            time.sleep(86400 - duration)

if __name__ == '__main__':
    # procs = []
    # try:    
    #     p = multiprocessing.Process(target=proc_download)
    #     procs.append(p)

    #     p = multiprocessing.Process(target=proc_bbo)
    #     procs.append(p)

    #     p = multiprocessing.Process(target=proc_except_bbo)
    #     procs.append(p)

    #     for p in procs:
    #         p.start()

    # except:
    #     for p in procs:
    #         p.close()
    proc_download()
    proc_bbo()
    proc_except_bbo()