import saspy
import paramiko
import pandas as pd
import logging
import os
import sys
import py7zr

from nyse_data_pipeline.config import Config
from nyse_data_pipeline.utils import Utils, DownloadInfo
from nyse_data_pipeline.exception import (
    BBOTaskFailError,
    ExceptBBOTaskFailError,
    SasProgramExecuteError,
    CollectDownloadTaskError
)

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
            # pass integrity test
        return True
    except py7zr.Bad7zFile: # zip file is corrupt
        return False 

def check_bbo_task_safe(zip_base: str, file: DownloadInfo):
    path = f"{zip_base}\cq{file.file_name[-13]}_{file.file_name[-11:-3]}.7z"
    if not os.path.exists(path):
        msg = f"Required file {path} does not exist"
        raise BBOTaskFailError(msg)
    if not check_7z_integrity(path):
        msg = f"Required file {path} is not complete. Remove"
        os.remove(path)
        raise BBOTaskFailError(msg)
    return True  

def check_except_bbo_task(zip_base: str, date: str):  # yyyymmdd
    """
    TODO: test running time of checking zip file integrity.
    Take SPLITS_US_ALL_BBO_A_20240401.gz for example, check 1.44 GB task 2 mins.
    Following types of files occupy about 13~15 GB.
    """
    # TODO: add to config
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
            ok = False
            continue
        if '.7z' in path and not check_7z_integrity(path):
            msg = f"Required file {path} is not complete. Remove"
            msg_list.append(msg)
            ok = False
    if not ok:
        for path in path_list: # remove all sas data because task fail
            if os.path.exists(path):
                os.remove(path)
        err_msg = '\n'
        for msg in msg_list:
            err_msg += f'{msg}\n'
        err_msg += '\n'
        raise ExceptBBOTaskFailError(err_msg)

    return ok

def collect_task(start_year: int,
                 start_month: int,
                 start_day: int,
                 end_year: int,
                 end_month: int,
                 end_day: int,
                 sftp: paramiko.SFTPClient,
                 safe_to: dict[str, str],
                 only: list[str]) -> list[DownloadInfo]:
    local_dir_base = safe_to
    dir_list = only
    download_task_list = []

    # TODO: modify validate_date to check `start_day`, `end_day`
    assert Utils.validate_date(str(start_year), str(start_month)), "Invalid start year or month"
    assert Utils.validate_date(str(end_year), str(end_month)), "Invalid end year or month"
    assert (start_year < end_year) or (start_year == end_year and start_month <= end_month), "Invalid date range"

    for dir_name in dir_list:
        for year in range(start_year, end_year + 1):
            year_str = str(year)
            for month in range(1, 13):
                month_str = str(month).zfill(2)  # Zero-padding for single-digit months
                if (year == start_year and month < start_month) or \
                    (year == end_year and month > end_month):
                    continue

                remote_dir = os.path.join(dir_name, f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')
                remote_dir = remote_dir.replace('\\', '/')

                # TODO
                # Now local dir path is "L:\2024\EQY_US_ALL_TRADE_ADMIN_2024",
                # can we change to "L:\EQY_US_ALL_TRADE_ADMIN\EQY_US_ALL_TRADE_ADMIN_2024"
                local_dir = os.path.join(local_dir_base[dir_name], f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')

                if not os.path.exists(local_dir_base[dir_name]): 
                    raise Exception(f"local_dir_base {local_dir_base[dir_name]} does not exist")
                if not os.path.exists(local_dir):
                    raise Exception(f"local_dir {local_dir} does not exist")

                try: 
                    local_files = Utils.list_dir_local_with_sizes(local_dir)
                    remote_files = Utils.list_dir_remote_with_sizes(sftp, remote_dir)
                    
                    download_tasks = []
                    for remote_file in remote_files:
                        if '.gz' not in remote_file.name: 
                            continue
                        day = remote_file.name[-5:-3] # day
                        # print(f"day={day}, start_day_str={str(start_day).zfill(2)}, end_day_str={str(end_day).zfill(2)}")
                        if day < str(start_day).zfill(2):
                            continue
                        if day > str(end_day).zfill(2):
                            continue
                        if remote_file.name not in (local_file.name for local_file in local_files):
                            continue
                        for local_file in local_files:
                            if remote_file.name == local_file.name and remote_file.size == local_file.size:
                                task = DownloadInfo(remote_dir=remote_dir,
                                                    local_dir=local_dir,
                                                    file_name=remote_file.name,
                                                    size=remote_file.size)
                                download_tasks.append(task)
                                break

                    download_task_list.extend(download_tasks)

                except Exception as e:
                    raise CollectDownloadTaskError(f"Error collecting tasks for {remote_dir}") from e

    return download_task_list

def proc_bbo_safe():
    CONFIG = Config()
    config = CONFIG.get_config() 
    sas_config = config['sas']
    sftp_config = config['sftp']
    download_config = config['download']

    # get logger
    logger = logging.getLogger('proc_bbo')
    logger.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # to stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    # to file
    file_handler = logging.FileHandler('bbo.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # add handler
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    engine = None 
    session = None
    sas = None 
    hr_df = None
    task_for_check = []

    start_year = sas_config['start_year']
    start_month = sas_config['start_month']
    start_day = sas_config['start_day']
    end_year = sas_config['end_year']
    end_month = sas_config['end_month']
    end_day = sas_config['end_day']

    # level 3 safety check
    # collect_boo_task 
        # +for each year ... for each month?
            # +check downloaded .gz files. delete file if it's corrupt (?) => 呼叫 collect task
            # for each date
                # +check sas data => call `check_except_bbo_task_by_date`
                # run sas program
    try:
        client, sftp = Utils.connect_to_sftp(user=sftp_config['user'],
                                            host=sftp_config['host'],
                                            port=sftp_config['port'],
                                            key_file=sftp_config['key_file'],
                                            max_retry=sftp_config['max_retry'])
        logger.info(f'Collect bbo tasks. Start from {start_year}/{start_month}/{start_day} to {end_year}/{end_month}/{end_day}')
        task_list = collect_task(start_year=start_year,
                                start_month=start_month,
                                start_day=start_day,
                                end_year=end_year,
                                end_month=end_month,
                                end_day=end_day,
                                sftp=sftp,
                                safe_to=download_config['safe_to'],
                                only=['SPLITS_US_ALL_BBO'])

        if len(task_list) == 0:
            logger.info(f"Collect 0 bbo tasks. Exit")
            return

        logger.info(f"Collect {len(task_list)} tasks.")
        for task in task_list:
            logger.info(f"file: {task.file_name}")
        
        for task in task_list:
            # check sas data 
            logger.info(f"Pre-check bbo task. file: {task.file_name}")
            try:
                check_bbo_task_safe(zip_base=sas_config['task']['bbo']['result']['7z'],
                                    file=task)
                logger.info(f"Task {task.file_name} is finish")
                continue
            except BBOTaskFailError as e:
                logger.info(f"Task {task.file_name} is not finish.")
                logger.info(e)

            # process task        
            hr_df = pd.DataFrame({
                'date':[task.file_name[-11:-3]],
                'filename':[task.file_name],
                'alphabet': [task.file_name[-13]]
            })

            logger.info(hr_df)

            os.path.join(os.path.dirname(__file__), 'bbo.csv')
            hr_df.to_csv(
                os.path.join(os.path.dirname(__file__), 'bbo.csv'), # modify !
                index=False)

            sas = saspy.SASsession()

            # r = sas.file_delete('work.filelist')
            # NOTE: if sas program exit abnormally, temporary lib, e.g. work.filelist,
            # it's difficult to delete.
            # Therefore, I write absolute path of work list, e.g. bbo.csv, into sas program
            
            program_path = sas_config['task']['bbo']['program']
            logger.info(f"Read sas program {program_path}")
            with open(program_path, 'r') as file:
                sas_code = file.read()
            logger.info(f"Read sas program {program_path}")

            # NOTE: Not sure if `options errorabend;` in sas program always works.
            # it will take into effect if work.filelist is opening within sas App.
            task_for_check.append(task)
            try:
                logger.info(f"Execute sas program {program_path}")
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
        ok = 0
        fail = 0
        for task in task_for_check:
            logger.info(f"Check bbo task. file: {task.file_name}")
            try:
                check_bbo_task_safe(zip_base=sas_config['task']['bbo']['result']['7z'],
                                    file=task)
                logger.info(f"Task {task.file_name} is ok")
                ok += 1
            except BBOTaskFailError as e:
                logger.error(f"Task {task.file_name} fail.")
                logger.error(e)
                fail += 1
        logger.info(f"Total {ok} tasks finish, {fail} tasks fail.")
        if sftp:
            sftp.close()
        if client: 
            client.close()
        if sas: 
            sas.disconnect()
        if session:
            session.close()
        if engine:
            engine.dispose()

def proc_except_bbo_safe():
    CONFIG = Config()
    config = CONFIG.get_config() 
    sas_config = config['sas']
    sftp_config = config['sftp']
    download_config = config['download']

    # get logger
    logger = logging.getLogger('proc_except_bbo')
    logger.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # to stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    # to file
    file_handler = logging.FileHandler('except_bbo.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # add handler
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    engine = None 
    session = None
    sas = None 
    hr_df = None
    task_for_check = []

    start_year = sas_config['start_year']
    start_month = sas_config['start_month']
    start_day = sas_config['start_day']
    end_year = sas_config['end_year']
    end_month = sas_config['end_month']
    end_day = sas_config['end_day']

    # level 3 safety check
    # collect_except_boo_task/proc_except_bbo
        # +for each year ... for each month?
            # +check downloaded .gz files. delete file if it's corrupt => 直接呼叫 collect task
            # collect all date
            # for each date
                # +check sas data => call `check_except_bbo_task_by_date``
                # run sas program
    try:
        client, sftp = Utils.connect_to_sftp(user=sftp_config['user'],
                                            host=sftp_config['host'],
                                            port=sftp_config['port'],
                                            key_file=sftp_config['key_file'],
                                            max_retry=sftp_config['max_retry'])
        sas = saspy.SASsession()
        logger.info(f'Collect except_bbo tasks. Start from {start_year}/{start_month}/{start_day} to {end_year}/{end_month}/{end_day}')
        task_type = [
            'EQY_US_ALL_ADMIN_CTS',
            'EQY_US_ALL_ADMIN_UTP',
            'EQY_US_ALL_BBO_ADMIN',
            'EQY_US_ALL_NBBO',
            'EQY_US_ALL_REF_MASTER',
            'EQY_US_ALL_TRADE',
            'EQY_US_ALL_TRADE_ADMIN']

        task_list = collect_task(start_year=start_year,
                                start_month=start_month,
                                start_day=start_day,
                                end_year=end_year,
                                end_month=end_month,
                                end_day=end_day,
                                sftp=sftp,
                                safe_to=download_config['safe_to'],
                                only=['EQY_US_ALL_ADMIN',
                                    'EQY_US_ALL_BBO_ADMIN',
                                    'EQY_US_ALL_NBBO',
                                    'EQY_US_ALL_REF_MASTER',
                                    'EQY_US_ALL_TRADE',
                                    'EQY_US_ALL_TRADE_ADMIN'])
        
        if len(task_list) == 0:
            logger.info(f"Collect 0 except_bbo candidates Exit")
            return

        logger.info(f"Collect {len(task_list)} candidates. Several types of candidates source data in a specific date forms a task.")        
        
        for year in range(start_year, end_year+1):
            for month in range(1, 12+1):
                if (year == start_year and month < start_month) or \
                    (year == end_year and month > end_month):
                    continue
                for day in range(1, 31+1):
                    if (year == start_year and month == start_month and day < start_day) or \
                        (year == end_year and month == end_month and day > end_day):
                        continue 
                    
                    date_str = f"{year}{str(month).zfill(2)}{str(day).zfill(2)}"

                    # pre-check task
                    logger.info(f"Pre-check except_bbo task. date: {date_str}")
                    try:
                        check_except_bbo_task(zip_base=sas_config['task']['except_bbo']['result']['7z'],
                                              date=date_str)
                        logger.info(f"Task {date_str} is finish")
                        continue
                    except ExceptBBOTaskFailError as e:
                        logger.info(f"Task {date_str} is not finish.")
                        logger.info(e)

                    find = True
                    for t in task_type: 
                        find_t = False 
                        for task in task_list:
                            if date_str in task.file_name and \
                                t in task.file_name and \
                                '.gz' in task.file_name:
                                find_t = True 
                                break
                        if not find_t:
                            find = False 
                            break
                    if not find:
                        continue
                    
                    # process task
                    hr_df = pd.DataFrame({'date': [date_str]})
                    logger.info(hr_df)
                    hr_df.to_csv(
                        os.path.join(os.path.dirname(__file__), 'except_bbo.csv'), # modify
                        index=False)

                    program_path = sas_config['task']['except_bbo']['program']
                    with open(program_path, 'r') as file:
                        sas_code = file.read()
                    logger.info(f"Read sas program {program_path}")

                    # TODO: Not sure if set `options errorabend` in sas program, 
                    # the program always suspends from errors.
                    task_for_check.append(date_str)
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
        ok = 0
        fail = 0
        for date_str in task_for_check:
            logger.info(f"Check except_bbo task. date: {date_str}")
            try:
                check_except_bbo_task(zip_base=sas_config['task']['except_bbo']['result']['7z'],
                                      date=date_str)
                logger.info(f"Task {date_str} is ok")
                ok += 1
            except ExceptBBOTaskFailError as e:
                logger.error(f"Task {date_str} fail.")
                logger.error(e)
                fail += 1
        logger.info(f"Total {ok} tasks finish, {fail} tasks fail.")
        if sftp:
            sftp.close()
        if client: 
            client.close()
        if sas: 
            sas.disconnect()
        if session:
            session.close()
        if engine:
            engine.dispose()

if __name__ == '__main__':
    # proc_bbo()
    # proc_except_bbo()
    # CONFIG = Config()
    # config = CONFIG.get_config() 
    # download_config = config['download']
    # sftp_config = config['sftp']
    # client, sftp = Utils.connect_to_sftp(user=sftp_config['user'],
    #                                     host=sftp_config['host'],
    #                                     port=sftp_config['port'],
    #                                     key_file=sftp_config['key_file'],
    #                                     max_retry=sftp_config['max_retry'])
    # tasks = collect_task(
    #     start_year=2024,
    #     start_month=5,
    #     start_day=1,
    #     end_year=2024,
    #     end_month=5,
    #     end_day=3,
    #     sftp=sftp,
    #     safe_to=download_config['safe_to'],
    #     only=['EQY_US_ALL_REF_MASTER']
    # )
    # print(len(tasks))

    # proc_bbo_safe()
    proc_except_bbo_safe()
    # print(os.path.exists(os.path.join(os.path.dirname(__file__), 'except_bbo.csv')))
    # print(os.path.exists(os.path.join(os.path.dirname(__file__), 'bbo.csv')))
    
    """test"""
    # import time 
    # # 2024/1/2  14.8 GB 330s
    # # 2024/1/31 17.3 GB 402s
    # st = time.time()
    # print(st)
    # ok = check_except_bbo_task_by_date(zip_base=r'k:\master2024',
    #                                    date=DATE(2024, 1, 31))
    # end = time.time()
    # print(end)
    # print(f"take: {end - st} s")
    