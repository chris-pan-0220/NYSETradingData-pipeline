# NYSETradeData-pipeline 

A customize NYSE trade data process pipeline, inclduing following features: 

* provide a robust pipeline to download trade data from NYSE sftp server
    * many setting is configurable, ex: date range, types of trade data
* provide a robust pipeline process downloaded trade data 
    * check integrity of downloaded trade data first
    * submit sas code to sas session
    * check integrity of result dataset in the end

## Install

Using poetry, a python package management tool

## Config

```yaml
sftp:
    user: your_user_name
    host: sftp.nyse.com
    port: 22
    key_file: path_to_private_key
    max_retry: 3 # self-defined variable. maximum retry times connect to sftp server
download:
    create_dir: True  # True: create dir recursively | False: do not create
    safe_to:
        EQY_US_ALL_ADMIN: path_to\EQY_US_ALL_ADMIN
        EQY_US_ALL_BBO_ADMIN: path_to\EQY_US_ALL_BBO_ADMIN
        EQY_US_ALL_NBBO: path_to\EQY_US_ALL_NBBO
        EQY_US_ALL_REF_MASTER: path_to\EQY_US_ALL_REF_MASTER
        EQY_US_ALL_REF_MASTER_PD: path_to\EQY_US_ALL_REF_MASTER_PD
        EQY_US_ALL_TRADE: path_to\EQY_US_ALL_TRADE
        EQY_US_ALL_TRADE_ADMIN: path_to\EQY_US_ALL_TRADE_ADMIN
        SPLITS_US_ALL_BBO: path_to\SPLITS_US_ALL_BBO
    only: # only download following types of data
        - SPLITS_US_ALL_BBO
        - EQY_US_ALL_ADMIN 
        - EQY_US_ALL_BBO_ADMIN
        - EQY_US_ALL_NBBO
        - EQY_US_ALL_REF_MASTER
        - EQY_US_ALL_TRADE
        - EQY_US_ALL_TRADE_ADMIN
  # date range 
    start_year: 2024
    start_month: 7
    end_year: 2024
    end_month: 7
sas: 
    # date range 
    start_year: 2024
    start_month: 5
    start_day: 20
    end_year: 2024
    end_month: 5
    end_day: 20
    task:  
        bbo:
            program: path_to\bbo_test.sas
            result:
                7z: path_to_result_dataset_dir
        except_bbo:
            program: path_to\except_bbo_test.sas
            result:
                7z: path_to_result_dataset_dir
```

## Usage

To download trade data from sftp server: 

```python
python proc_download.py
```

To process bbo trade data: 

```python
python bbo.py
```

To process except bbo trade data: 

```python
python except_bbo.py
```

## Test

To-do

## future work 

* unit test
* system health check
* To-do in code
* log viewer 
* process manager
* config GUI editor