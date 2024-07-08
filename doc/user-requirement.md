# user requirements
### 1. 從 NYSE sftp server 下載檔案，並且依據日期歸檔

檔案類型如下：

- `SPLITS_US_ALL_BBO`
    - SPLITS_US_ALL_BBO_20240301.done，檔案大小較小，用於標記 20240301 當天的文件都已經上傳到 sftp server
    - SPLITS_US_ALL_BBO_D_20240306.gz，檔案大小可以達到數 GB
- `EQY_US_ALL_ADMIN`
    - 注意！包含 CTS & UTP 類型的檔案
    - EQY_US_ALL_ADMIN_CTS_20240301.gz
    - EQY_US_ALL_ADMIN_UTP_20240328.gz
- `EQY_US_ALL_BBO`
    - 目前沒有看到檔案
- `EQY_US_ALL_BBO_ADMIN`
    - EQY_US_ALL_BBO_ADMIN_20240301.gz
- `EQY_US_ALL_NBBO`
    - EQY_US_ALL_NBBO_20240307.gz，檔案大小可以達到數 GB
- `EQY_US_ALL_REF_MASTER`
    - EQY_US_ALL_REF_MASTER_20240409.gz
- `EQY_US_ALL_REF_MASTER_PD`
- `EQY_US_ALL_TRADE`
    - EQY_US_ALL_TRADE_20240304.gz
- `EQY_US_ALL_TRADE_ADMIN`
    - EQY_US_ALL_TRADE_ADMIN_20240301.gz

資料夾格式如下 ( 截至 2024/04/26 )：

- SPLITS_US_ALL_BBO 類型的檔案
    - E:\SPLITS_US_ALL_BBO\SPLITS_US_ALL_BBO_2024\SPLITS_US_ALL_BBO_202403
- 非 SPLITS_US_ALL_BBO 類型的檔案，Ex: EQY_US_ALL_TRADE
    - L:\2024\EQY_US_ALL_TRADE_2024\EQY_US_ALL_TRADE_202403

### 2. 根據不同的檔案類型，執行不同的 SAS 腳本處理下載完的資料

對於 SPLITS_US_ALL_BBO 的資料，執行`C:\temp\bbo_test.sas`。對於每一天的 SPLITS_US_ALL_BBO 檔案 ( Ex: SPLITS_US_ALL_BBO_D_20240306.gz )，

* 使用以下指令 `"C:\progra~1\7-Zip\7z e &inpath\&filename -y -so"` 使用 7-zip 解壓縮目標檔案，並將 output 重新導向為 input，
* 進行一些資料處理之後，
* 使用以下指令 `x "c:\progra~1\7-zip\7z a -t7z &topath\cq&&f_alphabet&g.._&&f&g...7z &tmp1\cq&&f_alphabet&g.._&&f&g...sas7bdat &tmp1\cq&&f_alphabet&g.._&&f&g..end.sas7bdat  -slp -sdel";` 將目標檔案添加到 `xxx.sas7bdat` 以及 `xxx.end.sas7bdat` 的壓縮檔。


對於非 SPLITS_US_ALL_BBO 的資料，執行 `C:\temp\except_bbo_test.sas`。對於同一天的檔案，包含`EQY_US_ALL_ADMIN`,`EQY_US_ALL_BBO_ADMIN`,`EQY_US_ALL_NBBO`,
`EQY_US_ALL_REF_MASTER`,`EQY_US_ALL_TRADE`,`EQY_US_ALL_TRADE_ADMIN`

* 使用以下指令 `"C:\progra~1\7-Zip\7z e &inpath\&filename -y -so"` 使用 7-zip 解壓縮目標檔案，並將 output 重新導向為 input，
* 進行一些資料處理之後，
* 使用以下指令 `x "c:\progra~1\7-zip\7z a -t7z &iupathy\xxx&&f&g...7z &iupathy\xxx&&f&g...sas7bdat &iupathy\xxxxx&&f&g...sas7bdat  &iupathy\xxx&&f&g..end.sas7bdat";` 將目標檔案添加到 `xxx.sas7bdat` 以及 `xxx.end.sas7bdat` 的壓縮檔。
* 再根據字母分類，產生`ctq`, `tsp`, `vol`, `bs` 四種檔案
