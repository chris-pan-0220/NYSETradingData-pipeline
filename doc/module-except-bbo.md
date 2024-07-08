# except bbo 資料處理模組

根據設定檔案當中的日期範圍，按日執行以下操作：

- 列出該日的本地文件
- 列出該日的遠端文件
- 對比本地文件與遠端文件，列出已經成功下載的文件
- 如果 except bbo 任務的所有類型檔案都已經成功下載，將任務儲存為`except_bbo.csv`，並執行 `except_bbo_test.sas`
- `except_bbo_test.sas` 會從 `except_bbo.csv` 讀取待處理的任務，進行資料處理 
    - 在程式當中添加 `options errorabend;`，確保 sas program 遇到錯誤的時候會立即中止，並且回報錯誤
- 當成功執行完 sas program 的時候，會確認所有壓縮檔是否完整，確保 sas program 正常執行
- 當程式遇到意外中斷時，會將資料處理的任務刪除，並且清理壓縮檔。