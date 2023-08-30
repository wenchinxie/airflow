# 踩雷
- ExternalPythonOperator
    - Virtualenv 無法在windows與linux環境共用
    - 改用BashOperator解決Scrapy的問題
- Windows Path 在 Linux環境中會被放入/mnt/c且路徑包含的是/的分隔符號
- 調整cfg的部分
    - set killed_task_cleanup_time = 86400，以免太早關閉task導致錯誤
