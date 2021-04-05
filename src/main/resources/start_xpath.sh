#!/bin/ bash
source /etc/profile
delete_day = `data -d -2day'+%Y%m%d'`

echo delete_day:$delete_day

/usr/local/bin/python /home/zhaowenqing20/hainiu_cralwer/download_page/xpath_config.py

rm -rf /home/zhaowenqing20/xpath_cache_file/xpath_file${delete_day}*

hadoop fs -rmr hdfs://ns1/user/zhaowenqing20/xpah_cache_file/xpath_file${delete_day}*