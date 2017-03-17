from datetime import datetime
from datetime import timedelta

from apscheduler.schedulers.blocking import BlockingScheduler

from sync.sync_script import SyncDatabase
one_day = timedelta(days=1)

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':
    sync = SyncDatabase()
    def main():
        print('任务重启', now())
        sync.process_flow(sync.tag_time)
        # print((datetime.now() - one_day).strftime("%Y-%m-%d %H:%M:%S"))
        sync.tag_time = (datetime.now() - one_day).strftime("%Y-%m-%d %H:%M:%S")
    scheduler = BlockingScheduler()
    scheduler.add_job(main, trigger='cron', minute='*/2', hour='7-23', day='*')
    try:
        print("开始计划任务", now())
        scheduler.start()
    except:
        print('计划任务终止', now())

# if __name__ == '__main__':
#     sync = SyncDatabase()
#     sync.process_flow(sync.tag_time)
