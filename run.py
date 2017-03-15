from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from sync.sync_script import SyncDatabase


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':
    sync = SyncDatabase()
    def main():
        print('任务重启', now())
        sync.process_flow(sync.tag_time)
        sync.tag_time = now()
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', minutes=2)
    try:
        print("开始计划任务", now())
        scheduler.start()
    except:
        print('计划任务终止', now())