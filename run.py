from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from sync.sync_script import SyncDatabase

if __name__ == '__main__':
    sync = SyncDatabase()
    def main():
        sync.process_flow(sync.tag_time)
        sync.tag_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', seconds=60)
    try:
        print("开始计划任务")
        scheduler.start()
    except:
        print('计划任务终止')