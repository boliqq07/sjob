# sjob.py
import os
import shutil
import argparse
import time
import json
import subprocess
from pathlib import Path
import logging
from datetime import datetime, timedelta
import signal
import fcntl
from functools import wraps
import psutil
from daemon import DaemonContext


SJOB_DIR = Path.home() / ".sjob"
QUEUE_FILE = SJOB_DIR / "sjob_queue.json"
LOG_DIR = SJOB_DIR / "logs"
LOCK_FILE = SJOB_DIR / "sjob.lock"
main_log_file = SJOB_DIR / "sjob.log"

# --- 日志配置调整开始 ---
import logging

# 确保日志目录存在
SJOB_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger("sjob")
logger.setLevel(logging.INFO)

# 添加一个FileHandler
file_handler = logging.FileHandler(main_log_file, mode='a')  # 日志仅在debug（DaemonContext外） 有效
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
# --- 日志配置调整结束 ---


STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_END = "end"
STATUS_EXIT = "exit"

SJOB_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_RETENTION_DAYS = 60




def perror(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[ERROR] {func.__name__} failed: {e}")
    return wrapper

class SJob:
    def __init__(self):
        self.load_queue()
        self.cleanup_old_logs()

    def load_queue(self):
        SJOB_DIR.mkdir(exist_ok=True)
        if QUEUE_FILE.exists():
            with open(QUEUE_FILE, 'r') as f:
                self.queue = json.load(f)
        else:
            self.queue = []

    def save_queue(self):
        SJOB_DIR.mkdir(exist_ok=True)
     
        with open(QUEUE_FILE, 'w') as f:
            json.dump(self.queue, f, indent=2)
            
    @perror
    def cleanup_old_logs(self, days=LOG_RETENTION_DAYS):
        now = datetime.now()
        for log_file in LOG_DIR.glob("*.log"):
            try:
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if now - mtime > timedelta(days=days):
                    print(f"[INFO] Deleting old log file: {log_file.name}")
                    log_file.unlink()
            except Exception as e:
                print(f"[WARN] Failed to check or delete {log_file.name}: {e}")
        
    
    @perror            
    def clean_old_queue(self, days=LOG_RETENTION_DAYS):
        now = time.time()
        queue2 = []
        for job in self.queue:
            if now - job['submit_time'] > days * 24 * 3600 and job['status'] == STATUS_RUNNING:
                print(f"[WARN] Job {job['id']} is running, it should be kill manually first.")
                queue2.append(job)
            elif now - job['submit_time'] > days * 24 * 3600 and job['status'] in [STATUS_END, STATUS_EXIT, STATUS_PENDING]:
                print(f"[INFO] Job {job['id']} is removed from the queue.")
            else:
                queue2.append(job)
        self.queue = queue2  # 修正：更新队列
        self.save_queue()   

    @perror
    def submit_job(self, filepath, dir=None):
        if not dir:
            dir = Path.cwd()
        filepath = filepath.strip()
        # 检查重复提交
        for job in self.queue:
            if job['cmd'] == filepath and job['dir'] == str(dir) and job['status'] in [STATUS_PENDING, STATUS_RUNNING]:
                print(f"[WARN] Duplicate job detected, not submitting again, please clean it form squeue.")
                return
        if Path(filepath).exists():
            # cmd = f"bash {filepath}"  # 变量未用到，可省略
            print(f"[INFO] Submitting job form file: bash {filepath}")
        else:
            print(f"[INFO] Submitting job from cmd: {filepath}")
        job_id = str(int(time.time()))
        job = {
            "id": job_id,
            "submit_time": time.time(),
            "dir": str(dir),
            "cmd": str(filepath),
            "status": STATUS_PENDING,
            "pid": None,
            "logfile": str(LOG_DIR / f"{job_id}.log")
        }
        self.queue.append(job)
        self.save_queue()
        time.sleep(1.2)  # 确保每个提交之间有间隔
        print(f"[INFO] Job {job_id} submitted.")

    @perror
    def submit_jobs(self, file_path, dirs=None):
        # 修正参数名和逻辑
        if isinstance(dirs, (list,tuple)) and len(dirs) == 1:
            dirs = dirs[0]

        if dirs is None:
            dirs = [Path.cwd()]
        elif isinstance(dirs, list):
            dirs = [Path(d) for d in dirs]
        else:
            dirs = str(dirs).strip()
            if ',' in dirs:
                dirs = [Path(d.strip()) for d in dirs.split(',')]
            elif Path(dirs).absolute().is_dir():
                dirs = [Path(dirs)]
            elif Path(dirs).absolute().is_file():
                with open(dirs, 'r') as f:
                    dirs = [Path(line.strip()) for line in f if line.strip()]
            else:
                print(f"[ERROR] Invalid directory input: {dirs}")
                return


        for di in dirs:
            di = Path(di).absolute()
            # print(f"[INFO] Processing directory: {di}",Path(str(di)).exists(), Path(str(di)).is_dir())
            if Path(str(di)).exists() and Path(str(di)).is_dir():
                print(f"[INFO] Submitting jobs in directory: {di}")
                self.submit_job(file_path, dir=di)
                
            else:
                print(f"[ERROR] Directory {di} does not exist or is not a directory.")
    @perror
    def recover_jobs(self,job_range):
        """
        Recover jobs from the exit, end status to pending status.
        """
        ids_to_recover = set()
        if '-' in job_range:
            start_id, end_id = job_range.split('-')
            ids_to_recover = {str(i) for i in range(int(start_id), int(end_id) + 1)}
        else:
            ids_to_recover.add(job_range)

        found_any = False
        for i,job in enumerate(self.queue):
            if job['id'] in ids_to_recover and job['status'] in [STATUS_EXIT, STATUS_END]:
                found_any = True
                job['status'] = STATUS_PENDING
                job['submit_time'] = time.time()
                job['pid'] = None
                print(f"[INFO] Job {job['id']} recovered to pending status.")
                self.queue[i] = job  # Update the job in the queue
        if not found_any:
            print(f"[WARN] No job(s) found for ID(s): {job_range}")
        self.save_queue()
    
    @perror
    def clean_by_id(self, job_ids):
        """
        Clean jobs by ID or ID range.
        """
        ids_to_delete = set()
        if '-' in job_ids:
            start_id, end_id = job_ids.split('-')
            ids_to_delete = {str(i) for i in range(int(start_id), int(end_id) + 1)}
        else:
            ids_to_delete.add(job_ids)

        found_any = False
        new_queue = []
        for job in self.queue:
            if job['id'] in ids_to_delete:
                found_any = True
                print(f"[INFO] Job {job['id']} removed from the queue.")
            else:
                new_queue.append(job)
        if not found_any:
            print(f"[WARN] No job(s) found for ID(s): {job_ids}")
        self.queue = new_queue
        self.save_queue()
        
    @perror
    def change_job_rank(self,job_range):
        """
        Change the jobs in job_range to first in queue.
        """
        ids_to_move = set()
        if '-' in job_range:
            start_id, end_id = job_range.split('-')
            ids_to_move = {str(i) for i in range(int(start_id), int(end_id) + 1)}
        else:
            ids_to_move.add(job_range)

        found_any = False
        new_queue = []
        for job in self.queue:
            if job['id'] in ids_to_move:
                found_any = True
                new_queue.insert(0, job)
                print(f"[INFO] Job {job['id']} moved to the front of the queue.")
            else:
                new_queue.append(job)
        if not found_any:
            print(f"[WARN] No job(s) found for ID(s): {job_range}")
        self.queue = new_queue
        self.save_queue()
               
    @perror
    def kill_job(self, job_range):

        if job_range=="all":
            print("[INFO] Killing all jobs in the queue.")
            ids_to_delete = {job['id'] for job in self.queue}
        else:
            ids_to_delete = set()
            if '-' in job_range:
                start_id, end_id = job_range.split('-')
                ids_to_delete = {str(i) for i in range(int(start_id), int(end_id) + 1)}
            else:
                ids_to_delete.add(job_range)

        found_any = False
        for job in self.queue:
            if job['id'] in ids_to_delete:
                found_any = True
                if job['status'] == STATUS_PENDING:
                    job['status'] = STATUS_EXIT
                    print(f"[INFO] Job {job['id']} (pending) marked as exited.")
                elif job['status'] == STATUS_RUNNING:
                    if job.get('pid'):
                        try:
                            os.killpg(os.getpgid(job['pid']), signal.SIGTERM)
                            job['status'] = STATUS_EXIT
                            print(f"[INFO] Job {job['id']} (running) killed and marked as exited.")
                        except Exception as e:
                            print(f"[ERROR] Failed to kill job {job['id']}: {e}")
                    else:
                        print(f"[WARN] No PID found for job {job['id']}. Cannot kill.")
                else:
                    print(f"[WARN] Job {job['id']} is already finished.")
        if not found_any:
            print(f"[WARN] No job(s) found for ID(s): {job_range}")
        self.save_queue()

    @perror
    def show_stats(self, filter_status=None,):
        now = time.time()
        print(f"{'Rank':<6}{'ID':<15}{'STATUS':<10}{'DIR':<40}{'SUBMIT TIME':<20}{'WAIT TIME(h)':<13}{'CMD':<30}")
        
        pending_rank_num = 0
        
        for job in self.queue:
            if filter_status and job['status'] != filter_status:
                continue
            submit_time = datetime.fromtimestamp(job['submit_time']).strftime('%Y-%m-%d %H:%M:%S')
            if job['status'] == STATUS_PENDING:
                wait = int(now - job['submit_time'])
                wait = str(round(wait / 3600, 3))
                pending_rank_num += 1
                pending_rank = str(pending_rank_num)
            else:
                wait = "-"
                pending_rank = "-"
                
            print(f"{str(pending_rank):<6}{job['id']:<15}{job['status']:<10}{job['dir']:<40}{submit_time:<20}{wait:<13}{job['cmd']:<30}")

    @perror       
    def refresh_stats(self):

        self.load_queue()

        for i, job in enumerate(self.queue):
            if job['status'] in [STATUS_RUNNING,STATUS_PENDING] and job.get('pid'):
                try:
                    # Check if process exists (does not kill)
                    os.kill(job['pid'], 0)
                    # If no exception, process is still running
                    job['status'] = STATUS_RUNNING
                except ProcessLookupError:
                    # Process has finished
                    job['status'] = STATUS_END
                    job['pid'] = None
                except PermissionError:
                    # Process exists but we have no permission, assume running
                    pass
                self.queue[i] = job  # Update the job in the queue
        self.save_queue()

    @perror
    def show_log(self, job_id,header=False):
        
        if "-" in job_id:
            print(f"For show log, job_id should be a single job ID, not a range.")
            return 

        for log_file in LOG_DIR.glob("*.log"):
            if log_file.stem == job_id:
                if log_file and Path(log_file).exists():
                    with open(log_file, 'r') as f:
                        if header:
                            # Just print the header
                            lines = f.readlines()
                            if lines:
                                print("".join(lines[:5]))
                        else:
                            print(f.read())
                else:
                    print(f"[WARN] Log file not found for job {job_id}.")
                return
        print(f"[WARN] Job {job_id} not found.")


    def run_jobs(self):
        print("[INFO] Single-Job Manager started.")
        with DaemonContext():
        # if True: # debug
            with open(LOCK_FILE, 'w') as lockf:
                try:
                    fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    print("[ERROR] Another sjob instance is already running.")
                    return

                def run_job(job):
                    
                    try:
                        with open(job['logfile'], 'w') as lf:
                            lf.write(f"Job: {job['id']}")
                            lf.write(f"Command: {job['cmd']}\n")
                            lf.write(f"Directory: {job['dir']}\n")
                            lf.write(f"Submit time: {datetime.fromtimestamp(job['submit_time'])}\n")


                            proc = subprocess.Popen(job['cmd'], cwd=job['dir'], shell=True,
                                                    preexec_fn=os.setsid, stdout=lf, stderr=lf)
                            job['pid'] = proc.pid
                            
                            num = [i["id"] for i in self.queue].index(job['id'])  # Update job ID in the queue
                            self.queue[num] = job  # Update the job in the queue
                            self.save_queue()
                            
                            lf.write(f"PID: {job.get('pid', 'N/A')}\n")
                            lf.write("Starting job...\n")
                            # 使用 preexec_fn=os.setsid 以确保子进程可以独立于父进程运行
                            # 这对于处理信号和终止子进程非常重要
                            lf.flush()
                            
                            proc.wait()
                            lf.write(f"\nJob {job['id']} end/exit at {datetime.now()}\n")
                            lf.flush()
                        shutil.copyfile(job['logfile'], Path(job['dir']) / f"{job['id']}.log")
                        job['status'] = STATUS_END if proc.returncode == 0 else STATUS_EXIT
                    except Exception as e:
                        try:
                            with open(job['logfile'], 'a+') as lf:
                                lf.write(f"\nJob {job['id']} failed with error: {e}\n")
                                lf.flush()
                            shutil.copyfile(job['logfile'], Path(job['dir']) / f"{job['id']}.log")
                        except Exception as e2:
                            pass
                        job['status'] = STATUS_EXIT
                        
                        if job['pid'] is not None:
                            try:
                                os.killpg(os.getpgid(job['pid']), signal.SIGTERM)
                            except ProcessLookupError:
                                pass
                    
                    return job

                # --- 修改开始 ---
                while True:
                    # logger.info("Loading job queue...")
                    self.load_queue()  
                    for i, job in enumerate(self.queue):
                        
                        if job['status'] == STATUS_PENDING:
                            logger.info(f"Processing job {job['id']}")
                            logger.info(f"Processing job path {job['dir']}")
                            logger.info(f"Processing job initial status {job['status']}")
                            job['status'] = STATUS_RUNNING
                            self.queue[i] = job  # Update the job in the queue
                            self.save_queue()
                            logger.info(f"Processing job middle status {job['status']}")
                            job = run_job(job)
                            self.queue[i] = job  # Update the job in the queue
                            self.save_queue()
                            logger.info(f"Processing job end status {job['status']}")
                            time.sleep(2)
                    time.sleep(5)  # 防止空转
                # --- 修改结束 ---
                
def stop_manager():
    print("[INFO] Stopping job manager， The last [Running] jobs would keep running...")
    if LOCK_FILE.exists():
        try:
            with open(LOCK_FILE, 'r') as f:
                # Try to find the process holding the lock
                for proc in psutil.process_iter(['pid', 'open_files']):
                    try:
                        files = proc.info['open_files']
                        if files:
                            for file in files:
                                if file.path == str(LOCK_FILE):
                                    print(f"[INFO] Stopping job manager process PID {proc.pid}")
                                    proc.terminate()
                                    proc.wait(timeout=5)
                                    print("[INFO] Job manager stopped.")
                                    return
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            print("[WARN] No running job manager process found holding the lock.")
        except Exception as e:
            print(f"[ERROR] Failed to stop job manager: {e}")
    else:
        print("[WARN] No lock file found. Job manager may not be running.")


def main():
    parser = argparse.ArgumentParser(description='Simple Job Manager')
    subparsers = parser.add_subparsers(dest='command')

    # Stats subcommand
    stats_parser = subparsers.add_parser('stats', help='Show job stats')

    stats_parser.add_argument('-p',"--pending", action='store_true', help='Show pending jobs')
    stats_parser.add_argument('-r', "--running", action='store_true', help='Show running jobs')
    stats_parser.add_argument('-e', "--end", action='store_true', help='Show ended jobs')
    stats_parser.add_argument('-x', "--exit", action='store_true', help='Show exited jobs')
    stats_parser.add_argument('-a', "--all", action='store_true', help='Show all jobs')
    
    clean_parser = subparsers.add_parser('clean', help='Clean up old logs')
    clean_parser.add_argument("-d",'--days', type=int, default=LOG_RETENTION_DAYS, help='Number of days to keep logs (default: 100)')
    
    del_parser = subparsers.add_parser('del', help='Delete jobs by ID or ID range')
    del_parser.add_argument('job_range', type=str, help='Job ID to clean ')
    
    kill_parser = subparsers.add_parser('kill', help='Kill a job by ID or ID range')
    kill_parser.add_argument('job_range', type=str, help='Job ID or range (e.g., 1000-1003)')
    
    sub_parser = subparsers.add_parser('sub', help='Submit a job from a file or command')
    sub_parser.add_argument('cmd', type=str, help='Command or script file to submit as a job')
    sub_parser.add_argument('-d', '--dir', type=str,nargs="*", default=None, help='Directory/s or file contains dirs, to run the job in (default: current directory)')
    
    
    log_parser = subparsers.add_parser('log', help='Show history job log')
    log_parser.add_argument('job_range', type=str, help='Job ID to show log for')
    log_parser.add_argument('-hd','--header', action='store_true', help='Just print the header to the log file (optional)')
    
    
    recover_parser = subparsers.add_parser('rec', help='Recover jobs from exit, status to pending status')
    recover_parser.add_argument('job_range', type=str, help='Job ID or range (e.g., 1000-1003) to recover')
    
    tune_parser = subparsers.add_parser('tune', help='Change the jobs in job_range to first in queue')
    tune_parser.add_argument('job_range', type=str, help='Job ID or range (e.g., 1000-1003) to move to the front of the queue')
    
    subparsers.add_parser('stop', help='Stop the running job manager')
    
    run_parser = subparsers.add_parser('run', help='Run all pending jobs in the queue')
    
    pid_parser = subparsers.add_parser('pid', help='Show the process ID of the job manager')

    # Handle stop command in main

    try:
        args = parser.parse_args()  # 修正：应在参数定义后立即调用

        if args.command == 'stop':
            stop_manager()
            return

        sjob = SJob()

        if args.command == 'run':
            sjob.run_jobs()
        if args.command == 'pid':
            print("please run commd: 'ps aux | grep sjob'")
        elif args.command == 'sub':
            sjob.submit_jobs(args.cmd, dirs=args.dir)
        elif args.command == 'rec':
            sjob.recover_jobs(args.job_range)
        elif args.command == 'tune':
            sjob.change_job_rank(args.job_range)
        elif args.command == 'kill':
            sjob.kill_job(args.job_range)
        elif args.command == 'log':
            sjob.show_log(args.job_range, header=args.header)
        elif args.command == 'stats':
            sjob.refresh_stats()
            if getattr(args, 'all', False):
                sjob.show_stats()
            elif getattr(args, 'pending', False):
                sjob.show_stats(filter_status=STATUS_PENDING)
            elif getattr(args, 'running', False):
                sjob.show_stats(filter_status=STATUS_RUNNING)
            elif getattr(args, 'end', False):
                sjob.show_stats(filter_status=STATUS_END)
            elif getattr(args, 'exit', False):
                sjob.show_stats(filter_status=STATUS_EXIT)
            else:
                sjob.show_stats()
        elif args.command == 'clean':

            sjob.cleanup_old_logs(days=args.days)
            sjob.clean_old_queue(days=args.days)
        elif args.command == 'del':
            sjob.clean_by_id(args.job_range)

        else:
            parser.print_help()
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")

if __name__ == '__main__':
    main()
