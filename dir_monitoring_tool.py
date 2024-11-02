import paramiko, datetime, time
import threading
from colorama import Fore, Back, Style
import sys

class BackupMonitorTool:
    def __init__(self, server, dir, time_interval, print_color):
        self.server = server
        self.dir = dir
        self.time_interval = time_interval
        self.backup_list = list()
        self.print_color = print_color

        # Creating ssh_client for servers
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(self.server, username="root", password="couchbase")

    def sleep(self, time_interval, msg):

        print(self.print_color + "Sleeping 30 seconds.", msg)
        time.sleep(time_interval)

    def check_dir_exists(self):

        cmd = "ls {}".format("/data")
        output = self.ssh_execute_command(cmd, print_output=False)
        if "backup_dir" not in output:
            print(self.print_color + f"Creating directory /data/backup_dir on {self.server}")
            self.ssh_execute_command("mkdir /data/backup_dir")
        else:
            print(self.print_color + f"Directory backup_dir present on {self.server}. Removing all contents inside it")
            self.ssh_execute_command("rm -rf {}/*".format(self.dir))

    def ssh_execute_command(self, command, print_output=False):

        try:
            ssh_stdin, ssh_stdout, ssh_stderr = self.ssh_client.exec_command(command)
            output = ssh_stdout.readlines()
            if print_output:
                print(self.print_color + "Output =", output, "\n")
            return output

        except Exception as e:
            print(e)

    def validate_backup_timestamps(self):
        if len(self.backup_list) >= 2:
            time_diff = self.backup_list[-1] - self.backup_list[-2]
            assert time_diff.seconds == self.time_interval, "Backups taken at incorrect intervals"

    def take_continuous_backups(self):

        self.check_dir_exists()

        i = 0
        while i < 5:
            file_name = "backup" + str(i) + ".txt"
            self.ssh_execute_command("touch {}/{}".format(self.dir, file_name))
            time_of_creation = datetime.datetime.now()
            print(self.print_color + f"Backup {file_name} on {self.server} created at timestamp: {time_of_creation}")
            self.backup_list.append([file_name, time_of_creation])
            print(self.print_color + f"Backup timestamps on {self.server}: {self.backup_list}")
            self.sleep(self.time_interval, f"Waiting before taking next backup on {self.server}")
            i += 1

    def close(self):
        self.ssh_client.close()

if __name__ == "__main__":

    servers = ["172.23.218.250", "172.23.218.251", "172.23.218.252"]
    backup_dir = "/data/backup_dir"
    time_interval = 15

    backup_threads = list()
    validate_threads = list()
    serverBackupObjs = dict()
    print_colors = [Fore.RED, Fore.BLUE, Fore.GREEN]

    server_idx = 0
    for server in servers:
        backupObj = BackupMonitorTool(server, backup_dir, time_interval, print_colors[server_idx])
        serverBackupObjs[server] = backupObj
        backup_th = threading.Thread(target=backupObj.take_continuous_backups)
        backup_th.start()
        backup_threads.append(backup_th)
        validate_th = threading.Thread(target=backupObj.validate_backup_timestamps)
        validate_th.start()
        validate_threads.append(validate_th)
        server_idx += 1

    for th in backup_threads:
        th.join()

    for th in validate_threads:
        th.join()

    # Closing all shh_clients
    for server in servers:
        serverBackupObjs[server].close()
