# coding: utf-8

# system packages
import os
import logging
import json

# third-party packages
import argparse
from fabric.api import run, env, sudo, hosts, settings, put
from fabric.tasks import execute

# own packages
from .redis_single import connect

"""
This server manage CronJob,
you can add cronjob, delete cronjob, stop cronjob
"""

env.use_ssh_config = True
local_sudo_user = "gaoxun"
remote_sudo_user = "zhihu"
remote_cron_job_path = "/data/apps/cronjobs"
remote_log_path = "/data/log/supervisor"
remote_supervisor_dir_path = "/data/etc/supervisor/conf.d"


class FabricException(Exception):
    pass


env.abort_exception = FabricException


class PathNotFoundException(Exception):
    pass


class HostNotExistException(Exception):
    pass


class CronJobManagement(object):

    def __init__(self, args, **redis_config):
        self._args = args
        self._redis_connection = connect(**redis_config).get_session()

    def deploy_local_files(self, local_path, remote_path):
        tmp_name = "temp-file"
        with settings(sudo_user=local_sudo_user):
            remote_tmp_path = "~/{}".format(tmp_name)
            put(local_path, remote_tmp_path)
            run("sudo mv {0} {1}".format(remote_tmp_path, remote_path))
            run("sudo chown {0}:{0} {1}".format(remote_sudo_user, remote_path))

    def _add(self):
        job_name = self._args.job_name
        job_path = self._args.job_path
        job_host = self._args.job_host
        if job_path:
            raise PathNotFoundException("{} does not exist".format(job_path))
        if job_host:
            raise HostNotExistException()
        # record these infos into redis
        args = dict()
        args["host"] = json.dumps(job_host)
        self._redis_connection.hmset(job_name, args)
        remote_execute_file_path = os.path.join(remote_cron_job_path, job_name)
        execute(self.deploy_local_files, job_path, remote_execute_file_path, hosts=job_host)
        supervisor_file_path = self._create_supervisor_file()
        remote_supervisor_path = os.path.join(remote_supervisor_dir_path, job_name)
        execute(self.deploy_local_files, supervisor_file_path, remote_supervisor_path)
        os.remove(supervisor_file_path)
        self._update()

    def _get_host(self):
        job_name = self._args.job_name
        h = self._redis_connection.hget(job_name, "hosts")
        if hosts:
            try:
                return json.loads(h)
            except:
                return []

    @staticmethod
    def _create_supervisor_cmd(operator, job_name):
        with settings(sudo_user=remote_sudo_user):
            try:
                sudo("supervisorctl {} {}".format(operator, job_name))
            except FabricException as e:
                logging.exception(e)

    def _inner_execute(self, operator):
        job_name = self._args.job_name
        h = self._get_host()
        if h:
            execute(CronJobManagement._create_supervisor_cmd(operator, job_name), hosts=h)

    def _stop(self):
        self._inner_execute("stop")

    def _start(self):
        self._inner_execute("start")

    def _restart(self):
        self._inner_execute("restart")

    def _update(self):
        self._inner_execute("update")

    def _delete(self):
        job_name = self._args.job_name
        remote_supervisor_path = os.path.join(remote_supervisor_dir_path, job_name)
        with settings(sudo_user=remote_sudo_user):
            sudo("rm {}".format(remote_supervisor_path))
        self._update()

    def _list(self):
        pass

    def _create_supervisor_file(self):
        job_name = self._args.job_name
        current_dir_path = os.path.dirname(__file__)
        file_path = os.path.join(current_dir_path, job_name)
        with open(file_path, "w") as supervisor:
            title = "[program:{}]".format(job_name)
            supervisor.write(title + '\n')
            cmd = "command = python {}".format(remote_cron_job_path, job_name)
            supervisor.write(cmd + '\n')
            supervisor.write("directory = " + remote_cron_job_path + '\n')
            supervisor.write("autostart = true\nautorestart = true\nloglevel = info\n")
            supervisor.write("stdout_logfile = {}/{}-stdout.log\n".format(remote_log_path, job_name))
            supervisor.write("stderr_logfile = {}/{}-stderr.log\n".format(remote_log_path, job_name))
            supervisor.write("stdout_logfile_maxbytes = 500MB\n")
            supervisor.write("stdout_logfile_backups = 50\n")
            supervisor.write("stdout_capture_maxbytes = 1MB\n")
            supervisor.write("stdout_events_enabled = false\b")
        return file_path


def main(**redis_config):
    parser = argparse.ArgumentParser(description="cron job management")
    parser.add_argument("-N", "--name", dest="job_name", help="cronjob name", required=True)
    parser.add_argument("-P", "--path", dest="job_path", help="cron job file path")
    parser.add_argument("-H", "--host", dest="job_host", help="cron job works host")
    parser.add_argument("-O", "--operation", dest="operation",
                        help="add, stop, start, restart, update, delete operations", required=True)
    args = parser.parse_args()
    cj = CronJobManagement(args, **redis_config)
    getattr(cj, "_"+args.operation)()

if __name__ == "__main__":
    redis_config = {
        "host": "127.0.0.1",
        "port": 6379,
        "db": 0,
        "max_connection": 50
    }
    main(**redis_config)
