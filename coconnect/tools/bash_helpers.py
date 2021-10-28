import subprocess
from subprocess import Popen, PIPE
from .logger import Logger


class BashHelpers:

    def __init__(self,dry_run=False):
        self.__logger = Logger("run_bash_cmd")
        self.dry_run = dry_run

    def run_bash_cmd(self,cmd):
        if isinstance(cmd,str):
            cmd = cmd.split(" ")
        elif not isinstance(cmd,list):
            raise Exception("run_bash_cmd must be passed a bash command as a str or a list")
        self.__logger.notice(" ".join(cmd))
        if self.dry_run:
            return None,None
        session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = (x.decode("utf-8") for x in session.communicate())
        return stdout,stderr
