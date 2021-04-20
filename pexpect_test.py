#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import pexpect
import sys
import time


log_file = 'pexpect_log.txt'
f = open(log_file, 'w')
ssh_login = "ssh -p 22 togie:12345@172.16.110.220"
child = pexpect.spawn(ssh_login)
# child.logfile_read = f
child.expect(["(?i)assword:\s*", "(?i)are you sure you want to continue connecting"],
        timeout=12)
child.send('12345\r')
child.expect(["--More--", "[^<\n\r]*[>$#]\s*$", pexpect.EOF, pexpect.TIMEOUT])
print(child.before + child.after)
print('====', 'loin completed..')

child.sendcontrol('c')
child.sendline('ps -ef|grep "smbd"|grep -v grep')
child.send('ps -ef|grep "smbd"|grep -v grep\r')
child.expect(["--More--", "[^<\n\r]*[>$#]\s*$", pexpect.EOF, pexpect.TIMEOUT])
print(child.before + child.after)


#while True:
#    print('cmd wait sleep ...')
#    time.sleep(10)
#    break
#with open(log_file, 'r') as f:
#    for line in f:
#        print(line)

    

