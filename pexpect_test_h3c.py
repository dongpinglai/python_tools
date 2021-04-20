#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import pexpect
import sys
import time

def send_cmd(child, cmd):
    child.sendline(cmd)
    # child.expect(["--More--", "[^<\n\r]*[>$#]\s*$", pexpect.EOF, pexpect.TIMEOUT])
    child.expect(["--More--", "[^<\n\r]*[>$#\]]\s*$", pexpect.EOF, pexpect.TIMEOUT])
    if not isinstance(child.after, str):
        child.after = '\ntimeout..'
    print('\n')
    print('\n')
    print(cmd, ':')
    print(child.before + child.after)

def send_control(child, char):
    child.sendcontrol(char)
    child.expect(["--More--", "[^<\n\r]*[>$#]\s*$", pexpect.EOF, pexpect.TIMEOUT])


log_file = 'pexpect_h3c_log.txt'
f = open(log_file, 'w')
ssh_login = "ssh -p 22 togie@172.16.110.253"
child = pexpect.spawn(ssh_login)
# child.logfile_read = f
index = child.expect(["(?i)assword:\s*", "(?i)are you sure you want to continue connecting"],
        timeout=12)
if index == 0:
    child.send('aabbcc\r')
elif index == 1:
    child.sendline('yes')
child.expect(["--More--", "[^<\n\r]*[>$#]\s*$", pexpect.EOF, pexpect.TIMEOUT])
print(child.before + child.after)
print('====', 'loin completed..')

send_control(child, 'c')
send_cmd(child, 'system-view')
send_cmd(child, 'show logbuffer size 1 | include "^Actual buffer size"')
send_control(child, 'z')
send_cmd(child, 'display copyright')
send_cmd(child, 'display debugging')


#while True:
#    print('cmd wait sleep ...')
#    time.sleep(10)
#    break
#with open(log_file, 'r') as f:
#    for line in f:
#        print(line)

    

