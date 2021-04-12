#!/bin/bash

P_NAME='apic_sim.py'

PID=$(ps -ef | grep python | grep -v 'grep' | grep "$P_NAME" | awk '{print $2}')

kill -9 $PID

