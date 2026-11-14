#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A stand-in program under test for the harness' own tests. Its cmd.exe twin, mytool.cmd, takes
# the same modes and prints the same thing, so a test drives one spelling on either shell.
#
# Usage: mytool echo <text...>     echoes its arguments, one space between them
#        mytool sleep <seconds>    stays alive for that long
#        mytool work <seconds> <status>
#                                  prints "starting", stays alive for that long, prints
#                                  "finished", exits with that status
#        mytool listen <port>      holds a TCP port open until killed

mode="$1"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$mode" in
  echo)
    echo "$*"
    ;;
  sleep)
    sleep "$1"
    ;;
  work)
    echo "starting"
    sleep "$1"
    echo "finished"
    exit "${2:-0}"
    ;;
  listen)
    echo "listening on $1"
    # nc is the only listener a POSIX shell can count on, and the -l spelling differs between its
    # BSD and GNU builds.
    nc -l "$1" >/dev/null 2>&1 || nc -l -p "$1" >/dev/null 2>&1
    ;;
  *)
    echo "usage: mytool echo <text...> | sleep <s> | work <s> <status> | listen <port>" >&2
    exit 2
    ;;
esac
