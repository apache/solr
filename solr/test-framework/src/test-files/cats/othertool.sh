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

# A second stand-in program, for the cases that need two programs under test at once. Its launcher
# has a name of its own, so a command that resolves proves which installation's launcher directory
# was on PATH. Its cmd.exe twin, othertool.cmd, takes the same modes.
#
# Usage: othertool echo <text...>     echoes its arguments, one space between them
#        othertool work <seconds>     prints "starting", stays alive for that long, prints "finished"

mode="$1"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$mode" in
  echo)
    echo "$*"
    ;;
  work)
    echo "starting"
    sleep "$1"
    echo "finished"
    ;;
  *)
    echo "usage: othertool echo <text...> | work <s>" >&2
    exit 2
    ;;
esac
