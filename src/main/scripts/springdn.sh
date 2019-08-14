#!/bin/bash

#
# SpringDN
#

action="$1"

pid_file="sdn.pid"

# Initialize stop wait time if not provided by the config file
[[ -z "$STOP_WAIT_TIME" ]] && STOP_WAIT_TIME="60"

# ANSI Colors
echoRed() { echo $'\e[0;31m'"$1"$'\e[0m'; }
echoGreen() { echo $'\e[0;32m'"$1"$'\e[0m'; }
echoYellow() { echo $'\e[0;33m'"$1"$'\e[0m'; }

isRunning() {
  ps -p "$1" &> /dev/null
}

start() {
    # Issue a warning if the application will run as root
    [[ $(id -u ${run_user}) == "0" ]] && { echoYellow "Application is running as root (UID 0). This is considered insecure."; }

    # Find Java
    if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
        javaexe="$JAVA_HOME/bin/java"
    elif type -p java > /dev/null 2>&1; then
        javaexe=$(type -p java)
    elif [[ -x "/usr/bin/java" ]];  then
        javaexe="/usr/bin/java"
    else
        echo "Unable to find Java"
        exit 1
    fi

    "$javaexe" -jar lib/springdn-0.0.1-SNAPSHOT.jar >> "app.log" 2>&1 &
    pid=$!
    disown $pid
    echo "$pid" > "$pid_file"

    [[ -z $pid ]] && { echoRed "Failed to start"; return 1; }
    echoGreen "Started [$pid]"
}

stop() {
  [[ -f $pid_file ]] || { echoYellow "Not running (pidfile not found)"; return 0; }
  pid=$(cat "$pid_file")
  isRunning "$pid" || { echoYellow "Not running (process ${pid}). Removing stale pid file."; rm -f "$pid_file"; return 0; }
  do_stop "$pid" "$pid_file"
}

do_stop() {
  kill "$1" &> /dev/null || { echoRed "Unable to kill process $1"; return 1; }
  for i in $(seq 1 $STOP_WAIT_TIME); do
    isRunning "$1" || { echoGreen "Stopped [$1]"; rm -f "$2"; return 0; }
    [[ $i -eq STOP_WAIT_TIME/2 ]] && kill "$1" &> /dev/null
    sleep 1
  done
  echoRed "Unable to kill process $1";
  return 1;
}

# Call the appropriate action function
case "$action" in
start)
  start "$@"; exit $?;;
stop)
  stop "$@"; exit $?;;
*)
  echo "Usage: $0 {start|stop}"; exit 1;
esac

exit 0