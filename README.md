# schd
scheduler deamon.

start a daemon process to run a task periodically.

## Usage

conf/schd.yaml
```
jobs:
  ls:
    class: CommandJob
    cron: "* * * * *"   # run command each minute.
    cmd: "ls -l"
```

start a daemon

```
schd 
```