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
schd -c conf/schd.yaml
```

## local scheduler
default 

conf/schd.yaml
```
scheduler_cls: LocalScheduler
```

## remote scheduler
schedule by RemoteScheduler (schd-server)

conf/schd.yaml
```
scheduler_cls: RemoteScheduler
scheduler_remote_host: http://localhost:8899/
worker_name: local
```


# Email Notifier

Send email notification when job run failed.

In schd.yaml
``` yaml
email:
  smtp_server: smtp.gmail.com
  smtp_user: yourname@gmail.com
  smtp_password: xxx
  from_addr: yourname@gmail.com
  to_addr: yourname@gmail.com
  smtp_port: 587
  smtp_starttls: true
```

Or use environments instead.

environments:
``` bash
export SCHD_SMTP_USER='yourname@gmail.com'
export SCHD_SMTP_PASS='xxx'
export SCHD_SMTP_SERVER='smtp.gmail.com'
export SCHD_SMTP_FROM="yourname@gmail.com"
export SCHD_SMTP_TO="yourname@gmail.com"
export SCHD_SMTP_PORT=25
export SCHD_SMTP_TLS=false
```
