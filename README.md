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


# Email Notifactor

```
error_notificator:
  type: email
  
  
```

environments:

export SMTP_USER='yourname@gmail.com'
export SMTP_PASS='xxx'
export SMTP_SERVER='smtp.gmail.com'
export SMTP_FROM="yourname@gmail.com"
export SCHD_ADMIN_EMAIL="yourname@gmail.com"
