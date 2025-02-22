#!/bin/bash
set -e
DEFAULT_START_DATE=$(date -u --date='7 day ago' +"%Y%m%d")
START_DATE=${START_DATE:=$DEFAULT_START_DATE}
END_DATE=$(date -u --date='1 day ago' +"%Y%m%d")
DEFAULT_TABLES="contractinfo dailyquote marketcap treasury_yield_curve fundingrate"
TABLES=${TABLES:=$DEFAULT_TABLES}
PYTHON="python"
FORCE=${FORCE:="0"}

loop_date=$START_DATE
while [[ $loop_date -le $END_DATE ]] 
do
  echo $loop_date
  for table in $TABLES
  do
    table_filepath="data/rafdb/${table}/${loop_date:0:4}/${loop_date}/${table}.csv"
    if [ -f "$table_filepath" ] && [ "$FORCE" != "1" ] ; then
      echo "$table_filepath exists."
    else
      # echo "$table_filepath does not exist."
      cmd="$PYTHON -m rafdb.$table --date=$loop_date --output=$table_filepath"
      if [ "$FORCE" == "1" ] ; then
        cmd="$cmd --force"
      fi
      echo $cmd
      $cmd
    fi

  done 

  loop_date=$(date -d"$loop_date + 1 day" +"%Y%m%d")
done