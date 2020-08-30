#!/bin/sh

#echo new cron into cron file
echo "*/59 * * * * /usr/local/bin/python /routes.py >> /var/log/cron.log 2>&1" >> mycron

#install new cron file
crontab mycron
rm mycron

touch /var/log/cron.log
