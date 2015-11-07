#!/bin/sh

sudo yum install mysql
sudo yum install mysql-server 
sudo yum install mysql-devel 
sudo chgrp -R mysql /var/lib/mysql 
sudo chmod -R 770 /var/lib/mysql 
