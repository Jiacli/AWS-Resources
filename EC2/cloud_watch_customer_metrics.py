#!/usr/bin/python
import os
import re
import sys
import boto
import boto.ec2
import boto.ec2.elb
import boto.ec2.cloudwatch
from boto.ec2.elb import HealthCheck
import time

max_TPS = 133.43

def get_queries():
    stream = os.popen("/usr/bin/mysql -u root -pdb15319root --execute=\"show status like \'Queries\'\"").read()
    index_stream = stream.find("Queries")
    number_stream = stream[index_stream+7:]
    query = (int)(number_stream)-3
    return query

def get_uptime():
    stream = os.popen("/usr/bin/mysql -u root -pdb15319root --execute=\"show status like \'Uptime\'\"").read()
    index = stream.find("Uptime")
    number_stream = stream[index+6:]
    time_get = int(number_stream)
    return time_get

def main(argv):
    print 'Customer Metric Collection Start!'
    instid = os.popen("/usr/bin/wget -q -O - http://169.254.169.254/latest/meta-data/instance-id").read()

    print 'Instance id -> ', instid

    elb = boto.ec2.cloudwatch.connect_to_region('us-east-1', aws_access_key_id='XXXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXX')
    queries_last = get_queries()
    time_last = get_uptime()
    queries_last = queries_last + 6
    time.sleep(60)

    while(True):
        queries_now = get_queries()
        time_now = get_uptime()
        query_per_second = (queries_now - queries_last) / (time_now - time_last) / 16
        ratio = query_per_second / max_TPS
        print ratio
        elb.put_metric_data("myMetrics/TPS","TPS Utilization",ratio,'','Percent',dict(InstanceID=instid),'')
        queries_last = queries_now + 6
        time_last = time_now
        time.sleep(60)
        
if __name__=="__main__":
    main(sys.argv)
