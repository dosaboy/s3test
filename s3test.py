#!/usr/bin/env python2
"""
S3 API test tool. Can be used to test Ceph Rados Gateway S3 API.

Authors:
    Edward Hope-Morley <edward.hope-morley@canonical.com>
"""
import datetime
import os
import random
import string
import time

import argparse
import boto
from boto.s3 import (
    connection,
    key
)


def get_ec2_creds():
    from keystoneclient.v2_0 import client

    print "Fetching EC2 credentials from Keystone"
    admin_url = os.environ['OS_AUTH_URL'].replace('5000', '35357')
    keystone = client.Client(token='ubuntutesting',
                             endpoint=admin_url)
    t = [t.id for t in keystone.tenants.list() if t.name == 'admin'][0]
    u = [u.id for u in keystone.users.list(t) if u.name == 'admin'][0]
    c = keystone.ec2.list(u)
    if not c:
        print "No creds found, creating new ones"
        c = keystone.ec2.create(u, t)
        c = keystone.ec2.list(u)

    c = c[0]

    print "Access: %s Secret: %s" % (c.access, c.secret)
    return c.access, c.secret


parser = argparse.ArgumentParser()
parser.add_argument('--port', type=int, default=80, required=False)
parser.add_argument('--host', type=str, default='10.5.100.1', required=False)
parser.add_argument('-n', '--num-objs', type=int, default=1, required=False)
parser.add_argument('--bytes', type=int, default=1024 * 110, required=False)
parser.add_argument('--objnamelen', type=int, default=0, required=False)
args = parser.parse_args()

print "S3 endpoint is '%s:%s'" % (args.host, args.port)
access_key, secret_key = get_ec2_creds()
conn = boto.connect_s3(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    host=args.host,
    port=args.port,
    is_secure=False,
    calling_format=connection.OrdinaryCallingFormat(),
)

bname = 'testbucket'
bucket = conn.create_bucket(bname)
print "Bucket '{}' contains {} objects".format(bname, len(list(bucket.list())))
k = key.Key(bucket)

print "Creating {} {}KB random data files".format(args.num_objs,
                                                  args.bytes / 1024)
for n in xrange(args.num_objs):
    fname = '/tmp/rgwtestdata-%d' % (n)
    with open(fname, 'w') as fd:
        fd.write(os.urandom(args.bytes))

if args.num_objs:
    print "Uploading {} objects to bucket '{}'".format(args.num_objs, bname)
    d1 = datetime.datetime.now()
    for n in xrange(args.num_objs):
        objname = 'obj{}'.format("{}+{}".format(n, time.time()))
        ext = ""
        if args.objnamelen > len(objname):
            delta = args.objnamelen - len(objname)
            for i in xrange(0, delta):
                ext += random.choice(string.letters)

        k.key = objname+ext
        fname = '/tmp/rgwtestdata-{}'.format(n)
        k.set_contents_from_filename(fname)

    d2 = datetime.datetime.now()
    print "Done. ({})".format(d2 - d1)

print "\nExisting buckets:",
all_buckets = conn.get_all_buckets()
print ', '.join([b.name for b in all_buckets])
for b in all_buckets:
    num_objs = len([o.name for o in b.list()])
    print "Bucket '{}' contains {} objects".format(b.name, num_objs)

print "\nDone."
