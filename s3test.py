#!/usr/bin/env python2
#
# S3 API test tool. Can be used to test Ceph Rados Gateway S3 API.
#
# Authors:
#    Edward Hope-Morley <edward.hope-morley@canonical.com>
#
import os
import time
import boto
import datetime
import argparse

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
args = parser.parse_args()

print "RadosGW endpoint is '%s:%s'" % (args.host, args.port)
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
print "Bucket '%s' contains %s objects" % (bname, len(list(bucket.list())))
k = key.Key(bucket)

print "Creating %s %dKB random data files" % (args.num_objs, args.bytes / 1024)
for n in xrange(args.num_objs):
    fname = '/tmp/rgwtestdata-%d' % (n)
    with open(fname, 'w') as fd:
        fd.write(os.urandom(args.bytes))

if args.num_objs:
    print "Uploading %d objects to bucket '%s'" % (args.num_objs, bname)
    d1 = datetime.datetime.now()
    for n in xrange(args.num_objs):
        k.key = 'obj%s' % ("%s+%s" % (n, time.time()))
        fname = '/tmp/rgwtestdata-%d' % (n)
        k.set_contents_from_filename(fname)

    d2 = datetime.datetime.now()
    print "Done. (%s)" % (d2 - d1)

print "\nExisting buckets:",
all_buckets = conn.get_all_buckets()
print ', '.join([b.name for b in all_buckets])
for b in all_buckets:
    print "Bucket '%s' contains %s objects" % (b.name, len([b.name for b in b.list()]))

print "\nDone."
