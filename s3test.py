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


def get_ec2_creds_v2():
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

    return c[0]


def get_ec2_creds_v3():
    from keystoneauth1.identity import v3
    from keystoneauth1 import session
    from keystoneclient.v3 import client
    admin_url = os.environ['OS_AUTH_URL'].replace('5000', '35357')
    project_name = (os.environ.get('OS_PROJECT_NAME') or
                    os.environ['OS_TENANT_NAME'])
    auth = v3.Password(
        user_domain_name=os.environ.get('OS_USER_DOMAIN_NAME'),
        username=os.environ['OS_USERNAME'],
        password=os.environ['OS_PASSWORD'],
        domain_name=os.environ.get('OS_DOMAIN_NAME'),
        project_domain_name=os.environ.get('OS_PROJECT_DOMAIN_NAME'),
        project_name=project_name,
        auth_url=admin_url,
    )
    sess = session.Session(auth=auth)
    ksclient = client.Client(session=sess)
    ksclient.auth_ref = auth.get_access(sess)
    domain_name = os.environ.get('OS_PROJECT_DOMAIN_NAME', 'default')
    d = [d.id for d in ksclient.domains.list() if d.name == domain_name][0]
    p = [p.id for p in ksclient.projects.list(domain=d)
         if p.name == project_name][0]
    u = [u.id for u in ksclient.users.list(project=p, domain=d)
         if u.name == 'admin'][0]
    c = ksclient.ec2.list(u)
    if c:
        print "Using existing EC2 creds"
    else:
        print "No creds found, creating new ones"
        ksclient.ec2.create(u, p)
        c = ksclient.ec2.list(u)

    return c[0]


def get_ec2_creds():
    try:
        c = get_ec2_creds_v2()
    except:
        c = get_ec2_creds_v3()

    print "EC2 credentials [access: %s secret: %s]" % (c.access, c.secret)
    return c.access, c.secret


parser = argparse.ArgumentParser()
parser.add_argument('--port', type=int, default=80, required=False)
parser.add_argument('--host', type=str, default='10.5.100.1', required=False)
parser.add_argument('-n', '--num-objs', type=int, default=1, required=False)
parser.add_argument('--bytes', type=int, default=1024 * 110, required=False)
parser.add_argument('--objnamelen', type=int, default=0, required=False)
parser.add_argument('--bucket', type=str, default='testbucket',
                    required=False)
args = parser.parse_args()

print "Using S3 endpoint '%s:%s'" % (args.host, args.port)
access_key, secret_key = get_ec2_creds()
conn = boto.connect_s3(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    host=args.host,
    port=args.port,
    is_secure=False,
    calling_format=connection.OrdinaryCallingFormat(),
)

bname = args.bucket
all_buckets = conn.get_all_buckets()
bucket = [b for b in all_buckets if b.name == bname]
if bucket:
    bucket = bucket[0]
    num_objs = len(list(bucket.list()))
    print('Bucket {} already exists and contains {} objects'
          .format(bucket.name, num_objs))
else:
    print 'Creating new bucket {}'.format(bname)
    bucket = conn.create_bucket(bname)

k = key.Key(bucket)

print "\nCreating {} {}KB random data files".format(args.num_objs,
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
