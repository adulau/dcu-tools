# -*- coding: utf-8 -*-
#
# dcu-fetch.py is a Python script to fetch blobs published by DCU
#
# Standard python libraries (>=2.6) + Azure Python SDK
#
# Copyright (C) 2013 Alexandre Dulaunoy - alexandre.dulaunoy@circl.lu
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import gzip
import StringIO
from azure.storage import BlobService
from optparse import OptionParser
import sys
import json
from bson import json_util

debug = False
header = True
output_format = "txt"

headers = ["SourcedFrom","FileTimeUtc", "Botnet", "SourceIp", "SourcePort", "SourceIpAsnNr", "TargetIp", "TargetPort", "Payload", "SourceIpCountryCode", "SourceIpRegion", "SourceIpCity", "SourceIpPostalCode", "SourceIpLatitude", "SourceIpLongitude", "SourceIpMetroCode", "SourceIpAreaCode", "HttpRequest", "HttpReferrer", "HttpUserAgent", "HttpMethod", "HttpVersion", "HttpHost", "Custom Field 1", "Custom Field 2", "Custom Field 3", "Custom Field 4", "Custom Field 5"]

usage = "usage: %prog [options] dcu feed blob fetcher"
parser = OptionParser(usage)
parser.add_option("-d","--debug", dest="debug", action='store_true', help="output debug message on stderr")
parser.add_option("-a","--account_name", dest="account_name", help="Microsoft Azure account name")
parser.add_option("-k","--account_key", dest="account_key", help="Microsoft Azure key to access DCU container")
parser.add_option("-c","--clear", dest="clear", action='store_true', help="Delete blobs and containers after fetching")
parser.add_option("-e","--header", dest="header", action='store_true', help="Remove field header in the output (default is displayed)")
parser.add_option("-f","--format", dest="output_format", help="output txt, json (default is txt)")

(options, args) = parser.parse_args()

if options.debug:
    debug=True

if options.header:
    header=False

if options.output_format:
    output_format = options.output_format

if options.account_name:
    account_name=options.account_name
else:
    sys.stderr.write("Azure account name is missing")
    sys.exit(1)

if options.account_key:
    account_key=options.account_key
else:
    sys.stderr.write("Azure key is missing")
    sys.exit(1)

if header and not options.output_format:
    print '\t'.join(str(h) for h in headers)

blob_service = BlobService(account_name, account_key)
for container in blob_service.list_containers():
    c = container.name
    if c == "heartbeat": continue
    if debug: sys.stderr.write("Processing container: "+str(c)+"\n")
    for b in blob_service.list_blobs(c):
        if debug: sys.stderr.write("Processing blob: "+str(b.name)+"\n")
        data = blob_service.get_blob(c, b.name)
        cs = StringIO.StringIO(data)
        gzipstream = gzip.GzipFile(fileobj=cs)
        if output_format == "txt":
            print gzipstream.read()
        elif output_format == "json":
            d = {}
            i = 0
            ds = gzipstream.read()
            # some DCU entries contains more than 28 values (outside the
            # definition of the headers)
            for x in ds.strip().split("\t")[:27]:
                d[headers[i]] = x
                i=i+1
            print (json.dumps(d, sort_keys=True, default=json_util.default))
        if options.clear:
            if debug: sys.stderr.write("Deleting blob: "+str(b.name)+"\n")
            blob_service.delete_blob(c, b.name)
    if options.clear:
        if debug: sys.stderr.write("Deleting container: "+str(c)+"\n")
        blob_service.delete_container(c)
