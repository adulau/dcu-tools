# -*- coding: utf-8 -*-
#
# dcu-fetch-ng.py is a Python script to fetch blobs published by DCU
#
# Standard python libraries (3.4 and up) + Azure Python SDK
#
# Copyright (C) 2013-2018 Alexandre Dulaunoy - alexandre.dulaunoy@circl.lu
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

#import StringIO
from io import StringIO, BytesIO
import azure
import azure.storage
from azure.storage.blob import BlockBlobService
#from azure.storage import BlobService
from optparse import OptionParser
import sys
import json
import re

debug = False
header = True
output_format = "txt"

#headers = ["SourcedFrom","FileTimeUtc", "Botnet", "SourceIp", "SourcePort", "SourceIpAsnNr", "TargetIp", "TargetPort", "Payload", "SourceIpCountryCode", "SourceIpRegion", "SourceIpCity", "SourceIpPostalCode", "SourceIpLatitude", "SourceIpLongitude", "SourceIpMetroCode", "SourceIpAreaCode", "HttpRequest", "HttpReferrer", "HttpUserAgent", "HttpMethod", "HttpVersion", "HttpHost", "Custom Field 1", "Custom Field 2", "Custom Field 3", "Custom Field 4", "Custom Field 5"]

usage = "usage: %prog [options] dcu feed blob fetcher"
parser = OptionParser(usage)
parser.add_option("-d","--debug", dest="debug", action='store_true', help="output debug message on stderr")
parser.add_option("-a","--account_name", dest="account_name", help="Microsoft Azure account name")
parser.add_option("-k","--account_key", dest="account_key", help="Microsoft Azure key to access DCU container")
parser.add_option("-c","--clear", dest="clear", action='store_true', help="Delete blobs and containers after fetching")
#parser.add_option("-e","--header", dest="header", action='store_true', help="Remove field header in the output (default is displayed)")
parser.add_option("-f","--format", dest="output_format", help="output txt, json (default is txt)")
parser.add_option("-t","--date", dest="date", help="date in format YYYY-MM-DD to limit the query (default is all)")

(options, args) = parser.parse_args()

if options.debug:
    debug=True

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


blob_service=azure.storage.blob.baseblobservice.BaseBlobService(account_name=account_name, account_key=account_key)


for container in blob_service.list_containers():
    c = container.name
    if c == "heartbeat": continue
    #if options.date and not ( re.match(options.date, c) ): continue
    if debug: sys.stderr.write("Processing container: {}\n".format(str(c)))
    for b in blob_service.list_blobs(c):
        if debug: sys.stderr.write("Processing blob: {}\n".format(str(b.name)))
        if options.date and not (re.match(options.date, b.name)): continue
        data = blob_service.get_blob_to_bytes(c, b.name)
        cs = BytesIO(data.content)
        gzipstream = gzip.GzipFile(fileobj=cs)
        if output_format == "txt":
            print (gzipstream.read().strip().decode('utf-8'))
        if options.clear:
            if debug: sys.stderr.write("Deleting blob: {}\n".format(str(b.name)))
            blob_service.delete_blob(c, b.name)
    if options.clear:
        if debug: sys.stderr.write("Deleting container: {}\n".format(str(c)))
        blob_service.delete_container(c)
