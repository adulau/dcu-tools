dcu-tools
=========

dcu-tools are tools used for fetching and analyzing (private) Microsoft Digital Crimes Unit feeds

Usage
=====

        Usage: dcu-fetch.py [options] dcu feed blob fetcher

        Options:
          -h, --help            show this help message and exit
          -d, --debug           output debug message on stderr
          -a ACCOUNT_NAME, --account_name=ACCOUNT_NAME
                                Microsoft Azure account name
          -k ACCOUNT_KEY, --account_key=ACCOUNT_KEY
                                Microsoft Azure key to access DCU container
          -c, --clear           Delete blobs and containers after fetching
          -e, --header          Remove field header in the output (default is
                                displayed)
          -f OUTPUT_FORMAT, --format=OUTPUT_FORMAT
                                output txt, json (default is txt)
          -t DATE, --date=DATE  date in format YYYY-MM-DD to limit the query (default
                                is all)


Dumping sink-hole addresses
---------------------------

    python ./bin/dcu-fetch.py -a <azure feed> -k "<azure key>" -f json | jq -r .TargetIp

Dumping some specific values and cleaning the container/blobs
----------------------------------------------------

    python ./bin/dcu-fetch.py -a <azure feed> -k "<azure key>" -f json | jq -r '.SourceIpAsnNr+" "+.SourceIp +" "+ .Botnet'

Dumping the JSON object for a specific ASN
------------------------------------------

    python ./bin/dcu-fetch.py -a <azure feed> -k "<azure key>" -f json | jq -r 'if .SourceIpAsnNr == "AS12345" then . else "" end'
