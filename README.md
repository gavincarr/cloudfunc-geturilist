
CloudFunc-GetURIList
====================

CloudFunc-GetURIList is a GCP Cloud Function, written in Go, that allows you
to make (non-javascript) web requests at scale and capture HTTP/HTML responses,
using Google Cloud Storage buckets for input and output.

Using Cloud Functions allows you to make requests much scalably than running
similar code on your own machines - you can run 10 or 100 instances of your
function just as easily as one.

GetURIList processes a list of URLs (a `text/uri-list` file, one URL per line)
uploaded to an input Google Storage Bucket object, fetches the content at each
url (following redirects), and saves the final content (and HTTP response headers)
to an output Google Storage Bucket object, one object per requested url.
Output objects can be named in various ways, but default to using the sha1 hash
of the requested url. Output files are in WARC (Web ARChive v1.1) format,
gzip-compressed.

Because cloud functions have a limited timespan (540s/9min), you will have to
experiment to see how many urls you can process at a given concurrency level
before you start getting jobs being killed before completion - 300 per file
seems to be a good conservative starting point.

Installation
------------

```sh
git clone https://github.com/gavincarr/cloudfunc-geturilist
```


Example Usage
-------------

```sh
NAME=cloudfunc-geturilist
BUCKET_IN=ofn-gul-in        # GCS Bucket where input uri-lists are copied to
BUCKET_OUT=ofn-gul-out      # GCS Bucket where output response archives are written
GOVER=go113
CONCURRENCY=3               # number of requests for each instance to run concurrently
MAX_INSTANCES=10            # number of instances to run
MEMORY=512MB                # memory allocation for each instance

# Deploying
gcloud functions deploy "$NAME" --entry-point=GetURIList --runtime "$GOVER" \
  --trigger-event google.storage.object.finalize --trigger-resource "$BUCKET_IN" \
  --timeout t9m --allow-unauthenticated \
  --max-instances "$MAX_INSTANCES" --memory "$MEMORY" \
  --set-env-vars "GUL_OUTPUT_BUCKET=$BUCKET_OUT,GUL_CONCURRENCY=$CONCURRENCY"

# Triggering
FILE=urls1.txt.gz
gsutil ls gs://$BUCKET_IN
gsutil cp $FILE gs://$BUCKET_IN/
gcloud functions logs read --limit 50

# Once completed...
gsutil ls gs://$BUCKET_OUT
gsutil -m cp gs://$BUCKET_OUT/* .
gsutil -m rm gs://$BUCKET_OUT/*
```


Copyright and Licence
---------------------

Copyright 2021 Gavin Carr <gavin@openfusion.com.au>.

This project is licensed under the terms of the MIT license.

