pip install -r requirements.txt


gcloud functions deploy archiver --memory=256MB --runtime python37 --trigger-http
gcloud functions deploy archiver --memory=512MB --runtime python37 --trigger-http 

gcloud functions deploy archiver --memory=512MB --runtime python37 --trigger-topic exch_archiver_topic --timeout 540s