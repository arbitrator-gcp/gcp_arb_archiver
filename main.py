import json
from google.cloud import bigquery, firestore
import datetime as dt
import requests
import pytz
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
from pandas.io.json import json_normalize


bq_client = bigquery.Client()
fs_client = firestore.Client()

def bq_pusher(dataset, tbl_name, pdf):
    dataset_ref = bq_client.dataset(dataset)
    table_ref = dataset_ref.table(tbl_name)
    job = bq_client.load_table_from_dataframe(pdf, table_ref, location="US")
    job.result()  # Waits for table load to complete.
    assert job.state == "DONE"

def pdf_builder(gen, exch):
    # set nb keys and column names
    if exch == "rate":
        order = ['timestamp_ms', 'ZAR', 'EUR', 'USD', 'THB', 'BGN', 'JPY', 'NZD', 'DKK', 'TRY', 'CNY', 'HRK', 'RUB', 'ILS', 'NOK', 'HUF', 'INR', 'CHF', 'SEK', 'SGD', 'HKD', 'KRW', 'AUD', 'IDR', 'PHP', 'GBP', 'ISK', 'MXN', 'CAD', 'MYR', 'CZK', 'BRL', 'PLN', 'RON']
    else:
        order = ['timestamp_ms', 
        'result_price_high', 'result_price_low', 'result_price_last',
        'result_price_change_percentage', 'result_price_change_absolute', 'result_volume',
        'allowance_remaining', 'allowance_cost',
        'result_volumeQuote']

    cols = {"timestamp": "timestamp_ms"}

    # generator to dictionary
    dict_tmp = {doc.id:{key:doc.to_dict()[key] for key in doc.to_dict().keys()} for doc in gen}

    # dict to pdf
    pdf_tmp = pd.DataFrame.from_dict(dict_tmp, orient="index").rename(columns=cols).sort_values("timestamp_ms", ascending=False).reset_index(drop=True)

    # change field type for BQ
    pdf_tmp['timestamp_ms'] = pd.to_datetime(pdf_tmp['timestamp_ms']).values.astype('datetime64[ms]').astype(int) # convert to unix timestamp - milliseconds

    # floats conversion
    floats = list(pdf_tmp.columns)
    floats.remove("timestamp_ms")
    pdf_tmp[floats] = pdf_tmp[floats].astype(float)

    # reorder just cos
    pdf_tmp = pdf_tmp[order]

    return(pdf_tmp)

def archiver(request):

    # if the table exists, get the most recent record. if not, set to a historic date
    try:
        # find most recent BQ record - use kraken - maybe check all some time
        q_bq = "SELECT MAX(`timestamp_ms`) as max FROM `arbitrator-251115.exchange_archive.kraken`"
        bq_max_record = bq_client.query(q_bq).to_dataframe()["max"][0]
        bq_max_record = dt.datetime.fromtimestamp(bq_max_record/1000, tz=pytz.UTC) + dt.timedelta(seconds=1) # add 1 sec to allow for nanosecond scale in FS
        # print("table exists. Max: " + str(bq_max_record))
    except:
        d = (dt.datetime.now(pytz.utc))
        bq_max_record = d - dt.timedelta(hours=24*50)
        # print("table doesn't exist. Max: " + str(bq_max_record))

    # get most recent FS record to set the upper limit for the queries - avoids duplicates appearing in BQ due to long query run time
    q_fs = fs_client.collection(u'kraken').order_by(u'timestamp', direction=firestore.Query.DESCENDING).limit(1).get()
    dict_tmp = {doc.to_dict()["timestamp"] for doc in q_fs}
    fs_max_record = list(dict_tmp)[0]
    # print(fs_max_record)

    # query FS for the data generators
    kraken_gen = fs_client.collection(u'kraken').where(u'timestamp', u'>', bq_max_record).where(u'timestamp', u'<=', fs_max_record).get()
    luno_gen = fs_client.collection(u'luno').where(u'timestamp', u'>', bq_max_record).where(u'timestamp', u'<=', fs_max_record).get()
    rates_gen = fs_client.collection(u'rates').where(u'timestamp', u'>', bq_max_record).where(u'timestamp', u'<=', fs_max_record).get()

    # put the FS generator results into dataframes
    pdf_kraken_min = pdf_builder(kraken_gen, "kraken")
    pdf_luno_min = pdf_builder(luno_gen, "luno")
    pdf_rates_min = pdf_builder(rates_gen, "rate")

    # push results to BQ
    bq_pusher(dataset = "exchange_archive", tbl_name = "kraken", pdf = pdf_kraken_min)
    bq_pusher(dataset = "exchange_archive", tbl_name = "luno", pdf = pdf_luno_min)
    bq_pusher(dataset = "exchange_archive", tbl_name = "exchangeratesapi", pdf = pdf_rates_min)

    # # drop > 7 days records from FS
    # d = (dt.datetime.now(pytz.utc))
    # a_week_ago = d - dt.timedelta(hours=24*7)

    # # enable this and test once youre sure the archivals work
    # # fs_client.collection(u'kraken').where(u'timestamp', u'<', a_week_ago).delete()
    # # fs_client.collection(u'luno').where(u'timestamp', u'<', a_week_ago).delete()
    # # fs_client.collection(u'rates').where(u'timestamp', u'<', a_week_ago).delete()