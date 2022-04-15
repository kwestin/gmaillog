from google.cloud import bigquery


def export(request):

    from flask import make_response

    import datetime
    import os

    partition_id = datetime.date.today().strftime('%Y%m%d')

    #req_data = request.get_json()

    #if req_data and req_data['partition']:
    #    partition_id = req_data['partition']

    destination_uri = "gs://{}/{}/{}".format(bucket_name, partition_id, "*.json")

    create_temp_table(project, dataset_id, dataset_location, table_id, partition_id)

    client = bigquery.Client()
    temp_dataset_id = "{}_temp".format(dataset_id)
    print("temp_dataset_id: {}".format(temp_dataset_id))

    dataset_ref = bigquery.DatasetReference(project, temp_dataset_id)
    print("dataset_ref: {}".format(dataset_ref))

    temp_table_id = "{}{}".format(table_id, partition_id)
    print("temp_table_id: {}".format(temp_table_id))

    table_ref = dataset_ref.table(temp_table_id)
    print("table_ref: {}".format(table_ref))

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

    client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location=dataset_location,
        job_config=job_config
    )  # API request

    print("client:{}".format(type(client)))

    print("Export {}:{}.{} to {} started.".format(project, temp_dataset_id, temp_table_id, destination_uri))
    return make_response('OK', 200)


def create_temp_table(project, dataset_id, dataset_location, table_id, partition_id):
    client = bigquery.Client(project=project)
    temp_dataset_id = "{}_temp".format(dataset_id)
    dataset_ref = bigquery.DatasetReference(project, temp_dataset_id)
    temp_table_id = "{}{}".format(table_id, partition_id)
    table_ref = dataset_ref.table(temp_table_id)

    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.write_disposition = 'WRITE_APPEND'

    sql = """
        SELECT *
        FROM `{}.{}.{}{}`
        WHERE TIMESTAMP_MICROS(event_info.timestamp_usec) > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -60 MINUTE)
    """.format(project, dataset_id, table_id, partition_id)
    print("sql:{}".format(sql))

    # Start the query, passing in the extra configuration.
    try:
      query_job = client.query(sql, job_config=job_config, location=dataset_location)  # Make an API request.
      query_job.result()  # Wait for the job to complete.
      result = query_job.result()
      print("Total rows available: ", result.total_rows)
    except Exception as e:
      print(e)

    print("Query results loaded to the table {}".format(temp_table_id))
