#!/usr/bin/env python
import sys
import subprocess
import click
import time
import json
import tempfile
from avro.datafile import DataFileReader
from avro.io import DatumReader
from deepdiff import DeepDiff

# from google.cloud import gstorage
# https://console.cloud.google.com/iam-admin/serviceaccounts/details/101272715562618766186?project=md-stag-16&supportedpurview=project
# TODO:  Consider using gstorage = storage.Client()

GCP_BUCKET_LOCATION = "gs://md_stag_graphdata/"


def get_client_test_avros(client):
    test_avros = {}
    client_url = f"{GCP_BUCKET_LOCATION}{client}/"
    test_urls = subprocess.run(f"gsutil ls -la {client_url}", capture_output=True, shell=True, text=True).stdout
    for avro in test_urls.splitlines(keepends=False):
        if avro[0:avro.find(' 20')].strip() and avro.endswith('.avro') and not avro.startswith('TOTAL'):
            avro_file = avro[avro.find('gs://'):avro.find('#')].replace(client_url, '')
            test_num = avro_file[0:avro_file.find('-')]
            if test_num not in test_avros:
                test_avros[test_num] = [avro_file]
            else:
                test_avros[test_num].append(avro_file)
    return test_avros


@click.group()
def cli():
    pass


@click.command()
def list_clients():
    client_urls = subprocess.run(f'gsutil ls {GCP_BUCKET_LOCATION}', capture_output=True, shell=True, text=True).stdout
    for client in client_urls.splitlines(keepends=False):
        click.echo(client.replace(GCP_BUCKET_LOCATION, '')[0:-1])


@click.command()
@click.argument('client')
def list_avros(client):
    total_files = 0
    total_cohort = 0
    total_pair = 0
    total_tag = 0
    total_bias = 0
    total_prediction = 0
    total_lift = 0
    total_customer = 0
    total_overall = 0
    other_types = []
    location = GCP_BUCKET_LOCATION + client + '/'
    client_urls = subprocess.run(f'gsutil ls {location}', capture_output=True, shell=True,
                                 text=True).stdout
    for client in client_urls.splitlines(keepends=False):
        if client.endswith('.avro'):
            total_files += 1
            if 'COHORT-' in client:
                total_cohort += 1
            elif 'PAIR-' in client:
                total_pair += 1
            elif 'TAG-' in client:
                total_tag += 1
            elif 'lift-manifest' in client:
                total_lift += 1
            elif 'bias' in client:
                total_bias += 1
            elif 'prediction-table' in client:
                total_prediction += 1
            elif 'customer' in client:
                total_customer += 1
            elif 'OVERALL' in client:
                total_overall += 1
            else:
                other_types.append(client.split('.')[1])
            click.echo(client.replace(location, ''))
        else:
            other_types.append(client)


    click.echo(f'\n{total_files} total avro files, including:')
    click.echo(f'  {total_pair} site pairs')
    click.echo(f'  {total_tag} tags')
    click.echo(f'  {total_cohort} cohorts')
    click.echo(f'  {total_lift} site/store pair lift manifests')
    click.echo(f'  {total_bias} bias manifests')
    click.echo(f'  {total_prediction} prediction tables')
    click.echo(f'  {total_customer} customer')
    click.echo(f'  {total_overall} overall')
    click.echo('  And these other variations:')
    if not other_types:
        click.echo('    <NONE>')
    else:
        for variation in other_types:
            print(f'    {variation}')

    # TODO:  THIS IMPLEMENTATION HAS PROBLEMS:
    # Execution took: 1069.295246403, no results printed see:
    #   https://codeutility.org/python-how-to-get-list-of-folders-in-a-given-bucket-using-google-cloud-api-stack-overflow/
    # start = time.perf_counter()
    # blobs = gsc.list_blobs(GCP_BUCKET_NAME)
    # count = 1
    # for blob in blobs:
    #     if not count % 100:
    #         print(count)
    #     if blob.name.endswith("/"):
    #         click.echo(blob.name)
    # time_elapsed = time.perf_counter() - start
    # print(f"\n\nExecution took: {time_elapsed}\n\n", file=sys.stderr)


@click.command()
@click.argument('client')
def list_tests(client):
    test_avros = get_client_test_avros(client)

    for test, avros in test_avros.items():
        click.echo(f"Test {test} has {len(avros)} avros.")
        for avro in avros:
            click.echo(avro)


@click.command()
@click.argument('client1')
@click.argument('client2')
def compare_clients(client1, client2):
    test_avros1 = get_client_test_avros(client1)
    test_avros2 = get_client_test_avros(client2)
    diff = DeepDiff(test_avros1, test_avros2, ignore_order=True)
    if len(diff.to_dict().items()) == 0:
        click.echo("IDENTICAL")
        return test_avros1
    else:
        click.echo(diff.to_json(indent=2))
        return diff


@click.command()
@click.argument('client')
@click.argument('avro_file')
def dump_avro(client, avro_file):
    # TODO:  THIS IMPLEMENTATION HAS PROBLEMS:
    # path = f"md_stag_graphdata/{client}"
    # bucket = gstorage.bucket(path)
    # blob = bucket.blob(avro_file)
    # contents = blob.download_as_string()
    # print(contents)

    with tempfile.TemporaryDirectory() as tempdir:
        client_url = f"gs://md_stag_graphdata/{client}/{avro_file}"
        print(f"Writing {client_url} to: {tempdir}", file=sys.stderr)
        results = subprocess.run(['gsutil cp', client_url, tempdir], capture_output=True, shell=True, text=True).stdout
        with open(f"{tempdir}/{avro_file}", "rb") as fo:
            avro_reader = DataFileReader(open(avro_file, "rb"), DatumReader())
            rows = []
            for record in avro_reader:
                rows.append(record)
            print(json.dumps(rows, indent=2))
            print(f'Dumped {len(rows)} records', file=sys.stderr)


@click.command()
@click.argument('avro1')
@click.argument('avro2')
def diff_avros(avro1, avro2):
    with open(avro1) as file1:
        with open(avro2) as file2:
            rows1 = json.load(file1)
            rows2 = json.load(file2)
            diff = DeepDiff(rows1, rows2, ignore_order=True)
            click.echo(diff.to_json(indent=2))


cli.add_command(list_clients)
cli.add_command(list_avros)
cli.add_command(list_tests)
cli.add_command(compare_clients)
cli.add_command(dump_avro)
cli.add_command(diff_avros)


if __name__ == '__main__':
    start = time.perf_counter()
    cli()
    time_elapsed = time.perf_counter() - start
    print(f"\n\nExecution took: {time_elapsed}\n\n")
