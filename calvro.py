#!/usr/bin/env python
import sys
import subprocess
import click
import time
import json
import tempfile
from avro.datafile import DataFileReader
from avro.io import DatumReader
from diff import compare

# from google.cloud import gstorage
# https://console.cloud.google.com/iam-admin/serviceaccounts/details/101272715562618766186?project=md-stag-16&supportedpurview=project
# TODO:  Consider using gstorage = storage.Client()

# TODO:  MetricDisplayData.json needs to be automatically refreshed from here:
#   https://github.com/marketdial/metric-config-service/blob/main/MetricDisplayData.json

GCP_BUCKET_LOCATION = "gs://md_stag_graphdata/"


class AvroCategorizer:
    def __init__(self, location):
        self.location = location
        self.total_files = 0
        self.total_cohort = 0
        self.total_pair = 0
        self.total_tag = 0
        self.total_bias = 0
        self.total_prediction = 0
        self.total_lift = 0
        self.total_customer = 0
        self.total_overall = 0
        self.other_types = []

    def add_avro(self, filename):
        if filename.endswith('.avro'):
            self.total_files += 1
            if 'COHORT-' in filename:
                self.total_cohort += 1
            elif 'PAIR-' in filename:
                self.total_pair += 1
            elif 'TAG-' in filename:
                self.total_tag += 1
            elif 'lift-manifest' in filename:
                self.total_lift += 1
            elif 'bias' in filename:
                self.total_bias += 1
            elif 'prediction-table' in filename:
                self.total_prediction += 1
            elif 'customer' in filename:
                self.total_customer += 1
            elif 'OVERALL' in filename:
                self.total_overall += 1
            else:
                self.other_types.append(filename.split('.')[1])
            # print(filename.replace(self.location, ''))
        else:
            self.other_types.append(filename)

    def tally_result(self):
        result = f'  {self.total_files} total avro files, including:'
        result += f'\n  {self.total_pair} site pairs'
        result += f'\n  {self.total_tag} tags'
        result += f'\n  {self.total_cohort} cohorts'
        result += f'\n  {self.total_lift} site/store pair lift manifests'
        result += f'\n  {self.total_bias} bias manifests'
        result += f'\n  {self.total_prediction} prediction tables'
        result += f'\n  {self.total_customer} customer'
        result += f'\n  {self.total_overall} overall'
        result += '\n  And these other variations:'
        if not self.other_types:
            result += '\n    <NONE>'
        else:
            for variation in self.other_types:
                result += f'\n    {variation}'

        return result


def get_client_test_avros(client):
    test_avros = {}
    client_url = f"{GCP_BUCKET_LOCATION}{client}/"
    test_urls = subprocess.run(f"gsutil ls -la {client_url}", capture_output=True, shell=True, text=True).stdout
    for avro in test_urls.splitlines(keepends=False):
        if avro[0:avro.find('#')].endswith('.avro') and not avro.startswith('TOTAL'):
            avro_file = avro[avro.find('gs://'):avro.find('#')].replace(client_url, '')
            test_num = avro_file[0:avro_file.find('-')]
            if test_num not in test_avros:
                test_avros[test_num] = [avro_file]
                print(f'Test {test_num}', file=sys.stderr)
            else:
                test_avros[test_num].append(avro_file)
    return test_avros


def get_avro_contents(client, avro_file):
    with tempfile.TemporaryDirectory() as tempdir:
        client_url = f"gs://md_stag_graphdata/{client}/{avro_file}"
        print(f"Writing {client_url} to: {tempdir}", file=sys.stderr)
        results = subprocess.run(f'gsutil cp {client_url} {tempdir}', capture_output=True, shell=True, text=True).stdout

        metric_data = {}
        with open(f"MetricDisplayData.json", "r") as metric_file:
            metric_data = json.load(metric_file)

        rows = []
        with open(f"{tempdir}/{avro_file}", "rb") as fo:
            avro_reader = DataFileReader(fo, DatumReader())
            for record in avro_reader:
                add_name = {'name': metric_data[str(record['uuid'])]['displayName']} | record
                rows.append(add_name)

        return rows


@click.group()
def cli():
    pass


@click.command()
def list_clients():
    client_urls = subprocess.run(f'gsutil ls {GCP_BUCKET_LOCATION}', capture_output=True, shell=True, text=True).stdout
    for client in client_urls.splitlines(keepends=False):
        print(client.replace(GCP_BUCKET_LOCATION, '')[0:-1])


@click.command()
@click.argument('client')
def list_avros(client):
    location = GCP_BUCKET_LOCATION + client + '/'
    client_urls = subprocess.run(f'gsutil ls {location}', capture_output=True, shell=True,
                                 text=True).stdout

    categories = AvroCategorizer(location)
    for client in client_urls.splitlines(keepends=False):
        categories.add_avro(client)

    print(categories.tally_result(), file=sys.stderr)


@click.command()
@click.argument('client')
def list_tests(client):
    test_avros = get_client_test_avros(client)
    print(f'\n{client} has the following {len(test_avros)} tests:')
    for test, avros in test_avros.items():
        # print(f"Test {test} has {len(avros)} avros.")
        categories = AvroCategorizer(client)
        for avro in avros:
            categories.add_avro(avro)
        print(f'\nTest {test}: {categories.tally_result()}')


@click.command()
@click.argument('client')
def list_metrics(client):
    overall_metrics = {}
    print(f'Getting test avros...', file=sys.stderr)
    test_avros = get_client_test_avros(client)
    print(f'\nProcessing {len(test_avros)} tests for {client} :', file=sys.stderr)
    for test, avros in test_avros.items():
        print(f'Test {test}', file=sys.stderr)
        for avro in avros:
            if 'OVERALL' in avro:
                rows = get_avro_contents(client, avro)
                for row in rows:
                    metric = f'{row["name"]} ({row["uuid"]})'
                    if metric in overall_metrics:
                        if test not in overall_metrics[metric]:
                            overall_metrics[metric].append(test)
                    else:
                        overall_metrics[metric] = []

    print(f'\nAll {len(overall_metrics)} metrics used by client {client}: \n{json.dumps(overall_metrics, indent=2)}')


@click.command()
@click.argument('client1')
@click.argument('client2')
def compare_clients(client1, client2):
    test_avros1 = get_client_test_avros(client1)
    test_avros2 = get_client_test_avros(client2)
    diff = compare(test_avros1, test_avros2)
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
    rows = get_avro_contents(client, avro_file)
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
            diff = compare(rows1, rows2)
            click.echo(diff.to_json(indent=2))


cli.add_command(list_clients)
cli.add_command(list_avros)
cli.add_command(list_metrics)
cli.add_command(list_tests)
cli.add_command(compare_clients)
cli.add_command(dump_avro)
cli.add_command(diff_avros)


if __name__ == '__main__':
    start = time.perf_counter()
    cli()
    time_elapsed = time.perf_counter() - start
    print(f"\n\nExecution took: {time_elapsed}\n\n")
