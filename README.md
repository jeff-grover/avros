# avros
## A regression test and .avro file utilities 
### Purpose
This code is for validating calcs job outputs in google cloud storage, especially for answering the question... "Did I make a code change to calcs that inadvertently changed the metric output values?"

The intended users of this code are calcs team developers, data scientists and testers validating the metrics calculations against customer data in the staging feature environments.

### Setup
All the code in this repository is in Python.  You can either clone (if you want to make changes) or just download this repository as a .zip file and extract it to a project directory.  Then`cd` to that directory.

To set up your python/pyenv environment, see the first part confluence page (the Poetry steps do not apply for this project):

https://marketdial.atlassian.net/wiki/spaces/ENG/pages/1342930945/Poetry+Black+Setup

ALSO:  The steps specifically mention Python version 3.7.9, but feel free to install any version up to the latest, currently 3.10.x)

Instead of using poetry, you can simply install the dependencies for this project using `pip`:

```
pip install -r requirements.txt
```

That's it... you're all set!

### Prerequisites

This code assumes that the staging environment has been set up with at least two feature environments, as described here in confluence:

https://marketdial.atlassian.net/wiki/spaces/ENG/pages/1246756865/Feature+Environment+Management

The `create_reference.sh` script in this repository shows how to create one of those environments.

In most cases, the other environment will be one you, as a developer are working on, and this code will help you compare the reference (unchanged) calc job outputs to the results after having made some changes to the calcs code.

Of course, the last prerequisite is that the calcs job actually be run on *both* environments to produce the actual output files for the client and test or tests being examined.  The instructions for how to run calcs jobs using the `gcloud pubsub` method are contained in the README file of the calcs-light repository:

https://github.com/marketdial/calcs-light#readme

The script `reference_calcs_job.sh` also contains an example of running the calcs job.

You may ask, "Why two environments, why not just compare against staging itself?"  The answer has to do with when the customer data gets synchronized in the Staging environment.  It is different for the feature environments and staging itself.  In fact, you may find yourself having to re-build the environments to ensure that they both recently got the same customer data upon which to perform the calcs job calculations.

When in doubt about unexpected changes to the outputs, the first thing to try is to re-create your feature environments and re-run calcs on those environments.

### Usage

There are two commands available:

#### calvro

The "calvro" (calcs avro) command has a number of utilities to help view and compare the calcs output files as stored in google cloud.
```bash
% python calvro.py
Usage: calvro.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  compare-clients
  diff-avros
  dump-avro
  list-clients
  
```
The `calvro list-clients` utility allows you to list the various feature environment clients available.  It is essentially equivalent to this command:
```bash
gsutil ls gs://md_stag_graphdata/
```
The `calvro dump-avro client filename` command will print the contents of the specified avro filename for the given client (as JSON).  This may be redirected to a file and referenced later, for example.

The `calvro compare-clients reference changed` tool compares the avro filenames contained in the two buckets, and shows if they are identical or if files were new or missing in one or the other environment.

The `calvro diff-avros reference changed filename` option will compare a *single* avro filenames contained in the storage buckets for both clients, and shows if they are identical or if items were new or missing in that single avro file.


#### regression
The "regression" command will attempt to compare the outputs of two different feature environments, and show any differences that exist in a very specific way.

```bash
 % python regression.py

USAGE:  regression.py reference changed [test_id]

Where "reference" and "changed" are of the format: env_client
(i.e., ftrcalcsteststandard_maverick and ftrmar1415mychanges_maverick)
```
Although the `test_id` argument is optional, it is recommended, especially for large clients with many tests.  Running comparisons of all tests for a client can take a very, very long time and be very CPU intensive.

### Examples

