# icat-database-checker

This script checks the iRODS ICAT database for unexpected issues, specifically:
- Referential integrity issues
- Timestamp order issues (creation timestamp is later than modification timestamp)
- Creation or modification timestamps that refer to the future
- Object names that contain characters which are not handled correctly on XML-based clients,
  such as the python-irods-client (See https://github.com/irods/irods/issues/4132 for details).
- Data objects with empty names
- Collection and data object names with trailing slashes (See https://github.com/irods/irods/issues/3892)
- Files in vaults that have a directory name which is inconsistent with the collection name
- Hard links: multiple data objects refer to the same physical file
- Duplicate replica: multiple replica entries for the same file
- Data objects with too few replicas (the default minimum is one replica)
- Missing indexes

The present version of the script is suitable for PostgreSQL databases. It is compatible
with iRODS 4.2.x, 4.3.x. and 5.0.x.

# Requirements

It is recommended to use Python 3.8 or higher. Older Python versions are not supported.

You'll also need tools to build the psycopg2 package. Example command for CentOS 7:

```
sudo yum -y install python3 python3-devel python-virtualenv gcc git postgresql-devel postgresql-libs
```

Example command for Ubuntu 20.04 LTS:

```
sudo apt install -y python3.8 python3.8-venv python3.8-dev python3-wheel gcc libpq5
```

# Installation

The script can be installed virtual environment, typically in the irods account on an iRODS server.

If the system doesn't have the virtualenv module yet, install it first: _sudo python3 -m pip install virtualenv_

Now create a virtual environment and install the tool:
- _python3 -m virtualenv venv_
- _source venv/bin/activate_
- _pip3 install ./icat-database-checker_

# Usage

```
usage: icat-database-checker [-h] [--config-file CONFIG_FILE] [-m {human,csv}] [-v]
                             [-o OUTPUT]
                             [--run-test {ref_integrity,timestamps,names,hardlinks,minreplicas,path_consistency,indexes,all}]
                             [--ref-integrity-check REF_INTEGRITY_CHECK]
                             [--min-replicas MIN_REPLICAS]
                             [--data-object-prefix DATA_OBJECT_PREFIX]

Performs a number of sanity checks on the iRODS ICAT database

optional arguments:
  -h, --help            show this help message and exit
  --config-file CONFIG_FILE
                        Location of the irods server_config file (default:
                        etc/irods/server_config.json )
  -m {human,csv}        Type of output
  -v                    Verbose mode
  -o OUTPUT, --output OUTPUT
                        Output file (default: standard output)
  --run-test {ref_integrity,timestamps,names,hardlinks,minreplicas,path_consistency,indexes,all}
                        Test to run (default: all)
  --ref-integrity-check REF_INTEGRITY_CHECK
                        Comma-separated list of specific referential integrity checks to run.
                        This option has no effect if referential integrity checks have been
                        disabled using the --run-test option. In order to get a list of
                        supported referential integrity checks, run the tool with the --run-
                        test ref_integrity --ref-integrity-check '' -v options. By default,
                        the tool runs all referential integrity checks.
  --min-replicas MIN_REPLICAS
                        Minimum number of replicas that a dataobject must have (default: 1).
  --data-object-prefix DATA_OBJECT_PREFIX
                        Only check data objects with a particular prefix. The referential
                        integrity and hard links tests do not support this option yet, and
                        will ignore it.
```

By default, the script retrieves the database connection parameters from the iRODS server configuration file.
It is possible to override the server config file location using the --config-file parameter, like so:
_./icat-database-checker --config-file my-server-config.json_ . 

By default, the script only displays (potential) issues.  Use the -v (verbose mode) switch to print additional
information about which checks are performed.
