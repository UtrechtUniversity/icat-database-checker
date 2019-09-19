# icat-database-checker

This script checks the iRODS ICAT database for unexpected issues, specifically:
- Referential integrity issues
- Timestamp order issues (creation timestamp is later than modification timestamp)
- Creation or modification timestamps that refer to the future
- Empty object names
- Object names that contain characters which are not handled correctly on XML-based clients,
  such as the python-irods-client (See https://github.com/irods/irods/issues/4132 for details).
- Files in vaults that have a directory name which is inconsistent with the collection name
- Hard links: multiple data objects refer to the same physical file
- Duplicate replica: multiple replica entries for the same file

The present version of the script is suitable for Postgresql databases. It is compatible with iRODS 4.2.x.

# Requirements

The script requires Python 3, and has been tested with Python 3.6.

# Installation

The script can be installed virtual environment, typically in the irods account on an iRODS server.

If the system doesn't have the virtualenv module yet, install it first: _sudo python3 -m pip install virtualenv_

Now create a virtual environment for the script and install it:
- _python3 -m virtualenv venv_
- _source venv/bin/activate_
- _python3 -m pip install -r requirements.txt_

# Usage

Standard usage: _./icat-database-checker_

By default, the script retrieves the database connection parameters from the iRODS server configuration file.
It is possible to override the server config file location using the --config-file parameter, like so:
_./icat-database-checker --config-file my-server-config.json_ . 

By default, the script only displays (potential) issues.  Use the -v (verbose mode) switch to print additional
information about which checks are performed.
