# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-18

* Minimum version is now Python 3.8+
* Add --ref-integrity--check parameter to select specific referential
  integrity checks, rather than running all of them. This can be useful
  on larger environments, where running all checks would be very time-consuming
  or even not feasible.
* Add support for iRODS 5.0.x

## [1.0.0] - 2022-11-08

First release version, for Python 3.6+.
