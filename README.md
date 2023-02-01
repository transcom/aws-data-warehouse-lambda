# Description

This AWS Lambda function serves as a means of replicating data from a RDS
database to a third-party's S3 bucket. Specifically, it retrieves changes
made to a audit history table in the RDS database and exports changes to an
S3 bucket in the same AWS account. Additionally, the Database schema is also
written out as well.

## Building a new version

You will want to edit the source files for this Lambda in the ./src
directory. Once you are ready to build a new release package you can do
the following:

`./build.sh`

The build script will ask you the version number you would like to build
and create a release package (.zip) in the main directory with that version
in the file name.

## Tracking changes to source

After a release it is highly recommended that you commit your changes to
the repo. You can revert a previous commit if you would like to build a
previous version.

## Initial SSM Seed

SSM Should be seeded with initial JSON data to coordinate the first export
and would look like this:

```json
{
    "data":
    {
        "lastRunTime": "1970-01-01 00:00:00.000000",
        "serialNumber": 0
    },
    "schema":
    {
        "serialNumber": 0,
        "lastMD5Hash": "None"
    }
}
```
