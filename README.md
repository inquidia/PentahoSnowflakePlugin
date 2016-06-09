# Pentaho Snowflake Plugin

This plugin was built as a collaboration between [Inquidia Consulting](http://inquidia.com) and [Snowflake Computing](http://snowflake.net).

The Snowflake Plugin for Pentaho Data Integration (PDI) includes the Snowflake database type and a bulk loader step to improve performance when loading a Snowflake database and a utility job entry to aid in resuming, suspending, and re-sizing warehouses.

## Snowflake Database Type

This plugin adds the Snowflake database type to the database list in Pentaho.  Instead of having to select the generic database type, specify the driver class, and JDBC connection string when using Snowflake, there is now a Snowflake database type.

Simply specify the account, database, username, and password and you can connect to Snowflake.  Additional configuration options such as warehouse, schema, and passcode may be configured on the Options tab.

## Snowflake Bulk Loader

The Snowflake Bulk Loader step utilizes the Snowflake Copy command to load data as opposed to sending individual insert statements through the Table Output step.  It performs this bulk load as a 3 step process:

  1. Write the data to local temp files.
  2. Run a put statement to copy the local files to a Snowflake stage.
  3. Run a copy command to bulk load the data from the Snowflake stage to a table.

### Options

#### Bulk loader tab

 - **Connection**: The database connection to use when bulk loading
 - **Schema**: (Optional) The schema containing the table being loaded.
 - **Table name**: The name of the table being loaded.
 - **Staging location type**: The type of Snowflake stage to use to store the files.
   * **User Location**: Uses the user's home directory to store the files being loaded.
   * **Table Location**: Uses the table's internal stage to store the files being loaded.
   * **Internal Stage**: Use an already created internal stage tos tore the files being loaded.
 - **Internal Stage Name**: (When Staging location type = Internal stage) The name of the internal stage to use.
 - **Work directory**: The local work directory to store temporary files before they are loaded to snowflake.
 - **On Error**: (Abort, Skip File, Skip File Percent, Continue) The behavior when errors are encountered on a load.
 - **Error limit**: (When On Error = Skip File or Skip File Percent) The error limit before the file should be skipped.  If empty or 0 the file will be skipped on the first error.
 - **Split load every ... rows**: Breaking the temp files into multiple smaller files will allow Snowflake to perform the bulk load in parallel, thus improving performance.  This is the number of rows each file should contain.
 - **Remove files after load**: (Y/N) Should the files be removed from the Snowflake stage after the load.  (Local temp files are always removed.)

#### Data type tab

 - **Data type**: The type of the data being bulk loaded.
   * **CSV**:
     * **Trim whitespace**: (Y/N) Should any whitespace around field values be trimmed when loading Snowflake.
     * **Null if**: A comma delimited list of strings that should be converted to null when loading Snowflake.  The strings do not need to be quoted.
     * **Error on column count mismatch**: If the number of columns in the table, do not match the number of columns output do not load the line and throw an error.
   * **JSON**: The data being loaded is received on the input stream in a single field containing JSON.
     * **Remove nulls**: Should nulls in the JSON be removed thus lowering the amount of storage required.
     * **Ignore UTF8 errors?**: Ignore any UTF8 character encoding errors when parsing the JSON.
     * **Allow duplicate elements**: Allow the JSON to contain the same element multiple times.  If the same element occurs multiple times, the last value for the element will be stored in Snowflake.
     * **Parse octal numbers**: Parse any numbers stored in the JSON as Octal instead of decimal.

#### Fields tab

 - **Data type CSV**
   * **Specifying fields**: (Y/N) Is the mapping of fields from Pentaho to Snowflake being explicitly specified.  If the mapping of the fields is not being specified, the order of the input fields to this step must match the order of the fields in the table.
   * **Field mapping table**: (When specifying fields is checked.)  Fields do not have to be in any order.
     * **Stream field**: The field on the input stream
     * **Table field**: The field in the table to map the input field to.
     * **Get fields button**: Gets the fields from the input stream, and maps them to a field of the same name in the table.
     * **Enter field mapping button**: Opens a window to help users specify the mapping of input fields to table fields.
 - **Data type JSON**
   * **JSON field**: The field on the input stream containing the JSON data to be loaded.

## Snowflake Warehouse Manager

The Snowflake Warehouse Manager job entry provides functionality to create, drop, resume, suspend, and alter warehouses.  This allows the ETL process to resume a warehouse before loading, and then suspend the warehouse as soon as it is done, along with resizing warehouses for portions of the load that may require more processing power.

### Options

 - **Connection**: The connection to the Snowflake database to use
 - **Warehouse name**: The name of the warehouse
 - **Action**: (Create, Drop, Resume, Suspend, Alter) What is the action you wish to take against the warehouse.

#### Action: Create warehouse

This action creates a new warehouse using the settings you provide.

 - **Replace?**: If the warehouse already exists, replace the existing warehouse with your new settings.
 - **Fail if warehouse exists?**: If the warehouse already exists the job entry will fail.  If not checked and Replace? is not checked the job entry will perform no action if the warehouse already exists.
 - **Warehouse Size**: (X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large) The size of the warehouse to create.
 - **Warehouse Type**: (Standard, Enterprise) The type of warehouse to create.  Enterprise warehouses have more memory available than Standard warehouses.
 - **Max cluster size**: The maximum cluster size for clustered warehouses.
 - **Min cluster size**: The minimum cluster size for clustered warehouses.
 - **Auto suspend**: The number of minutes of inactivity after which the warehouse will automatically suspend.
 - **Auto resume?**: Should the warehouse automatically resume when queries are run against it?
 - **Initially suspended?**: Should the warehouse be created in a suspended state.
 - **Resource monitor**: The resource monitor used for tracking Snowflake usage and billing.
 - **Comment**: A comment to be included about the warehouse.

#### Action: Drop warehouse

This action removes a warehouse from Snowflake.

 - **Fail if warehouse does not exist?**: If the warehouse does not exist, the job entry will fail.

#### Action: Resume warehouse

This action resumes a warehouse in Snowflake.  The warehouse must be in a suspended state prior to this job entry, or the job entry will fail.

 - **Fail if warehouse does not exist?**: If the warehouse does not exist, the job entry will fail.

#### Action: Suspend warehouse

This action suspends a warehouse in Snowflake.  The warehouse must be in a started state prior to this job entry, or the job entry will fail.

 - **Fail if warehouse does not exist?**: If the warehouse does not exist, the job entry will fail.

#### Action: Alter warehouse

This action alters the warehouse, enabling users to re-size, change warehouse types, change auto-suspend settings, etc.

 - **Fail if warehouse does not exist?**: If the warehouse does not exist, the job entry will fail.
 - **Warehouse Size**: (X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large) The size of the warehouse to create.
 - **Warehouse Type**: (Standard, Enterprise) The type of warehouse to create.  Enterprise warehouses have more memory available than Standard warehouses.
 - **Max cluster size**: The maximum cluster size for clustered warehouses.
 - **Min cluster size**: The minimum cluster size for clustered warehouses.
 - **Auto suspend**: The number of minutes of inactivity after which the warehouse will automatically suspend.
 - **Auto resume?**: Should the warehouse automatically resume when queries are run against it?
 - **Resource monitor**: The resource monitor used for tracking Snowflake usage and billing.
 - **Comment**: A comment to be included about the warehouse.

## Contributing

Thew Snowflake Plugin for PDI is a community supported plugin.  We encourage and support an active community that accepts contribution from the public -- including you!  Community contributions take all forms from filing issues, to improving documentation, to patching code or offering improvements.

To contribute:

 * The Snowflake Plugin for PDI uses Github as its issue tracking system.  Issues may be submitted at [https://github.com/inquidia/PentahoSnowflakePlugin/issues](https://github.com/inquidia/PentahoSnowflakePlugin/issues)
 * To improve documentation, or submit code improvements/patches fork this repository, make your changes, and submit a pull request to this repository.  We will then review your contribution and accept as appropriate.
 * The Snowflake Plugin for PDI follows the [Pentaho coding standards](https://github.com/pentaho/pentaho-coding-standards).  Please make sure any contributions follow these standards.

## Copyright & License

Copyright (C) 2016 by Inquidia Consulting : http://www.inquidia.com

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.