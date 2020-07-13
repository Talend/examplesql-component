# Talend ExampleSQL Component #
### Author: Thomas Bennett <tbennett@talend.com> ###

As of this writing this component will work with the following Talend platforms
* Talend Studio 7.2.1+
* Talend Cloud

Talend Pipeline Designer is currently in testing for this component and will be validated soon.

## How to setup in your environment ##
On the same machine as your Talend Studio
### PreRequisites ###
1. Java 1.8 JRE

### Deploy ###
1. Download examplesql-component-1.0.0.car
2. java -jar examplesql-component-1.0.0.car studio-deploy --location <Talend Studio Home>
EX: /Applications/TalendStudio-7.2.1/studio
3. Edit <Path to Talend Studio>/configuration/config.ini and add the following at the end of the file **talend.component.server.icon.paths=icons/%s_icon32.png,icons/png/%s_icon32.png**
4. Start or Restart Talend Studio

## Documentation ##
### ExampleSQLTableNameInput

Executes a DB query with a strictly defined order which must correspond to the schema definition.

### ExampleSQLTableNameInput

 connects to a given database and extracts fields based on a user-defined query. Then it passes the selected data to the next component via a  **Main**  row link.

For more technologies supported by Talend, see [Talend components](https://help.talend.com/access/sources/DITA_64_new/topic?pageid=st-ExampleSQL_link&amp;mapid=talend_components&amp;afs:lang=en&amp;EnrichVersion=6.4).

[Writing data to and reading data from a ExampleSQL database table](https://help.talend.com/reader/9SHLCAb6AuCDplv0LLvplw/dyacc2c7SVdI2JtUzGZBTA)

#### ExampleSQLTableNameInput Standard properties

These properties are used to configure ExampleSQLTableNameInput running in the Standard Job framework.

The Standard ExampleSQLTableNameInput component belongs to the Databases families.

The component in this framework is available in all [Talend products with Big Data](https://www.talend.com/products/big-data/big-data-compare-all).

## Basic settings

| **Property type** | Either  **Built-in**  or  **Repository**  .Since version 5.6, both the  **Built-In**  mode and the  **Repository**  mode are available in any of the _Talend _solutions. |
| --- | --- |
|   | **Built-in** : No property data stored centrally. |
|   | **Repository** : Select the repository file in which the properties are stored. The fields that follow are completed automatically using the data retrieved. |
| **Use an existing connection** | Select this check box and in the  **Component List**  click the relevant connection component to reuse the connection details you already defined. **Note:**  When a Job contains the parent Job and the child Job, if you need to share an existing connection between the two levels, for example, to share the connection created by the parent Job with the child Job, you have to:
1. In the parent level, register the database connection to be shared in the  **Basic settings**  view of the connection component which creates that very database connection.
2. In the child level, use a dedicated connection component to read that registered database connection.
For an example about how to share a database connection across Job levels, see _Talend Studio User Guide_. |
| **jdbcUrl** | Enter the jdbc compliant url to ExampleSQL along with additional jdbc parameters.This component utilizes the MariaDB driver. |
| **Username**  and  **Password** | Enter the authentication data used to connect to the ExampleSQL database to be used. |
| **Schema**  and  **Edit Schema** | A schema is a row description. It defines the number of fields (columns) to be processed and passed on to the next component. The schema is either  **Built-In**  or stored remotely in the  **Repository**.This component offers the advantage of the dynamic schema feature. This allows you to retrieve unknown columns from source files or to copy batches of columns from a source without mapping each column individually. For further information about dynamic schemas, see _Talend Studio User Guide_.This dynamic schema feature is designed for the purpose of retrieving unknown columns of a table and is recommended to be used for this purpose only; it is not recommended for the use of creating tables. |
|   | **Built-In** : You create and store the schema locally for this component only. Related topic: see _Talend Studio User Guide_. |
|   | **Repository** : You have already created the schema and stored it in the Repository. You can reuse it in various projects and Job designs. Related topic: see _Talend Studio User Guide_. |
|   | Click  **Edit schema**  to make changes to the schema. If the current schema is of the  **Repository**  type, three options are available:
- **View schema** : choose this option to view the schema only.
- **Change to built-in property** : choose this option to change the schema to  **Built-in**  for local changes.
- **Update repository connection** : choose this option to change the schema stored in the repository and decide whether to propagate the changes to all the Jobs upon completion. If you just want to propagate the changes to the current Job, you can select  **No**  upon completion and choose this schema metadata again in the **[Repository Content]** window.
 |
| **Table**   **Name** | Name of the table to be read. |

### ExampleSQLQueryInput

Executes a DB query with a strictly defined order which must correspond to the schema definition.

### ExampleSQLQueryInput

 connects to a given database and extracts fields based on a user-defined query. Then it passes the selected data to the next component via a  **Main**  row link.

For more technologies supported by Talend, see [Talend components](https://help.talend.com/access/sources/DITA_64_new/topic?pageid=st-ExampleSQL_link&amp;mapid=talend_components&amp;afs:lang=en&amp;EnrichVersion=6.4).

[Writing data to and reading data from a ExampleSQL database table](https://help.talend.com/reader/9SHLCAb6AuCDplv0LLvplw/dyacc2c7SVdI2JtUzGZBTA)

#### ExampleSQLQueryInput Standard properties

These properties are used to configure ExampleSQLQueryInput running in the Standard Job framework.

The Standard ExampleSQLQueryInput component belongs to the Databases families.

The component in this framework is available in all [Talend products with Big Data](https://www.talend.com/products/big-data/big-data-compare-all).

## Basic settings

| **Property type** | Either  **Built-in**  or  **Repository**  .Since version 5.6, both the  **Built-In**  mode and the  **Repository**  mode are available in any of the _Talend _solutions. |
| --- | --- |
|   | **Built-in** : No property data stored centrally. |
|   | **Repository** : Select the repository file in which the properties are stored. The fields that follow are completed automatically using the data retrieved. |
| **Use an existing connection** | Select this check box and in the  **Component List**  click the relevant connection component to reuse the connection details you already defined. **Note:**  When a Job contains the parent Job and the child Job, if you need to share an existing connection between the two levels, for example, to share the connection created by the parent Job with the child Job, you have to:|
| | 1. In the parent level, register the database connection to be shared in the  **Basic settings**  view of the connection component which creates that very database connection. |
| | 2. In the child level, use a dedicated connection component to read that registered database connection. For an example about how to share a database connection across Job levels, see _Talend Studio User Guide_. |
| **jdbcUrl** | Enter the jdbc compliant url to ExampleSQL along with additional jdbc parameters.This component utilizes the MariaDB driver. |
| **Username**  and  **Password** | Enter the authentication data used to connect to the ExampleSQL database to be used. |
| **Schema**  and  **Edit Schema** | A schema is a row description. It defines the number of fields (columns) to be processed and passed on to the next component. The schema is either  **Built-In**  or stored remotely in the  **Repository**.This component offers the advantage of the dynamic schema feature. This allows you to retrieve unknown columns from source files or to copy batches of columns from a source without mapping each column individually. For further information about dynamic schemas, see _Talend Studio User Guide_.This dynamic schema feature is designed for the purpose of retrieving unknown columns of a table and is recommended to be used for this purpose only; it is not recommended for the use of creating tables. |
|   | **Built-In** : You create and store the schema locally for this component only. Related topic: see _Talend Studio User Guide_. |
|   | **Repository** : You have already created the schema and stored it in the Repository. You can reuse it in various projects and Job designs. Related topic: see _Talend Studio User Guide_. |
|   | Click  **Edit schema**  to make changes to the schema. If the current schema is of the  **Repository**  type, three options are available: **View schema** : choose this option to view the schema only.  **Change to built-in property** : choose this option to change the schema to  **Built-in**  for local changes. **Update repository connection** : choose this option to change the schema stored in the repository and decide whether to propagate the changes to all the Jobs upon completion. If you just want to propagate the changes to the current Job, you can select  **No**  upon completion and choose this schema metadata again in the **[Repository Content]** window.
 |
| **Query** | Must be a SELECT only query |

### ExampleSQLOutput

Reads data incoming from the preceding component in the Job and executes the action defined on a given ExampleSQL table and/or on the data contained in the table.

ExampleSQLOutput connects to a given ExampleSQL database and inserts, updates, upserts, deletes and bulk loads in that database.

For more technologies supported by Talend, see [Talend components](https://help.talend.com/access/sources/DITA_64_new/topic?pageid=st-ExampleSQL_link&amp;mapid=talend_components&amp;afs:lang=en&amp;EnrichVersion=6.4).

[Writing data to and reading data from a ExampleSQL database table](https://help.talend.com/reader/9SHLCAb6AuCDplv0LLvplw/dyacc2c7SVdI2JtUzGZBTA)

#### ExampleSQLOutput Standard properties

These properties are used to configure ExampleSQLOutput running in the Standard Job framework.

The Standard ExampleSQLOutput component belongs to the Big Data and the Databases families.

The component in this framework is available in all [Talend products with Big Data](https://www.talend.com/products/big-data/big-data-compare-all).

## Basic settings

| **Property type** | Either  **Built-in**  or  **Repository**  . |
| --- | --- |
|   | **Built-in** : No property data stored centrally. |
|   | **Repository** : Select the repository file in which the properties are stored. The fields that follow are completed automatically using the data retrieved. |
| **Use an existing connection** | Select this check box and in the  **Component List**  click the relevant connection component to reuse the connection details you already defined. **Note:**  When a Job contains the parent Job and the child Job, if you need to share an existing connection between the two levels, for example, to share the connection created by the parent Job with the child Job, you have to:|
|  | **1.** In the parent level, register the database connection to be shared in the  **Basic settings**  view of the connection component which creates that very database connection.|
|  | **2.** In the child level, use a dedicated connection component to read that registered database connection. For an example about how to share a database connection across Job levels, see _Talend Studio User Guide_.|
| **jdbcUrl** | Enter the jdbc compliant url to ExampleSQL along with additional jdbc parameters.This component utilizes the MariaDB driver. |
| **Username**  and  **Password** | Enter the user authentication data for connecting to the database to be used. |
| **Table** | Enter the name of the table to be written. Note that only one table can be written at a time **If table is not present, then under Custom you can enter the name of the table. The &#39;Create If Not Exists&#39; property must also be selected as this will then create the table in the ExampleSQL instance.** |
| **Create table if not exists** | The table is created if it does not exist. |
| **varcharLength** | If &#39;Create table if not exists&#39; is checked then you can enter the max size of varchar fields. Default: -1 This means typically 255 |
| **keys** | If &#39;Create table if not exists&#39; is checked OR Action on Data is Update / Upsert. Then user can enter which field(s) should be marked as keys |
| **ignoreUpdate** | If Action on Data is Update / Upsert then user can enter which field(s) should be ignored when component performs Update statement. |
| **rewriteBatchStatements** | If checked will create a single batch operation to be executed on the ExampleSQL instance |
| **truncateTable** | If checked will truncate data in the selected table before performing the given action selected. |
| **Action on data** | On the data of the table defined, you can perform:
| | **Insert** : Add new entries to the table. If duplicates are found, the job stops. **Update** : Make changes to existing entries.**Upsert** : Insert a new record. If the record with the given reference already exists, an update would be made. **Delete** : Remove entries corresponding to the input flow. **Bulk Load:** Uses the ExampleSQL bulk load command **Warning:** _You must specify at least one column as a primary key on which the _ **Update** _ and _ **Delete** _ operations are based. You can do that by clicking _ **Edit Schema** _ and selecting the check box(es) next to the column(s) you want to set as primary key(s). For an advanced use, click the _ **Advanced settings** _ view where you can simultaneously define primary keys for the update and delete operations. To do that: Select the _ **Use field options** _ check box and then in the _ **Key in update** _ column, select the check boxes next to the column name on which you want to base the update operation. Do the same in the _ **Key in delete** _ column for the deletion operation._ |
| **Schema**  and  **Edit schema** | A schema is a row description. It defines the number of fields (columns) to be processed and passed on to the next component. The schema is either  **Built-In**  or stored remotely in the  **Repository**.This component offers the advantage of the dynamic schema feature. This allows you to retrieve unknown columns from source files or to copy batches of columns from a source without mapping each column individually. For further information about dynamic schemas, see _Talend Studio User Guide_.This dynamic schema feature is designed for the purpose of retrieving unknown columns of a table and is recommended to be used for this purpose only; it is not recommended for the use of creating tables. |
|   | **Built-In** : You create and store the schema locally for this component only. Related topic: see _Talend Studio User Guide_. |
|   | **Repository** : You have already created the schema and stored it in the Repository. You can reuse it in various projects and Job designs. Related topic: see _Talend Studio User Guide_.When the schema to be reused has default values that are integers or functions, ensure that these default values are not enclosed within quotation marks. If they are, you must remove the quotation marks manually.For more details, see [Verifying default values in a retrieved schema](javascript:;). |
|   | Click  **Edit schema**  to make changes to the schema. If the current schema is of the  **Repository**  type, three options are available: **View schema** : choose this option to view the schema only. **Change to built-in property** : choose this option to change the schema to  **Built-in**  for local changes. **Update repository connection** : choose this option to change the schema stored in the repository and decide whether to propagate the changes to all the Jobs upon completion. If you just want to propagate the changes to the current Job, you can select  **No**  upon completion and choose this schema metadata again in the **[Repository Content]** window.|

## Advanced settings

| **Use batch size** | Select this check box to activate the batch mode for data processing. In the  **Batch Size**  field that appears when this check box is selected, you can type in the number you need to define the batch size to be processed. |
| --- | --- |# examplesql-component
