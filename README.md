# DR2 Custodial Copy

This repository contains six components which together make up the custodial copy service.

The principle of the Custodial Copy approach is described [here](https://zenodo.org/records/13647420)

## 1. Custodial Copy backend

This is a service which is intended to run in a long-running Docker container.

Every 10 seconds, it polls the queue specified in the `SQS_QUEUE_URL` environment variable.
This will be set to the queue which receives messages from the entity event generator.
For each invocation, it will try to fetch messages from the queue until there are either no more messages, or there are
50 messages.

### Messages

The queue sends messages in one of two formats:

```json
{
  "id": "io:1b9555dd-43b7-4681-9b0d-85ebe951ca02"
}
```

```json
{
  "id": "1b9555dd-43b7-4681-9b0d-85ebe951ca02"
}
```

For the first message, the prefix of the id can be `io`, `co` or `so` depending on the entity type; they are dealt with differently.
For the second message, there is no entity type as these messages are for deleted entities where we don't know the entity type.

#### Processing incoming messages based on message group ID.

The SQS queue which feeds this process is a FIFO queue. Each message has a UUID as a message group ID; this is the IO
UUID for an IO message or the parent of the CO for a CO message.

The process groups by the message group ID. Each group id corresponds to an OCFL object (which may not exist yet).

The messages within each group are deduplicated to prevent repeat processing of the same entity.

All updates for a single group of messages with the same group id are staged in OCFL; this allows us to write multiple
changes without creating new versions.

Once all messages are processed, we commit the changes to that OCFL object which writes the version permanently.

#### Handling Information Object (IO) messages

* Create the metadata file name `IO_Metadata.xml`
* Get the metadata from the Preservica API
* Create the destination path for this metadata file `{IO_REF}/IO_Metadata.xml`
* Wrap all returned metadata fragments in a `<AllMetadata/>` tag
* Calculate the checksum of this metadata string
* Use all of this information to create a `MetadataObject`

#### Handling Content Object (CO) messages

* Get the bitstream information (which contains the parent ID) from the Preservica API
* Verify that the parent ID is present
* Use parent ID to get the URLs of the representations
* Use URLs to get the COs under each representation
* Return the representation types (`{representationType}_{index}`) for a given CO ref
* If a CO has more than 1 representation type, throw an Exception
* Create the metadata file name `CO_Metadata.xml`
* Get the metadata from the Preservica API
* Create the destination path for this metadata file `{IO_REF}/{REP_TYPE}/{CO_REF}/{FILE_NAME}`
* Use metadata information to create a `MetadataObject`
* If the CO is an embedded file, this cannot be downloaded from Preservica. In this case, the url bitstream content url
  in the metadata will be empty, and we skip the final two steps.
* Create the destination path for the CO file `{IO_REF}/{REP_TYPE}/{CO_REF}/{GEN_TYPE}/g{GEN_VERSION}/{FILE_NAME}`
* Use the path and bitstream information to create a `FileObject`

#### Handling deleted messages
* Try to get an object with the id from the message. 
* If the object doesn't exist, this will probably be an id from a CO or an SO so log and continue.
* If the object does exist:
  * Get all the paths to the files that sit underneath this object
  * Delete all the paths

### Example of the OCFL Structure

```
<IO_Ref>
├── IO_Metadata.xml
└── <Representation_Type>
    ├── <CO_Ref>
    │   ├── derived
    │   │   ├── g2
    │   │   │   └── 58154e7d-6271-488d-bf78-989d937580d5.pdf
    │   │   └── g3
    │   │       └── 58154e7d-6271-488d-bf78-989d937580d5.pdf
    │   ├── CO_Metadata.xml
    │   └── original
    │       └── g1
    │           └── 58154e7d-6271-488d-bf78-989d937580d5.docx
    └── <CO_Ref>
        ├── CO_Metadata.xml
        └── original
            └── g1
                └── c0c767b7-0eaf-41cc-b941-cabd60e50532.json
```

#### Looking up, Creating/Updating files and creating SNS messages

* Once the list of all `MetadataObject`s and `FileObject`s have been generated
* Check the OCFL repository for an object stored under this IO id.
    * If an object is not found, that means no files belong under this IO and therefore, add the metadata object to the
      list of "missing" objects
    * If an object is found:
        * Get the file, using the destination path
            * If the file is missing, add metadata object to the list of "missing" files
            * If the file is found
                * Compare the calculated checksum with the one in the OCFL repository.
                    * If they are the same, do nothing.
                    * If they are different, add the metadata object to the list of "changed" files
* If a MetadataObject (IO) is missing, then it, as well as the COs that belong to it, need to be downloaded so that we
  can be sure that we have at least one version of the IO (and its COs) saved.
    * In order to do this, the steps from
      the [CO Messages](#handling-non-deleted-content-object-co-messages-if-entity-has-not-been-deleted),
      starting from the "getting the URLs of the representations" step, are followed
* Once the list of all "missing" and "changed" files are generated, for the ones that are bitstreams, stream them from
  Preservica
  to a tmp directory or if it's a metadata update, convert the XML to a String and save it to a tmp directory
    * For "missing" files:
        * Call `createObjects` on the OCFL repository in order to:
            * insert a new object into the `destinationPath` provided
            * add a new version to the OCFL repository
    * For "changed" files:
        * Call `createObjects` on the OCFL repository in order to:
            * overwrite the current file stored at the `destinationPath` provided
            * add a new version to the OCFL repository
* Once these files are added to the OCFL repository, they can be deleted from the `work` directory in order to reduce
  space
    * Even though files get deleted when the container restarts, there is a possibility that the container is active for
      a long time
* Finally, generate a list of SNS messages with information on this update; more information directly below

#### Sending Status messages to SNS

Once the process completes successfully, a message per OCFL update is sent to SNS with this information, the:

* entity type
* `ioRef`
* `ObjectStatus`: `Created`, `Updated`
* `ObjectType`: `Bitstream`, `Metadata`, `MetadataAndPotentialBitstreams`
* `tableItemIdentifier` - the reference that can be used to find it in the files table

#### Deleting Received SQS messages

If the Custodial Copy process completes successfully, the messages that were received from SQS are then deleted from the
SQS queue.

#### Parallel processing

Each message is processed in parallel, except for writing to the OCFL repository.
The OCFL library will throw an exception if you try to write to the same object at the same time so there is a single
semaphore to prevent two fibers writing at the same time.
All other processes such as fetching the data from Preservica and deleting the SQS messages are processed in parallel.
All non-deleted messages are processed (in parallel first) and then the deleted messages to prevent unwanted behaviour,
like a
deletion of an Information Object and then a CO being created afterward; this scenario could happen if a CO gets
added to Preservica (manually or automatically) and then the IO gets deleted within the time window.

### Infrastructure

This will be hosted on a machine at Kew rather than in the cloud so the only infrastructure resource needed is the
repository to store the Docker image.

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

### Environment Variables

| Name                   | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| PRESERVICA_URL         | The URL of the Preservica server                                            |
| PRESERVICA_SECRET_NAME | The Secrets Manager secret used to store the API credentials                |
| SQS_QUEUE_URL          | The queue the service will poll                                             |
| REPO_DIR               | The directory for the OCFL repository                                       |
| WORK_DIR               | The directory for the OCFL work directory                                   |
| DOWNLOAD_DIR           | The directory to use for downloading files                                  |
| HTTPS_PROXY            | An optional proxy. This is needed running in TNA's network but not locally. |

## 2. Frontend database builder

This is a service which listens to an SQS queue.
This queue receives a message whenever the main custodial-copy process adds or updates an object in the OCFL
repository.
Given the IO id, the builder service looks up the metadata from the metadata files in the OCFL repo and stores it in a
sqlite database.

### Environment Variables

| Name          | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| QUEUE_URL     | The URL of the input queue                                                  |
| DATABASE_PATH | The path to the sqlite database                                             |
| SQS_QUEUE_URL | The queue the service will poll                                             |
| OCFL_REPO_DIR | The directory for the OCFL repository                                       |
| OCFL_WORK_DIR | The directory for the OCFL work directory                                   |
| HTTPS_PROXY   | An optional proxy. This is needed running in TNA's network but not locally. |

### Running locally.

You will need to create a sqlite3 database and run the following to create the files table:

```sql
create table files
(
    version        int,
    id             text,
    name           text,
    fileId         text,
    zref           text,
    path           text,
    fileName       text,
    ingestDateTime datetime,
    sourceId       text,
    citation       text,
    consignmentRef text
);
```

This can be run in Intellij by running the `uk.gov.nationalarchives.builder.Main` class and providing values for each of
the environment variables.

It can also be run using `sbt run`

## 3. Frontend

This is a webapp which allows a user to search for a file within the sqlite database.
If a file is found, the webapp allows the user to download that file by reading it directly from the OCFL repo

### Environment Variables

| Name          | Description                     |
|---------------|---------------------------------|
| DATABASE_PATH | The path to the sqlite database |

### Running locally.

The sqlite database must exist along with the file table.

This can be run in Intellij by running the `uk.gov.nationalarchives.webapp.Main` class and providing values for the
database path environment variable.

It can also be run using `sbt run`

## 4. Database re-indexer

This is built as a docker image but is intended to be run periodically. The program takes a subcommand and three
mandatory arguments.

```bash
reindex --file-type CO --column-name asdasd --xpath //Generation//EffectiveDate
```

The program runs through these steps:

* Select a distinct list of ids from the database.
* For each ID, get the object from OCFL and find either the `IO_Metadata.xml` or all `CO_Metadata.xml` files.
* Run the XPath against the metadata files and get the result. Get the ID from `<Ref>` field in the metadata XML.
* For an `IO` update, write the value to the column name with a given `id`. For a CO, do this with the `fileId`

### Arguments

#### File type

This can either be IO or CO and tells the reindexer whether the value for the database column we're updating is in
`IO_Metadata.xml` or `CO_Metadata.xml`

#### Column name

The column in the database to write the value to

#### XPath

An XPath that will return a single value which will be written to the database column.
The behaviour if the XPath expression returns more than one value is undefined.

### Environment Variables

| Name          | Description                               |
|---------------|-------------------------------------------|
| DATABASE_PATH | The path to the sqlite database           |
| OCFL_REPO_DIR | The directory for the OCFL repository     |
| OCFL_WORK_DIR | The directory for the OCFL work directory |

### Running locally.

You will need to create a sqlite3 database and run the following to create the files table:

```sql
create table files
(
    version        int,
    id             text,
    name           text,
    fileId         text,
    zref           text,
    path           text,
    fileName       text,
    ingestDateTime datetime,
    sourceId       text,
    citation       text,
    consignmentRef text
);
```

This can be run in Intellij by running the `uk.gov.nationalarchives.reindexer.Main` class and providing values for each
of
the environment variables. You will need to provide the arguments listed above as well.

It can also be run using `sbt run`

## 5. OCFL Confirmer

This is a service which is intended to run in a long-running Docker container.

Every 10 seconds, it polls the queue specified in the `SQS_QUEUE_URL` environment variable.
For each invocation, it will try to fetch messages from the queue until there are either no more messages, or there are
50 messages.

### Messages

The queue sends messages in this format:

```json
{
  "ioRef": "1b9555dd-43b7-4681-9b0d-85ebe951ca02",
  "batchId": "TDR-ABC-123_0"
}
```

For each message, the Confirmer checks to see if this object exists in the OCFL repository. 

If it does exist, it writes `true` to an attribute specified by `DYNAMO_ATTRIBUTE_NAME` in the table specified by `DYNAMO_TABLE_NAME`
It then deletes the message from the queue. 

If the object is not in the queue, nothing happens. The message will eventually be resent after the visibility timeout has expired.

### Environment Variables

| Name                  | Description                                                                |
|-----------------------|----------------------------------------------------------------------------|
| DYNAMO_TABLE_NAME     | The DynamoDB table to update                                               |
| DYNAMO_ATTRIBUTE_NAME | The attribute to update in the DynamoDB table                              |
| SQS_QUEUE_URL         | The queue the service will poll                                            |
| OCFL_REPO_DIR         | The directory for the OCFL repository                                      |
| OCFL_WORK_DIR         | The directory for the OCFL work directory                                  |
| HTTPS_PROXY           | An optional proxy. This is needed running in TNA's network but not locally |

## 6. Custodial Copy Reconciler

This is a service that retrieves all Content Object (CO) refs (ids), their checksums and parent Information Object (IO) refs that are 
currently in Preservica. Using each CO's parent (IO) ref, it then gets the corresponding CO refs
and checksum in OCFL. Once we have both lists, it writes each of them to a Preservica CO and an OCFL CO table respectively; it
then compares the checksums and sends a message to Slack if there are any mismatches.

The reason is that there are events such as failures or deletions (intentional or unintentional) that could cause the
two storage mediums to become out of sync. We are only concerned with original non-"Access" COs.

### The process

1. Stream every entity from Preservica via paginated calls to the `updated-since` endpoint.
2. It will filter out anything that is not a CO ref.
3. Splits the remaining object refs into Chunks:
4. Run this process for each Chunk:
   1. get the bitstream info from Preservica
   2. retrieve the IO ref and sha256 checksum
   3. Return these `CoRow`s
5. There is a parallel process which streams all OCFL objects from the repository and writes them in chunks to the database. 
6. Once both processes have completed and all rows have been written to the database, the stream can be drained (in order to discard anything returned)
7. Find the missing COs in each table
   1. first parse the `PreservicaCOs` table and check if the checksum(s) appear in the `OcflCos` table
      1. if not, for each missing CO
         1. generate an informative message
         2. log that message
         3. return the message
   2. next parse the `OcflCos` table and check if the checksum(s) appear in the `PreservicaCOs` table
      1. if not, for each missing CO
          1. generate an informative message
          2. log that message
          3. return the message
   3. return both sets of messages concatenated
8. If there are messages, send them to Slack via EventBridge
   1. if the number of messages is 10 or fewer, then send the messages one by one to EventBridge
   2. if the number of messages is greater than 10, then send a general message to EventBridge informing clients that
      there are more than 10 messages and to check the logs for more details

| Name                   | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| PRESERVICA_SECRET_NAME | The Secrets Manager secret used to store the API credentials                |
| DATABASE_PATH          | The path to the sqlite database                                             |
| MAX_CONCURRENCY        | The maximum number of chunks to process concurrently                        |
| OCFL_REPO_DIR          | The directory for the OCFL repository                                       |
| OCFL_WORK_DIR          | The directory for the OCFL work directory                                   |
| HTTPS_PROXY            | An optional proxy. This is needed running in TNA's network but not locally. |