# DR2 Disaster Recovery

This is a service which is intended to run in a long-running Docker container.

Every 20 seconds, it polls the queue specified in the `SQS_QUEUE_URL` environment variable.
This will be set to the queue which receives messages from the entity event generator.

## Messages

The queue sends messages in this format

```json
{
  "id": "io:1b9555dd-43b7-4681-9b0d-85ebe951ca02"
}
```

The prefix of the id can be `io`, `co` or `so` depending on the entity type; they are dealt with differently.
Messages are deduped before they are processed.

### Handling Information object messages

* Create the metadata file name `IO_Metadata.xml`
* Get the metadata from the Preservica API
* Create the destination path for this metadata file `{IO_REF}/IO_Metadata.xml`
* Wrap all returned metadata fragments in a `<AllMetadata/>` tag
* Calculate the checksum of this metadata string.
* Use all of this information to create a `MetadataObject`

### Handling Content Object (CO) messages

* Get the bitstream information (which contains the parent ID) from the Preservica API.
* Verify that the parent ID is present.
* Use parent ID to get the URLs of the representations
* Use URLs to get the COs under each representation
* Return the representation types (`{representationType}_{index}`) for a given CO ref
* If a CO has more than 1 representation type, throw an Exception
* Create the metadata file name `CO_Metadata.xml`
* Get the metadata from the Preservica API
* Create the destination path for this metadata file `{IO_REF}/{REP_TYPE}/{CO_REF}/{FILE_NAME}`
* Use metadata information to create a `MetadataObject`
* Create the destination path for the CO file `{IO_REF}/{REP_TYPE}/{CO_REF}/{GEN_TYPE}/g{GEN_VERSION}/{FILE_NAME}`
* Use the path and bitstream information to create a `FileObject`

### Handling Structural Object (SO) messages

These are ignored as there is nothing in the structural objects we want to store.

### Looking up, Creating and Updating files
* Once the list of all `MetadataObject`s and `FileObject`s have been generated
* Check the OCFL repository for an object stored under this IO id.
  * If an object is not found that means no files belong under this IO and therefore, add the metadata object to the list of "missing" files
  * If an object is found:
    * Get the file, using the destination path
      * If the file is missing, add metadata object to the list of "missing" files
      * If the file is found
        * Compare the calculated checksum with the one in the OCFL repository.
          * If they are the same, do nothing.
          * If they are different, add the metadata object to the list of "changed" files
* Once the list of all "missing" and "changed" files are generated, stream them from Preservica to a tmp directory
  * For "missing" files:
    * Call `createObjects` on the OCFL repository in order to:
      * insert a new object into the `destinationPath` provided
      * add a new version to the OCFL repository
  * For "changed" files:
    * Call `createObjects` on the OCFL repository in order to:
      * overwrite the current file stored at the `destinationPath` provided
      * add a new version to the OCFL repository

### Deleting SQS messages

If the disaster recovery process completes successfully, the messages are deleted from the SQS queue.
If any messages in a batch fail, all messages are left to try again. This avoids having to work out which ones were
successful which could be error-prone.

## Infrastructure

This will be hosted on a machine at Kew rather than in the cloud so the only infrastructure resource needed is the
repository to store the docker image.

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                   | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| PRESERVICA_URL         | The URL of the Preservica server                                            |
| PRESERVICA_SECRET_NAME | The Secrets Manager secret used to store the API credentials                |
| SQS_QUEUE_URL          | The queue the service will poll                                             |
| REPO_DIR               | The directory for the OCFL repository                                       |
| WORK_DIR               | The directory for the OCFL work directory                                   |
| HTTPS_PROXY            | An optional proxy. This is needed running in TNA's network but not locally. |
