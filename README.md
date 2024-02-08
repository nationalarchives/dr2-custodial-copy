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

The entity type can be `io`, `co` or `so` depending on the entity type. They are dealt with differently.

### Information object messages

* Get the metadata from the Preservica API
* Wrap all returned metadata fragments in a `<AllMetadata/>` tag
* Calculate the checksum of this metadata string.
* Check the OCFL repository for an object stored under this IO id.
    * If an object is not found, insert a new object into the OCFL repository with the metadata stored in a file
      named `tna-dr2-disaster-recovery-metadata.xml`
    * If an object is found:
        * Compare the calculated checksum with the one in the OCFL repository.
        * If they are the same, do nothing.
        * If they are different, add a new version to the OCFL repository.

### Content object messages

* Get the bitstream information from the Preservica API.
* Get the entity, so we can find the IO parent ID.
* Check the OCFL repository for an object stored under this IO id.
    * If an object is not found, insert a new file into the repository under `{ioId}/{bistreamName}`
    * If an object is found:
        * Compare the checksum returned from Preservica with the checksum in the repository.
        * If they are the same, do nothing.
        * If they are different, add a new version to the repository.

### Structural object messages.

These are ignored as there is nothing in the structural objects we want to store.

## Deleting SQS messages

If the disaster recovery process completes successfully, the messages are deleted from the SQS queue.
If any messages in a batch fail, all messages are left to try again. This avoids having to work out which ones were
successful which could be error-prone.

### Infrastructure

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
