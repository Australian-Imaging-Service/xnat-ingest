Deployment
==========

Rather than a one-off run, most nodes want ``group``/``assign``/``deidentify``/
``upload`` running continuously, each watching its input directory and picking up new
files as they arrive. Every stage except ``check-upload`` supports this directly via
``--loop <seconds>``: instead of running once and exiting, the command re-runs itself
every ``<seconds>`` seconds, forever, in a single process.

Errors encountered while processing an individual session (a bad file, a missing
spec, a dropped connection) are logged and skipped rather than crashing the process —
in fact ``--raise-errors`` and ``--loop`` can't be combined, since raising would
defeat the point of looping forever. This means each stage is designed to be started
once and left running, rather than needing a process supervisor to restart it after
every transient failure (though a restart policy is still good practice as a
backstop — see below).

A published Docker image (``ghcr.io/australian-imaging-service/xnat-ingest``) wraps
the CLI as its entrypoint, so each stage becomes a single long-running container:

.. code-block:: console

    $ docker run -d \
        -v /data/incoming:/input \
        -v /data/staging:/staging \
        ghcr.io/australian-imaging-service/xnat-ingest \
        group /input /staging/grouped --loop 300

Repeat with ``assign``, optionally ``deidentify``, and ``upload`` as separate
containers, each mounting the previous stage's output as its input, chained through a
shared staging area on disk (or an S3 bucket, which ``upload``/``check-upload`` can
read from directly).

Docker Compose
--------------

A ``docker-compose.yml`` can express the whole chain as one stack, with each stage as
a service sharing a named volume for the staging directories:

.. code-block:: yaml

    services:
      group:
        image: ghcr.io/australian-imaging-service/xnat-ingest
        command: group /input /staging/grouped --loop 300
        volumes:
          - /data/incoming:/input
          - staging:/staging
        restart: unless-stopped

      assign:
        image: ghcr.io/australian-imaging-service/xnat-ingest
        command: assign /staging/grouped /staging/assigned --loop 300
        volumes:
          - staging:/staging
        restart: unless-stopped

      upload:
        image: ghcr.io/australian-imaging-service/xnat-ingest
        command: upload /staging/assigned xnat.example.org --always-include all --loop 300
        environment:
          XINGEST_USER: my-upload-user
          XINGEST_PASS: my-upload-password
        volumes:
          - staging:/staging
        restart: unless-stopped

    volumes:
      staging:

Kubernetes
----------

The same shape maps onto a Kubernetes ``Deployment`` per stage (one replica each,
``restartPolicy: Always``), with the staging directories on a shared
``PersistentVolumeClaim`` mounted into each Pod, and connection details (XNAT host,
user, password) injected via a ``ConfigMap``/``Secret`` as ``XINGEST_*`` environment
variables rather than passed as command-line flags. ``check-upload`` doesn't support
``--loop``, so it's a natural fit for a ``CronJob`` instead, run periodically to audit
what has and hasn't made it to XNAT rather than as an always-on service.
