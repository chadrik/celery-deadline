
## Task Storage Options

### plugin params
- one value per job, so precludes many tasks per job

### aux files
- not sure if we can add more aux_files after submission
- requires a location on disk that both the client and the web service have access to

### extra info
- difficult to avoid clobbering end-user's additional extra info

### topic queues
- requires one queue per job.  too many queues?
- even with one-queue-per-job, a deadline-task will need to gather all celery-tasks from the queue to find the one task to execute
- may be tricky to not ack the message, so that it stays on the queue in case of re-queue

### celery backend
- not all backends are key-value

### deadline mongo database
- requires more configuration
- is there a way to automatically get the deadline url from pulse?


