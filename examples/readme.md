### examples usage

test server:
```
go build test_server
./test_server -projectID=foo -topic=barT0pic
 ```

basic example:
```
go build basic
GOOGLE_APPLICATION_CREDENTIALS=sa.json ./basic -topicPath=projects/mb-internal-srv-tst/topics/pubsub-tester
 ```