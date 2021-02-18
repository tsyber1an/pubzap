### examples usage

test server:
```
cd test_server
go build
./test_server -projectID=foo -topic=barT0pic
 ```

basic example:
```
cd basic
go build
GOOGLE_APPLICATION_CREDENTIALS=sa.json ./basic -topicPath=projects/mb-internal-srv-tst/topics/pubsub-tester
 ```