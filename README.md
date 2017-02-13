## Assignment

In this assignment, you will be building an off-line HTTP scheduler that retrieves and forwards the calls to the destinations until sucess or certain number of retry.  

### Requirement
 
The HTTP scheduler will do the following tasks:

1. Read requests from input.bson.
2. Invoke a Http call to request->url.
3. If success, then make HTTP POST call to the "callback_webhook_url".
4. Store both the request details from input.bson and response details into output.bson.
5. When completed requests from input.bson, revisit all requests with "STILL_TRYING" from the output.bson.
6. Retry those requests and update the response. 
7. When a request reaches to either FAILED or COMPLETED, make HTTP POST call to the "callback_webhook_url".
8. Wait for the next round of the scheduler.
 
### INPUT (input.bson)

- id - current system time in milliseconds.

```json
{
    "id": 1462406556741,
    "success_http_response_code": 200,
    "max_retries": 3,
    "callback_webhook_url": "http://requestb.in/vh61ztvh",
    "request": {
        "url": "http://requestb.in/vh61ztvh",
        "method": "POST",
        "http_headers" : {
            "Content-Type": "application/json",
            "Accept": "application/json"      
        },
        "body" : {
            "foo": "bar"
        }
    }
}
```

### OUTPUT (output.bson)

- job.status - COMPLETED, STILL_TRYING, or FAILED.

```json
{
    "job": {
        "status": "COMPLETED",
        "num_retries": 0
    },
    "input": {
        "id": 1462406556741,
        "success_http_response_code": 200,
        "max_retries": 3,
        "callback_webhook_url": "http://requestb.in/vh61ztvh",
        "request": {
            "url": "http://requestb.in/vh61ztvh",
            "method": "POST",
            "http_headers" : {
                "Content-Type": "application/json",
                "Accept": "application/json"      
            },
            "body" : {
                "foo": "bar"
            }
        }
    },
    "output": {
        "response": {
            "http_response_code": 200,
            "http_headers": {
                "Date" : "Thu, 05 May 2016 00:08:56 GMT",
                "Content-Type" : "application/json; charset=utf-8",
                "Content-Length" : 2  
            },
            "body" : {
            "hello": "world"   
            }
        }
    },
    "callback_response_code": 200
}
```

```sh
# Run the scheduler in every 5 seconds and read input.bson file.
go run app.go 5
```

