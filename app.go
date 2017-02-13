/*
Class:		CMPE 273-01
Assignment:	assignment3
Name:		Joji Kubota
Email:		joji.kubota@sjsu.edu
SID:		010404602
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jasonlvhit/gocron"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"net/http"
	"os"
	// "reflect"
	"strconv"
	// "strings"
	"time"
)

// Global variables
var numRetries float64
var maxRetries float64

// Helper to generate current timestamp
func makeTimestamp() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

// Helper to check error
func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Helper to convert json input to bson
func createBsonfromJson(jsonFile string, bsonFile string) {
	// Read in the input json file.
	jsonData, err := ioutil.ReadFile(jsonFile)
	check(err)

	// Unmarsh json to map[]
	var newRequest map[string]interface{}
	json.Unmarshal(jsonData, &newRequest)

	// Marshal map[] to bson
	bsonData, err := bson.Marshal(&newRequest)
	check(err)
	fmt.Printf("%q", bsonData)

	// Create bson file
	file, err := os.Create(bsonFile)
	check(err)
	defer file.Close()

	// Write to bson file
	numBytes, err := file.Write(bsonData)
	fmt.Printf("\nwrote %d bytes to %s\n", numBytes, bsonFile)
	file.Sync()
}

// Helper to read in bson input file
func readBsonInput(bsonFile string) map[string]interface{} {
	// Read in the input bson file.
	input, err := ioutil.ReadFile(bsonFile)
	check(err)

	// Store data in a map
	inputData := make(map[string]interface{})
	bson.Unmarshal(input, &inputData)

	// fmt.Println(inputData)

	return inputData
}

// Helper to make http call
func callHttp(inputData map[string]interface{}) map[string]interface{} {

	// Container for the http request
	httpRequest := make(map[string]interface{})
	httpRequest = inputData["request"].(map[string]interface{})

	// Parse out the info to make the http call
	url := httpRequest["url"].(string)
	method := httpRequest["method"].(string)
	headers := httpRequest["http_headers"].(map[string]interface{})

	// Some setup for the body parsing
	httpBody := httpRequest["body"].(map[string]interface{})
	bodyJson, err := json.Marshal(httpBody)
	check(err)
	bodyStr := []byte(string(bodyJson))

	// Create new http request
	req, err := http.NewRequest(method, url, bytes.NewBuffer(bodyStr))
	check(err)

	// Set headers
	for key, value := range headers {
		req.Header.Set(key, value.(string))
	}

	// Make http call
	client := &http.Client{}
	resp, err := client.Do(req)
	check(err)
	defer resp.Body.Close()

	// Container for the http resposne
	httpResponse := make(map[string]interface{})

	httpResponse["http_response_code"] = float64(resp.StatusCode)
	httpResponse["http_headers"] = resp.Header
	httpResponse["body"] = resp.Body

	return httpResponse

	/*
		// Parse out the info to make the http call
		var urlToCall string
		var httpMethod string
		var httpHeaders map[string]interface{}
		var httpBody map[string]interface{}

		for key, value := range inputData {
			if key == "request" {
				httpRequest := make(map[string]interface{})
				httpRequest = value.(map[string]interface{})
				for k, v := range httpRequest {
					switch k {
					case "url":
						urlToCall = v.(string)
					case "method":
						httpMethod = v.(string)
					case "http_headers":
						httpHeaders = v.(map[string]interface{})
					case "body":
						httpBody = v.(map[string]interface{})
					}
				}
			}
		}

		// Setup for http body
		bodyJson, err := json.Marshal(httpBody)
		check(err)
		bodyStr := []byte(string(bodyJson))

		// Create new http request
		req, err := http.NewRequest(
			httpMethod, urlToCall, bytes.NewBuffer(bodyStr))
		check(err)

		// Set headers
		for key, value := range httpHeaders {
			req.Header.Set(key, value.(string))
		}

		// Make http call
		client := &http.Client{}
		resp, err := client.Do(req)
		check(err)
		defer resp.Body.Close()

		// Some prep work to parse response Header
		tempHeaderMap := make(map[string]string)
		for key, value := range resp.Header {
			var tempValue string
			for i := 0; i < len(value)-1; i++ {
				tempValue += value[i] + " "
			}
			tempValue += value[len(value)-1]
			tempHeaderMap[key] = tempValue
		}

		// Save the response
		var httpResponseJson string
		httpResponseJson += `{"response": {`
		httpResponseJson += `"http_response_code": `
		httpResponseJson += strconv.Itoa(resp.StatusCode) + `,`
		httpResponseJson += `"http_headers": {`
		for key, value := range tempHeaderMap {
			httpResponseJson += `"` + key + `": `
			httpResponseJson += `"` + value + `",`

			// underlyingValue := reflect.TypeOf(value)
			// fmt.Println(underlyingValue)
			// typeAssertedValue, ok := value.(underlyingValue)
			// if !ok {
			// 	fmt.Println("Type assertion failed")
			// } else {
			// 	httpResponseJson += `"` + key + `": `
			// 	if typeAssertedValue == "string" {
			// 		httpResponseJson += `"` + typeAssertedValue + `",`
			// 	} else {
			// 		httpResponseJson += strconv.Itoa(typeAssertedValue) + `,`
			// 	}
			// }
		}
		httpResponseJson = strings.TrimSuffix(httpResponseJson, ",")
		httpResponseJson += `},`
		httpResponseJson += `"body": {`
		body, err := ioutil.ReadAll(resp.Body)
		check(err)
		trimmedBody := strings.TrimSpace(string(body))
		if string(trimmedBody) == "Not found" {
			httpResponseJson += `"error":`
			httpResponseJson += `"`
			httpResponseJson += string(trimmedBody)
			httpResponseJson += `"`
		} else {
			httpResponseJson += string(trimmedBody)
		}
		httpResponseJson += `}}}`

		// fmt.Println(httpResponseJson)

		// Stroe the response data in a map
		var httpResponse map[string]interface{}
		if err := json.Unmarshal([]byte(httpResponseJson), &httpResponse); err != nil {
			check(err)
		}

		// fmt.Println(httpResponse)

		return httpResponse

	*/
}

// Helper to update job status
func updateJobStatus(inputData map[string]interface{},
	httpResponse map[string]interface{}) map[string]interface{} {
	// Check what the successful http response code is.
	jobCompleted := checkHttpStatus(inputData, httpResponse)

	// Set the job status
	jobStatus := make(map[string]interface{})
	if jobCompleted {
		jobStatus["status"] = "COMPLETED"
	} else if !jobCompleted && numRetries < maxRetries {
		jobStatus["status"] = "STILL_TRYING"
	} else {
		jobStatus["status"] = "FAILED"
	}

	jobStatus["num_retries"] = numRetries

	return jobStatus
}

// Helper to check http status
func checkHttpStatus(inputData map[string]interface{},
	httpResponse map[string]interface{}) bool {
	// Compare the status code between the input and response
	successfulResponseCode := inputData["success_http_response_code"]
	returnedResponseCode := httpResponse["http_response_code"]

	// fmt.Println(reflect.TypeOf(successfulResponseCode))
	// fmt.Println(reflect.TypeOf(returnedResponseCode))

	return successfulResponseCode == returnedResponseCode
}

// Helper to call webhook
func callWebhook(inputData map[string]interface{},
	httpResponse map[string]interface{}) map[string]interface{} {

	webhookResponse := make(map[string]interface{})
	// Get webhook url
	webhookUrl := inputData["callback_webhook_url"].(string)

	// Prepare POST body
	postBody, err := json.Marshal(httpResponse["body"])
	check(err)

	// Create new http request
	req, err := http.NewRequest("POST", webhookUrl, bytes.NewBuffer(postBody))
	check(err)

	// Make http call
	client := &http.Client{}
	resp, err := client.Do(req)
	check(err)
	defer resp.Body.Close()

	// Save the response
	webhookResponse["callback_response_code"] = resp.StatusCode

	return webhookResponse
}

func writeBsonOutput(inputData map[string]interface{}, httpResponse map[string]interface{},
	jobStatus map[string]interface{}, webhookResponse map[string]interface{},
	bsonFile string) map[string]interface{} {

	outputData := make(map[string]interface{})

	// Combine the data
	outputData["job"] = jobStatus
	outputData["input"] = inputData
	outputData["output"] = httpResponse
	outputData["callback_response_code"] = webhookResponse["callback_response_code"]

	outputBson, err := bson.Marshal(&outputData)
	check(err)
	// fmt.Printf("%q", outputBson)

	// Create bson file
	outputFile, err := os.Create(bsonFile)
	check(err)
	defer outputFile.Close()

	// Write to bson file
	numBytes, err := outputFile.Write(outputBson)
	fmt.Printf("\nwrote %d bytes to %s\n", numBytes, bsonFile)
	outputFile.Sync()

	return outputData
}

// Read input and generate output
func task() {
	// Generate task id
	t := makeTimestamp()
	fmt.Printf("Running task...@%d\n", t)

	// Generate input.bson from input.json (initial setup only)
	if _, err := os.Stat("input.bson"); os.IsNotExist(err) {
		fmt.Println("Creating Bson from Json")
		createBsonfromJson("input.json", "input.bson")
	}

	// Read in the requests
	inputData := make(map[string]interface{})
	inputData = readBsonInput("input.bson")

	// Initialize the retry counter
	numRetries = 0
	maxRetries = inputData["max_retries"].(float64)

	// Invoke a http call
	httpResponse := make(map[string]interface{})
	httpResponse = callHttp(inputData)

	// Update job status
	jobStatus := make(map[string]interface{})
	jobStatus = updateJobStatus(inputData, httpResponse)

	// Call webhook url on successful call
	webhookResponse := make(map[string]interface{})
	if jobStatus["status"] == "COMPLETED" {
		webhookResponse = callWebhook(inputData, httpResponse)
		// fmt.Println(webhookResponse)
	}

	// Store transaction details in output.bson
	outputData := make(map[string]interface{})
	outputData = writeBsonOutput(inputData, httpResponse, jobStatus,
		webhookResponse, "output.bson")
	fmt.Println(outputData)

	// Repeat the job until FAILED or COMPLETED
	for jobStatus["status"] == "STILL_TRYING" &&
		numRetries < maxRetries {

		numRetries++
		httpResponse = callHttp(inputData)
		jobStatus = updateJobStatus(inputData, httpResponse)
		outputData = writeBsonOutput(inputData, httpResponse, jobStatus,
			webhookResponse, "output.bson")
		fmt.Println(outputData)
	}

	// Call webhook url when status reaches FAILED or COMPLETED
	webhookResponse = callWebhook(inputData, httpResponse)
	outputData = writeBsonOutput(inputData, httpResponse, jobStatus,
		webhookResponse, "output.bson")
	fmt.Println(outputData)

}

// Main function
func main() {
	// Read in the command line args
	timeInt, _ := strconv.Atoi(os.Args[1])
	timeUint64 := uint64(timeInt)

	// Start the loop
	s := gocron.NewScheduler()
	s.Every(timeUint64).Seconds().Do(task)
	<-s.Start()
}
