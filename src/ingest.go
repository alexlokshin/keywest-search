package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

// Maps products to subcategories
type HierarchyLocator struct {
	Name             string
	Products         KeyWestStringSet
	HierarchyLocator map[string]HierarchyLocator
}

// Maps phrase to a list of products that contain it in this field
type FieldIndex struct {
	HierarchyLocator map[string]HierarchyLocator
}

type Record map[string]interface{}

var schema = make(map[string]string)
var records = make(map[string]Record)
var indices = make(map[string]FieldIndex)

var mutex sync.Mutex

func ingest(c *gin.Context) {
	req := make(map[string]interface{})

	buf := make([]byte, 1024)
	num, _ := c.Request.Body.Read(buf)
	reqBody := string(buf[0:num])

	if err := json.Unmarshal([]byte(reqBody), &req); err != nil {
		c.JSON(500, gin.H{
			"Status": "Error",
			"Error":  fmt.Sprintf("%v", err),
		})
		return
	}

	recordId := recordIngest(req)

	c.JSON(200, gin.H{
		"Status": "OK",
		"_id":    recordId,
	})
}

func batchIngest(c *gin.Context) {
	body := c.Request.Body
	x, _ := ioutil.ReadAll(body)

	request := string(x)
	items := strings.Split(request, "\n")

	recordIds := make([]string, 0)
	errors := make([]string, 0)

	for _, item := range items {
		if len(strings.TrimSpace(item)) == 0 {
			continue
		}
		req := make(map[string]interface{})
		err := json.Unmarshal([]byte(item), &req)

		if err == nil {
			recordId := recordIngest(req)
			recordIds = append(recordIds, recordId)
		} else {
			errors = append(errors, err.Error())
		}
	}

	c.JSON(200, gin.H{
		"Status": "OK",
		"_id":    recordIds,
		"Errors": errors,
	})
}

func update(c *gin.Context) {
	req := make(map[string]interface{})

	buf := make([]byte, 1024)
	num, _ := c.Request.Body.Read(buf)
	reqBody := string(buf[0:num])

	if err := json.Unmarshal([]byte(reqBody), &req); err != nil {
		c.JSON(500, gin.H{
			"Status": "Error",
			"Error":  fmt.Sprintf("%v", err),
		})
		return
	}

	recordId := recordUpdate(req)

	c.JSON(200, gin.H{
		"Status": "OK",
		"_id":    recordId,
	})
}

func getSchema(c *gin.Context) {
	c.JSON(200, schema)
}

func getStats(c *gin.Context) {
	c.JSON(200, gin.H{
		"Status":      "OK",
		"RecordCount": len(records),
		"Schema":      schema,
	})
}

func clearIndex(c *gin.Context) {
	schema = make(map[string]string)
	records = make(map[string]Record)
	indices = make(map[string]FieldIndex)
	c.JSON(200, gin.H{
		"Status": "Cleared",
	})
}

func deleteRecord(c *gin.Context) {
	id := c.Param("id")
	deleteProduct(id)
	c.JSON(200, gin.H{
		"Status": "Removed",
	})
}

func deleteProduct(productId string) {
	if len(productId) > 0 {
		delete(records, productId)
		mutex.Lock()
		for _, v := range indices {
			removeProduct(v.HierarchyLocator, productId)
		}
		mutex.Unlock()
	}
}

func removeProduct(hierarchyLocator map[string]HierarchyLocator, id string) {
	emptyLocatorKeys := make([]string, 0)
	for key, locator := range hierarchyLocator {
		locator.Products.Remove(id)
		if locator.HierarchyLocator != nil {
			removeProduct(locator.HierarchyLocator, id)
		}
		if len(locator.Products) == 0 {
			emptyLocatorKeys = append(emptyLocatorKeys, key)
		}
	}
	for _, key := range emptyLocatorKeys {
		delete(hierarchyLocator, key)
	}
}

func analyzeSchema(req map[string]interface{}) {
	for k, v := range req {
		fieldName := strings.ToLower(k)
		typeString := getType(v)
		if len(typeString) > 0 {
			if "array" == typeString {
				elementTypes := make(map[string]int)
				lastElementType := ""
				for _, a := range v.([]interface{}) {
					lastElementType = getType(a)

					mutex.Lock()
					elementTypes[lastElementType] = elementTypes[lastElementType] + 1
					mutex.Unlock()
				}
				if len(elementTypes) == 1 {
					properType := fmt.Sprintf("%s[%s]", typeString, lastElementType)
					if len(schema[fieldName]) > 0 && properType != schema[fieldName] {
						fmt.Printf("Field %s is not consistent.\n", k)
					} else {
						mutex.Lock()
						schema[fieldName] = properType
						mutex.Unlock()
					}
				} else {
					if len(schema[fieldName]) > 0 && "array" != schema[fieldName] {
						fmt.Printf("Fidelity of field %s has decreased.\n", k)
					}
				}
			} else {
				if len(schema[fieldName]) > 0 && schema[fieldName] != typeString {
					if "float64" == schema[fieldName] && "int" == typeString {
						// Ignore this
					} else {
						fmt.Printf("Field %s is not type consistent.\n", k)
					}
				} else {
					mutex.Lock()
					schema[fieldName] = typeString
					mutex.Unlock()
				}
			}
		}
	}
}

func getType(v interface{}) string {
	r := ""

	switch t := v.(type) {
	case int:
		r = "int"
	case float64:
		r = "float64"
		floatValue := v.(float64)
		if floatValue == math.Trunc(floatValue) {
			r = "int"
		}
	case string:
		r = "string"
	case bool:
		r = "bool"
	case []interface{}:
		r = "array"
	case map[string]interface{}:
		r = "obj"
	case nil:
		r = ""
	default:
		var i = reflect.TypeOf(t)
		r = fmt.Sprintf("%v", i)
	}

	return r
}

func recordIngest(req map[string]interface{}) string {
	// Analyze schema
	analyzeSchema(req)

	recordId := ""
	if req["_id"] != nil {
		recordId = req["_id"].(string)
	}

	if len(recordId) > 0 {
		// Capture the original document
		mutex.Lock()
		records[recordId] = req
		mutex.Unlock()
		// Break the document down into searchable bits

		for k, v := range req {
			if "_weight" == k {
				continue
			}
			fieldName := strings.ToLower(k)
			typeString := schema[fieldName]

			if "bool" == typeString {
				value := strconv.FormatBool(v.(bool))
				ingestGeneric(fieldName, value, recordId)
			}

			if "float64" == typeString {
				value := fmt.Sprintf("%.2f", v.(float64))
				ingestGeneric(fieldName, value, recordId)
			}

			if "int" == typeString {
				value := fmt.Sprintf("%d", int(v.(float64)))
				ingestGeneric(fieldName, value, recordId)
			}

			if "string" == typeString {
				value := strings.ToLower(v.(string))
				ingestText(value, fieldName, recordId)
			}

			if "array[string]" == typeString {
				intArray := v.([]interface{})
				value := make([]string, 0)
				for _, val := range intArray {
					value = append(value, val.(string))
				}
				ingestStringArray(fieldName, value, recordId)
			}
		}
	}
	return recordId
}

func recordUpdate(req map[string]interface{}) string {
	analyzeSchema(req)

	recordId := ""
	if req["_id"] != nil {
		recordId = req["_id"].(string)
	}

	if len(recordId) > 0 {
		rec, ok := records[recordId]
		if ok {
			for k, v := range req {
				rec[k] = v
			}

			deleteProduct(recordId)
			recordIngest(rec)
		}
	}
	return recordId
}

func ingestStringArray(fieldName string, value []string, recordId string) {
	if len(strings.TrimSpace(fieldName)) == 0 {
		fmt.Printf("Empty field name\n")
	}
	if _, ok := indices[fieldName]; !ok {
		mutex.Lock()
		indices[fieldName] = FieldIndex{
			HierarchyLocator: make(map[string]HierarchyLocator),
		}
		mutex.Unlock()
	}
	index := indices[fieldName].HierarchyLocator
	for _, iitem := range value {
		item := strings.TrimSpace(strings.ToLower(iitem))
		subIndex, ok := index[item]
		if !ok {
			if len(strings.TrimSpace(item)) == 0 {
				fmt.Printf("Empty field name\n")
			}
			mutex.Lock()
			subIndex = HierarchyLocator{Name: item, Products: KeyWestStringSet{}, HierarchyLocator: make(map[string]HierarchyLocator)}
			index[item] = subIndex
			mutex.Unlock()
		}
		mutex.Lock()
		subIndex.Products.Add(recordId)
		mutex.Unlock()
		index = subIndex.HierarchyLocator
	}
}

func ingestText(value string, fieldName string, recordId string) {
	ngrams := GetNGrams(value, 1, 5)
	if len(fieldName) == 0 {
		fmt.Printf("Empty field name\n")
	}
	for ngram, _ := range ngrams.Weights {

		if _, ok := indices[fieldName]; !ok {
			mutex.Lock()
			indices[fieldName] = FieldIndex{
				HierarchyLocator: make(map[string]HierarchyLocator),
			}
			mutex.Unlock()
		}

		index := indices[fieldName].HierarchyLocator
		if index == nil {
			mutex.Lock()
			indices[fieldName] = FieldIndex{
				HierarchyLocator: make(map[string]HierarchyLocator),
			}
			mutex.Unlock()
		}

		if _, ok := indices["*"]; !ok {
			mutex.Lock()
			indices["*"] = FieldIndex{
				HierarchyLocator: make(map[string]HierarchyLocator),
			}
			mutex.Unlock()
		}

		subIndex, ok := index[ngram]
		if !ok {
			mutex.Lock()
			subIndex = HierarchyLocator{Name: ngram, Products: KeyWestStringSet{}, HierarchyLocator: make(map[string]HierarchyLocator)}
			index[ngram] = subIndex
			mutex.Unlock()
		}
		mutex.Lock()
		subIndex.Products.Add(recordId)
		mutex.Unlock()

		index = indices["*"].HierarchyLocator

		subIndex, ok = index[ngram]
		if !ok {
			mutex.Lock()
			subIndex = HierarchyLocator{Name: ngram, Products: KeyWestStringSet{}, HierarchyLocator: make(map[string]HierarchyLocator)}
			index[ngram] = subIndex
			mutex.Unlock()
		}
		mutex.Lock()
		subIndex.Products.Add(recordId)
		mutex.Unlock()
	}
}

func ingestGeneric(fieldName string, value string, recordId string) {
	if _, ok := indices[fieldName]; !ok {
		mutex.Lock()
		indices[fieldName] = FieldIndex{
			HierarchyLocator: make(map[string]HierarchyLocator),
		}
		mutex.Unlock()
	}
	index := indices[fieldName].HierarchyLocator
	if index == nil {
		mutex.Lock()
		indices[fieldName] = FieldIndex{
			HierarchyLocator: make(map[string]HierarchyLocator),
		}
		mutex.Unlock()
	}
	subIndex, ok := index[value]
	if !ok {
		mutex.Lock()
		subIndex = HierarchyLocator{Name: value, Products: KeyWestStringSet{}, HierarchyLocator: make(map[string]HierarchyLocator)}
		index[value] = subIndex
		mutex.Unlock()
	}
	mutex.Lock()
	subIndex.Products.Add(recordId)
	mutex.Unlock()
}
