package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var storageLocation = "."

func main() {
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})

	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("main() Recovered from: %v\n", err)
		}
	}()

	if len(os.Getenv("STORAGE_LOCATION")) > 0 {
		storageLocation = strings.TrimSpace(os.Getenv("STORAGE_LOCATION"))
	}
	fmt.Printf("Using storage location: %s\n", storageLocation)

	err := LoadBinary(storageLocation+"/snapshot.dat", &records)
	if err != nil {
		fmt.Printf("Error loading snapshot: %v\n", err)
	}
	err = LoadBinary(storageLocation+"/indices.dat", &indices)
	if err != nil {
		fmt.Printf("Error loading indices: %v\n", err)
	}
	err = Load(storageLocation+"/schema.dat", &schema)
	if err != nil {
		fmt.Printf("Error loading schema: %v\n", err)
	}

	r := createRouter()
	registerRoutes(r)

	fmt.Printf("Starting ${CICD_GIT_REPO_NAME} on 0.0.0.0:5000\n")
	r.Run("0.0.0.0:5000")
}

func cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c != nil && c.Request != nil && "OPTIONS" == c.Request.Method {
			// Added this to debug Anu's OPTIONS requests
			fmt.Printf("\nOPTIONS headers:\n")
			for k, v := range c.Request.Header {
				fmt.Printf("Header: %s\n", k)
				fmt.Printf("Value: ")
				for i, val := range v {
					if i > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("[%s]", val)
				}
				fmt.Printf("\n")
			}
			fmt.Printf("\n")
		}

		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, authorization, content-type, accept, origin, Cache-Control, X-Requested-With, access-control-allow-origin, access-control-allow-credentials, access-control-allow-headers, access-control-allow-methods")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if "OPTIONS" == c.Request.Method {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func createRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors())
	return r
}

func registerRoutes(r *gin.Engine) {
	go func() {
		for true {
			time.Sleep(60 * time.Second)
			fmt.Printf("Saving search index...\n")

			saveIndex()
		}
	}()

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"Status":  "OK",
			"Service": "${CICD_GIT_REPO_NAME}",
			"Version": "[${CICD_GIT_REPO_NAME}]-[${CICD_GIT_BRANCH}]-[${CICD_GIT_COMMIT}]-[${CICD_EXECUTION_SEQUENCE}]",
		})
	})

	r.GET("/api/search/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"Status":  "OK",
			"Service": "${CICD_GIT_REPO_NAME}",
			"Version": "[${CICD_GIT_REPO_NAME}]-[${CICD_GIT_BRANCH}]-[${CICD_GIT_COMMIT}]-[${CICD_EXECUTION_SEQUENCE}]",
		})
	})

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"Status": "OK",
		})
	})

	r.PUT("/api/search/ingest", ingest)

	r.POST("/api/search/batch/ingest", batchIngest)

	r.POST("/api/search/update", update)

	r.GET("/api/search/schema", getSchema)

	r.GET("/api/search/stats", getStats)

	r.GET("/api/search/query", search)

	r.GET("/api/search/clear", clearIndex)

	r.DELETE("/api/search/delete/:id", deleteRecord)

	r.Static("/api/search/swaggerui/", "doc/swagger-ui-dist")
	r.StaticFile("/api/search/swagger.json", "./doc/swagger.json")
}

var saving sync.Mutex

func saveIndex() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("saveIndex() Recovered from: %v\n", err)
		}
	}()

	if _, err := os.Stat(storageLocation + "/tmp"); os.IsNotExist(err) {
		err := os.MkdirAll(storageLocation+"/tmp", 0700)
		if err != nil {
			fmt.Printf("Cannot create folder: %s\n", storageLocation+"/tmp")
		}
	}

	mutex.Lock()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		err := SaveBinary(storageLocation+"/tmp/snapshot.dat", records)
		if err != nil {
			fmt.Printf("Error saving snapshot: %v\n", err)
		}
	}()
	go func() {
		defer wg.Done()
		err := SaveBinary(storageLocation+"/tmp/indices.dat", indices)
		if err != nil {
			fmt.Printf("Error saving indices: %v\n", err)
			os.Remove(storageLocation + "/tmp/indices.dat")
		}

	}()
	go func() {
		defer wg.Done()
		err := Save(storageLocation+"/tmp/schema.dat", schema)
		if err != nil {
			fmt.Printf("Error saving schema: %v\n", err)
			os.Remove(storageLocation + "/tmp/schema.dat")
		}
	}()

	wg.Wait()

	if _, err := os.Stat(storageLocation + "/tmp/snapshot.dat"); !os.IsNotExist(err) {
		if _, err := os.Stat(storageLocation + "/tmp/indices.dat"); !os.IsNotExist(err) {
			if _, err := os.Stat(storageLocation + "/tmp/schema.dat"); !os.IsNotExist(err) {

				err := os.Rename(storageLocation+"/tmp/snapshot.dat", storageLocation+"/snapshot.dat")
				if err != nil {
					fmt.Printf("Cannot move snapshot.dat: %v\n", err)
					os.Remove(storageLocation + "/tmp/snapshot.dat")
				}

				err = os.Rename(storageLocation+"/tmp/indices.dat", storageLocation+"/indices.dat")
				if err != nil {
					fmt.Printf("Cannot move indices.dat: %v\n", err)
				}

				err = os.Rename(storageLocation+"/tmp/schema.dat", storageLocation+"/schema.dat")
				if err != nil {
					fmt.Printf("Cannot move schema.dat: %v\n", err)
				}
			}
		}
	}
	mutex.Unlock()
}
