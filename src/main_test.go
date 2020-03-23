package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"

	"io"
	"log"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/kljensen/snowball"
)

var r = createRouter()

func TestMain(m *testing.M) {
	r.Use(gin.Logger())
	registerRoutes(r)
	code := m.Run()
	os.Exit(code)
}

func TestStem(t *testing.T) {
	stemmed, err := snowball.Stem("cucumber", "english", true)
	if err == nil {
		fmt.Println(stemmed)
	}
}

func TestGetType(t *testing.T) {
	fmt.Printf("Int? %s\n", getType(1))
	fmt.Printf("Float? %s\n", getType(1.0))
}

func TestSplit(t *testing.T) {
	items := split("Kirkman Senior Essentials 60 to 90 Years Women's Multi-Vitamin & Mineral Boost -- 60 Capsules")

	for _, item := range items {
		fmt.Printf("%v\n", item)
	}

}

func TestVarTree(t *testing.T) {
	varTree := NewVarTreeSet()
	varTree.Add("Hydroponic").Add("Fruit").Add("Red Fruit").Add("Seedy Fruit")
	varTree.Add("Hydroponic").Add("Fruit").Add("Red Fruit").Add("Seedless Fruit")
	refinements, inertPath := varTree.GetInertPath()
	if len(refinements) != 2 {
		t.Errorf("Unexpected number of refinements: %d", len(refinements))
	}
	if inertPath != "Hydroponic>Fruit>Red Fruit" {
		t.Errorf("Unexpected inert path: %s", inertPath)
	}
}

// func TestCeresIngest(t *testing.T) {
// 	file, _ := os.Open("./catalog/ceres/products.json")
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		var product map[string]interface{}
// 		json.Unmarshal([]byte(scanner.Text()), &product)

// 		w := httptesting.PerformRequest(r, httptesting.HttpRequest{Method: "PUT", Path: "/api/search/ingest", Body: product, Description: "Get product details"})

// 		httptesting.AssertResponseStatus(t, w, "OK")
// 	}

// 	time.Sleep(10 * time.Second)
// 	saveIndex()
// }

// func TestIngest(t *testing.T) {
// 	csvFile, _ := os.Open("./catalog/instacart_2017_05_01/products.csv")
// 	reader := csv.NewReader(bufio.NewReader(csvFile))
// 	for {
// 		line, error := reader.Read()
// 		if error == io.EOF {
// 			break
// 		} else if error != nil {
// 			log.Fatal(error)
// 		}
// 		skuId := line[0]
// 		description := line[1]

// 		product := gin.H{
// 			"_id":         skuId,
// 			"description": description,
// 		}

// 		w := httptesting.PerformRequest(r, httptesting.HttpRequest{Method: "PUT", Path: "/api/search/ingest", Body: product, Description: "Get product details"})

// 		httptesting.AssertResponseStatus(t, w, "OK")

// 	}
// 	csvFile.Close()
// }

func TestPunct(t *testing.T) {

	fmt.Printf("Punct: %v\n", split("style�����"))
	fmt.Printf("Punct: %v\n", "�"[0])

}

// func TestNGrams(t *testing.T) {
// 	for i := 1; i < 6; i++ {
// 		fmt.Sprintf("Saving: %d\n", i)
// 		saveWeights(i)
// 	}
// }

// func TestNGramLimits(t *testing.T) {
// 	ng := GetNGrams("chickens crossed the road", 1, 5)
// 	for k, v := range ng.Weights {
// 		fmt.Printf("%s - %d\n", k, v)
// 	}
// }

func saveWeights(n int) {
	weights := make(map[string]int)
	csvFile, _ := os.Open("./catalog/instacart_2017_05_01/products.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		ng := GetNGrams(strings.ToLower(line[1]), n, n)
		for k, v := range ng.Weights {
			if val, ok := weights[k]; ok {
				weights[k] = val + v
			} else {
				weights[k] = v
			}
		}
	}

	keys := make([]string, 0, len(weights))
	for key := range weights {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return weights[keys[i]] > weights[keys[j]] })

	f, err := os.Create(fmt.Sprintf("./%d-gram.txt", n))
	if err != nil {
		fmt.Printf("%v\n", err)
	} else {
		w := bufio.NewWriter(f)
		for _, key := range keys {
			w.WriteString(fmt.Sprintf("%s|%d\n", key, weights[key]))
		}
		w.Flush()
	}
	csvFile.Close()
}

func TestConvertRawProducts(t *testing.T) {
	ngs := GetNGrams("los angeles", 1, 2)
	for k, _ := range ngs.Weights {
		fmt.Printf("%s\n", k)
	}
	csvfile, err := os.Open("./catalog/demo/raw-products.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}
	r := csv.NewReader(csvfile)
	header := true
	file, err := os.Create("./catalog/demo/products.txt")
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	defer file.Close()
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if !header {
			product := make(map[string]interface{})
			product["_id"] = strings.ToLower(fmt.Sprintf("%s-%s", record[1], record[2]))
			product["location"] = record[1]
			product["description"] = record[3]
			product["categories"] = []string{record[4], record[5]}
			product["brand"] = record[6]
			product["supplier"] = record[7]
			product["stockonhand"], _ = strconv.Atoi(record[8])
			product["stockonorder"], _ = strconv.Atoi(record[9])
			product["backorder"], _ = strconv.Atoi(record[10])
			product["buyprice"], _ = strconv.ParseFloat(record[11], 64)
			product["sellprice"], _ = strconv.ParseFloat(record[12], 64)

			b, err := json.Marshal(product)
			if err == nil {
				value := strings.Replace(string(b), "\n", " ", 0)

				_, err = io.WriteString(file, fmt.Sprintf("%s\n", value))
			}
		}
		header = false

	}
	csvfile.Close()
	file.Sync()
}

func TestBinaryPersist(t *testing.T) {
	m := make(map[string]Record)
	m["product1"] = Record{}
	m["product1"]["description"] = "test description"
	SaveBinary("test.bin", m)
}
