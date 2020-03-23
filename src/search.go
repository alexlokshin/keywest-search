package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const MaxRefinements = 5
const Expand = "..."

type ProductAttributeValue struct {
	Value   string
	Counter int
}

type ProductAttribute struct {
	Name    string
	Values  []ProductAttributeValue
	Counter int
}

type Filters struct {
	Attributes []ProductAttribute
}

type SortOrder struct {
	SortField string
	SortOrder bool
}

type RecordStats struct {
	Ngrams KeyWestStringSet
	Weight int
}

func (r *RecordStats) SetWeight(w int) {
	r.Weight = w
}

func search(c *gin.Context) {

	// Query format q=<FIELD>:<VALUE>
	// To search all indexed fields, use q=*:VALUE
	// To search for the value in several fields, use q=f1,f2,f3:<VALUE> (TODO)
	// To intersect multiple queries, q=<FIELD1>:<VALUE1>|<FIELD2>:<VALUE2> (TODO)
	// Query like *:VALUE will automatically find best possible combination of ngrams using the search strategy.
	// Default strategy is s=exact. Other strategies are s=fuzzy (TODO) -- it will find all possible sub n-grams that
	// have matches and return them. The longer the match, the higher the accuracy

	// By default, response will contain only the _id field. To include other fields in the response, use
	// fields=field1,field2,field3 . To include all fields, use f=*

	t := time.Now()

	// Text query against an index
	q := strings.ToLower(strings.TrimSpace(c.DefaultQuery("q", "")))
	// Navigation filter
	// selectors=hydroponic:true+category:Hydroponic,Vegetables
	s := strings.ToLower(strings.TrimSpace(c.DefaultQuery("selectors", "")))

	// Record fields to return
	fields := strings.ToLower(strings.TrimSpace(c.DefaultQuery("fields", "")))
	// Navigation filters to return
	filters := strings.ToLower(strings.TrimSpace(c.DefaultQuery("filters", "")))

	// sort=relevancy():d,price:a
	sortParam := strings.ToLower(strings.TrimSpace(c.DefaultQuery("sort", "")))
	sortOrders := parseSort(sortParam)

	// page=1|25 - page 1, with 25 items per page
	paginationParam := strings.ToLower(strings.TrimSpace(c.DefaultQuery("page", "")))

	promoteParam := strings.TrimSpace(c.DefaultQuery("promote", ""))

	debug := strings.ToLower(strings.TrimSpace(c.DefaultQuery("debug", "false"))) == "true"

	resultFields := strings.Split(fields, ",")
	resultFilters := strings.Split(filters, ",")
	resultFilterSet := KeyWestStringSet{}
	for _, filter := range resultFilters {
		resultFilterSet.Add(filter)
	}

	recordsToPromote := KeyWestIntSet{}
	if len(strings.TrimSpace(promoteParam)) > 0 {
		promoteIds := strings.Split(promoteParam, ",")
		for i, promoteId := range promoteIds {
			trimmedPromoteId := strings.TrimSpace(promoteId)
			if len(trimmedPromoteId) > 0 {
				recordsToPromote.AddWithCount(trimmedPromoteId, i)
			}
		}
	}

	selectorMap := extractSelectors(s)

	query := strings.Split(q, ":")
	if len(query) != 2 {
		c.JSON(500, gin.H{
			"Status": "Error",
			"Error":  "Improperly formatted query",
			"Time":   time.Since(t).Nanoseconds(),
		})
		return
	}

	searchField := query[0]
	searchValue := query[1]

	if len(searchField) == 0 || len(searchValue) == 0 {
		c.JSON(200, gin.H{
			"Status":  "OK",
			"Results": nil,
			"Time":    time.Since(t).Nanoseconds(),
		})
		return
	}

	if index, ok := indices[searchField]; ok {

		// Get records ids for search; or the entire set for navigation
		recIds := getRecIds(searchValue, index)

		// Filter by provided selectors
		recIds = filterRecords(selectorMap, recIds)

		// Process all records, extracting attribute values
		variations := make(map[string]KeyWestIntSet)
		recs := make([]Record, 0)

		for recordId, recordStat := range recIds {
			rec := records[recordId]
			var filteredRecord Record
			if len(resultFields) == 1 && resultFields[0] == "*" {
				filteredRecord = rec
			} else {
				filteredRecord = Record{"_id": rec["_id"]}
				for _, field := range resultFields {
					if val, ok2 := rec[field]; ok2 {
						filteredRecord[field] = val
					}
				}
			}

			processRecord(rec, resultFilterSet, variations, selectorMap)

			if debug {
				filteredRecord["_ngrams"] = recordStat.Ngrams.Values()
			}
			filteredRecord["_weight"] = recordStat.Weight
			recs = append(recs, filteredRecord)
		}

		// Eliminate dead ends and inert values
		attributeMap := extractAttributeMap(variations, len(recIds))

		// Sort search results
		sortSearchResults(sortOrders, recordsToPromote, recs)

		totalRecs := len(recs)
		recs = paginateSearchResults(paginationParam, recs)

		topFilters := make([]string, 0)
		bottomFilters := make([]string, 0)
		unsortedFilters := make([]string, 0)
		// If filters!=* sort attributeMap keys by the order in which they appeared in resultFilters
		if filters == "*" {
			resultFilters = make([]string, 0)
			for attributeName := range attributeMap {
				unsortedFilters = append(unsortedFilters, attributeName)
			}
		} else {
			//topFilters := make([]string, 0)
			//bottomFilters := make([]string, 0)
			resultFilterSet = KeyWestStringSet{}
			direction := true

			for _, filter := range resultFilters {
				if strings.Index(filter, "-") == 0 {
					resultFilterSet.Add(strings.TrimLeft(filter, "-"))
				}
			}

			for _, filter := range resultFilters {
				if "*" == filter {
					direction = false
					continue
				}
				if strings.Index(filter, "-") == 0 {
					continue
				}
				if !resultFilterSet.Contains(filter) {
					if direction {
						//topFilters = append(topFilters, filter)
						topFilters = append(topFilters, filter)
					} else {
						//bottomFilters = append(bottomFilters, filter)
						bottomFilters = append(bottomFilters, filter)
					}
					resultFilterSet.Add(filter)
				}
			}
			for attributeName := range attributeMap {
				if !resultFilterSet.Contains(attributeName) {
					unsortedFilters = append(unsortedFilters, attributeName)
				}
			}
			// for _, filter := range bottomFilters {
			// 	topFilters = append(topFilters, filter)
			// }
			// resultFilters = topFilters
		}

		resultFilters = make([]string, 0)
		for _, filter := range topFilters {
			resultFilters = append(resultFilters, filter)
		}
		for _, filter := range unsortedFilters {
			resultFilters = append(resultFilters, filter)
		}
		for _, filter := range bottomFilters {
			resultFilters = append(resultFilters, filter)
		}

		// Arrange left nav attributes based on the order provided
		attributes := prepareFilters(selectorMap, resultFilters, attributeMap)

		attributeLocator := make(map[string]ProductAttribute)
		for _, attribute := range attributes {
			attributeLocator[attribute.Name] = attribute
		}

		fixedAttributes := make([]ProductAttribute, 0)
		for _, resultFilter := range topFilters {
			attribute, ok := attributeLocator[resultFilter]
			if ok {
				fixedAttributes = append(fixedAttributes, attribute)
				delete(attributeLocator, resultFilter)
			}
		}

		unsorteAttributes := make([]ProductAttribute, 0)
		for _, resultFilter := range unsortedFilters {
			attribute, ok := attributeLocator[resultFilter]
			if ok {
				unsorteAttributes = append(unsorteAttributes, attribute)
				delete(attributeLocator, resultFilter)
			}
		}

		// If all filters are specified, rank them by the counter

		sort.Slice(unsorteAttributes, func(i, j int) bool {
			if unsorteAttributes[i].Counter == unsorteAttributes[j].Counter {
				return strings.Compare(unsorteAttributes[i].Name, unsorteAttributes[j].Name) > 0
			} else {
				return unsorteAttributes[i].Counter > unsorteAttributes[j].Counter
			}
		})

		for _, attribute := range unsorteAttributes {
			fixedAttributes = append(fixedAttributes, attribute)
		}

		for _, resultFilter := range bottomFilters {
			attribute, ok := attributeLocator[resultFilter]
			if ok {
				fixedAttributes = append(fixedAttributes, attribute)
				delete(attributeLocator, resultFilter)
			}
		}

		c.JSON(200, gin.H{
			"Status":    "OK",
			"Results":   recs,
			"TotalRecs": totalRecs,
			"Count":     len(recs),
			"Filters":   Filters{Attributes: fixedAttributes},
			"Time":      time.Since(t).Nanoseconds(),
		})

	} else {
		c.JSON(500, gin.H{
			"Status": "Error",
			"Error":  "Field index not found",
			"Time":   time.Since(t).Nanoseconds(),
		})
	}

}

func getRecIds(searchValue string, index FieldIndex) map[string]*RecordStats {
	recIds := make(map[string]*RecordStats)
	if len(searchValue) > 0 && "*" != searchValue {
		ngrams := GetNGrams(searchValue, 1, 5)

		for ngram := range ngrams.Weights { // second parameter is weight of an ngram within an entire universe of documents
			if results, ok1 := index.HierarchyLocator[ngram]; ok1 {
				for k := range results.Products {
					if _, found := records[k]; found {
						if _, found1 := recIds[k]; !found1 {
							recIds[k] = &RecordStats{Ngrams: KeyWestStringSet{}, Weight: 0}
						}
						stat := recIds[k]
						stat.Ngrams.Add(ngram)
						stat.SetWeight(stat.Weight + len(ngram))
					}
				}
			}
		}
	} else {
		for k := range records {
			recIds[k] = &RecordStats{Ngrams: KeyWestStringSet{}, Weight: 0}
		}
	}
	return recIds
}

func sortSearchResults(sortOrders []SortOrder, recordsToPromote KeyWestIntSet, recs []Record) {

	sort.Slice(recs, func(i, j int) bool {
		id1 := recs[i]["_id"].(string)
		id2 := recs[j]["_id"].(string)
		rec1 := records[id1]
		rec2 := records[id2]

		returnValue := 0

		if len(recordsToPromote) > 0 {
			value1, ok1 := recordsToPromote[id1]
			value2, ok2 := recordsToPromote[id2]
			if !ok1 {
				value1 = 99999
			}
			if !ok2 {
				value2 = 99999
			}
			if value1 < value2 {
				return true
			} else {
				return false
			}
		}

		for _, sortOrder := range sortOrders {

			if sortOrder.SortField == "relevancy()" {
				value1 := recs[i]["_weight"].(int)
				value2 := recs[j]["_weight"].(int)
				if value1 == value2 {
					continue
				}

				if value1 < value2 {
					returnValue = -1
				} else {
					returnValue = 1
				}

				if !sortOrder.SortOrder {
					returnValue = -1 * returnValue
				}

				break
			}
			fieldType := schema[sortOrder.SortField]
			fieldName := sortOrder.SortField
			if "int" == fieldType || "float64" == fieldType {
				value1 := rec1[fieldName].(float64)
				value2 := rec2[fieldName].(float64)
				if value1 == value2 {
					continue // This means we have to move on to the next field in the chain
				}

				if value1 < value2 {
					returnValue = -1
				} else {
					returnValue = 1
				}

				if !sortOrder.SortOrder {
					returnValue = -1 * returnValue
				}
			}
			if "string" == fieldType {
				value1 := rec1[fieldName].(string)
				value2 := rec2[fieldName].(string)

				comp := strings.Compare(value1, value2)
				if comp == 0 {
					continue // This means we have to move on to the next field in the chain
				}
				if comp < 0 {
					returnValue = -1
				} else {
					returnValue = 1
				}

				if !sortOrder.SortOrder {
					returnValue = -1 * returnValue
				}
			}
		}

		if returnValue == 0 {
			return strings.Compare(id1, id2) < 0
		} else {
			return returnValue < 0
		}
	})
}

func extractAttributeMap(variations map[string]KeyWestIntSet, recordCount int) map[string]KeyWestIntSet {
	attributeMap := make(map[string]KeyWestIntSet)
	for attributeName, attributeValues := range variations {
		attValues := KeyWestIntSet{}
		fieldType, ok := schema[attributeName]
		if ok && "array[string]" == fieldType {
			varTree := NewVarTreeSet()

			for attributeValue, count := range attributeValues {
				current := &varTree
				parts := strings.Split(attributeValue, "|")
				for _, part := range parts {
					current = current.AddCount(part, count)
				}
				// if count == recordCount {
				// 	// TODO: Support inert paths
				// }

				//attValues.AddWithCount(attributeValue, count)
			}
			// if len(attValues) > 0 {
			// 	attributeMap[attributeName] = attValues
			// }
			refinements, inertPath := varTree.GetInertPath()
			if len(refinements) > 0 {
				if len(inertPath) > 0 {
					inertPath += ">"
				}
				for refinement, refCount := range refinements {
					attValues.AddWithCount(inertPath+refinement, refCount)
				}
				attributeMap[attributeName] = attValues
			}

		} else {
			for attributeValue, count := range attributeValues {
				if count < recordCount {
					attValues.AddWithCount(attributeValue, count)
				}
			}
			if len(attValues) > 0 && len(attValues) < recordCount {
				attributeMap[attributeName] = attValues
			}
		}
	}
	return attributeMap
}

func extractSelectors(s string) map[string]interface{} {
	selectorMap := make(map[string]interface{})
	requestSelectors := make([]string, 0)
	if strings.Contains(s, "|") {
		requestSelectors = strings.Split(s, "|")
	} else {
		requestSelectors = []string{s}
	}

	for _, requestSelector := range requestSelectors {
		parts := strings.Split(requestSelector, ":")
		if len(parts) == 2 {
			fieldName := strings.TrimSpace(strings.ToLower(parts[0]))
			fieldValue := parts[1]
			fieldType, ok := schema[fieldName]
			if !ok {
				continue
			}

			if "bool" == fieldType || "int" == fieldType || "float64" == fieldType {
				selectorMap[parts[0]] = fieldValue
			}

			if "string" == fieldType {
				selectorMap[parts[0]] = fieldValue
			}

			if "array[string]" == fieldType {
				segments := strings.Split(fieldValue, ",")
				selectorMap[parts[0]] = segments
			}
		}
	}
	return selectorMap
}

func processRecord(rec Record, resultFilterSet KeyWestStringSet, variations map[string]KeyWestIntSet, selectorMap map[string]interface{}) {
	for fieldName, v := range rec {
		k := strings.ToLower(fieldName)

		if "_id" == k {
			continue
		}
		if !resultFilterSet.Contains(k) && !resultFilterSet.Contains("*") {
			continue
		}

		fieldType, ok := schema[k]

		if !ok {
			continue
		}

		vars, ok := variations[k]
		if !ok {
			vars = KeyWestIntSet{}
			variations[k] = vars
		}

		val, ok := selectorMap[k]

		if Expand == val {
			ok = false
		}
		if !ok {

			if "bool" == fieldType {
				value := strconv.FormatBool(v.(bool))
				vars.Add(value)
			}
			if "int" == fieldType {
				value := fmt.Sprintf("%d", int(v.(float64)))
				vars.Add(value)
			}
			if "float64" == fieldType {
				value := fmt.Sprintf("%.2f", v.(float64))
				vars.Add(value)
			}

			if "string" == fieldType {
				value := v.(string)
				vars.Add(value)
			}
		}

		if "array[string]" == fieldType {
			value := ""
			var hierarchy []string
			if selectorMap[k] != nil {
				hierarchy = selectorMap[k].([]string)
			}

			for i, att := range v.([]interface{}) {
				if i >= len(hierarchy) {
					if len(value) > 0 {
						value += "|"
					}
					value += att.(string)
				} else if !strings.EqualFold(att.(string), hierarchy[i]) {
					break
				}
			}

			if len(value) > 0 {
				vars.Add(value)
			}
		}
	}
}

func prepareFilters(selectorMap map[string]interface{}, resultFilters []string, attributeMap map[string]KeyWestIntSet) []ProductAttribute {
	attributes := make([]ProductAttribute, 0)
	for _, attributeName := range resultFilters {
		values, ok := attributeMap[attributeName]
		if values == nil || !ok {
			continue
		}

		attributeValues := make([]ProductAttributeValue, 0)
		for value, count := range values {
			attributeValues = append(attributeValues, ProductAttributeValue{Value: value, Counter: count})
		}
		sort.Slice(attributeValues, func(i, j int) bool {
			if attributeValues[i].Counter == attributeValues[j].Counter {
				return strings.Compare(attributeValues[i].Value, attributeValues[j].Value) < 0
			} else {
				return attributeValues[i].Counter > attributeValues[j].Counter
			}
		})

		if len(attributeValues) > 0 {
			counter := 0
			for _, attributeValue := range attributeValues {
				counter += attributeValue.Counter
			}
			if counter > 0 {
				if float64(attributeValues[0].Counter)/float64(counter) > 0.02 {
					fmt.Printf("Ratio: %s->%f\n", attributeName, float64(attributeValues[0].Counter)/float64(counter))
					if Expand != selectorMap[attributeName] {
						if len(attributeValues) > MaxRefinements {
							attributeValues = attributeValues[0:MaxRefinements]
							visibleCounter := 0
							for _, attributeValue := range attributeValues {
								visibleCounter += attributeValue.Counter
							}

							attributeValues = append(attributeValues, ProductAttributeValue{Value: Expand, Counter: counter - visibleCounter})
						}
					}

					attribute := ProductAttribute{Name: attributeName, Values: attributeValues, Counter: counter}
					attributes = append(attributes, attribute)
				}
			}
		}

	}
	return attributes
}

func filterRecords(selectorMap map[string]interface{}, recIds map[string]*RecordStats) map[string]*RecordStats {
	for fieldName, selector := range selectorMap {
		fieldType, ok := schema[fieldName]
		if !ok {
			continue
		}
		if Expand == selector {
			continue
		}
		if "bool" == fieldType || "float64" == fieldType || "int" == fieldType {
			recordSet := indices[fieldName].HierarchyLocator[selector.(string)].Products
			recIds = Intersect(recIds, recordSet)
		}
		if "string" == fieldType {
			recordSet := indices[fieldName].HierarchyLocator[selector.(string)].Products
			recIds = Intersect(recIds, recordSet)
		}
		if "array[string]" == fieldType {
			hierarchy := selector.([]string)
			index, ok := indices[fieldName]
			if !ok {
				continue
			}

			subIndex := index.HierarchyLocator
			var products KeyWestStringSet = nil
			for _, item := range hierarchy {
				val, ok := subIndex[item]
				if ok {
					subIndex = val.HierarchyLocator
					products = val.Products
				}
			}

			if products != nil {
				recIds = Intersect(recIds, products)
			}
		}
	}
	return recIds
}

func Intersect(recIds map[string]*RecordStats, filteredSet KeyWestStringSet) map[string]*RecordStats {
	for id := range recIds {
		if _, ok := filteredSet[id]; !ok {
			delete(recIds, id)
		}
	}
	return recIds
}

func parseSort(sort string) []SortOrder {
	sortOrders := make([]SortOrder, 0)
	sortSequences := strings.Split(sort, ",")

	for _, sortSequence := range sortSequences {
		parts := strings.Split(sortSequence, ":")
		if len(parts) < 2 {
			continue
		}
		fieldName := parts[0]
		if _, ok := indices[fieldName]; !ok {
			continue
		}
		if parts[1] != "a" && parts[1] != "d" {
			continue
		}
		sortOrders = append(sortOrders, SortOrder{
			SortField: fieldName,
			SortOrder: parts[1] == "a",
		})
	}
	if len(sortOrders) == 0 {
		sortOrders = append(sortOrders, SortOrder{SortField: "relevancy()", SortOrder: false})
	}
	return sortOrders
}

func paginateSearchResults(paginationParam string, recs []Record) []Record {
	parts := strings.Split(paginationParam, "|")
	if len(parts) == 2 {

		pageNo, err := strconv.Atoi(parts[0])
		if err != nil {
			return recs
		}
		pageSize, err := strconv.Atoi(parts[1])
		if err != nil {
			return recs
		}
		if pageNo < 1 {
			return recs
		}
		offset := (pageNo - 1) * pageSize
		if offset > len(recs) {
			return make([]Record, 0)
		}
		lastIndex := offset + pageSize
		if lastIndex > len(recs) {
			lastIndex = len(recs)
		}
		fmt.Printf("Items: %d", len(recs[offset:lastIndex]))
		return recs[offset:lastIndex]
	} else {
		return recs
	}
}
