# keywest-search

Provides access to insertion and retrieval of searchable data.

- Attribute hierarchies are near real time
- Product is represented by a colleciton of attribute name-value pairs
- Attribute values can organize hierarchies

## Endpoints

- PUT /insert -- adds a new record
- POST /update -- updates an existing record
- DELETE /delete - deletes an existing record
- GET /index - re-indexes all stored data
- GET /clear - empty out all products

## Internal data structures

- Map Product ID->Product
- Map Product -> Attribute Values[]
- Map Product -> Phrase/Keyword
- Map Phrase/Keyword -> Products[] // tagging
- Map Attribute Value -> Products[]

## Search

- Filter by attribute values
- Filter by phrase/keyword

## Persistence

- As a result of /index (or periodically), all data is snapshotted to disk (but for operational purposes is stored in memory)
- Upon start, service recovers a snapshot
- There's only one indexer in the system, but multiple search API pods are running, using a shared read-only volume. All of them are notified to refresh their configuration from disk from time to time. We use the gcePersistentDisk volumes.

## Query examples

- `http://localhost:5000/api/search/query?q=*:*` - get a list of all product ids, ignore navigation filters in the result set
- `http://localhost:5000/api/search/query?q=*:*&filters=categories` - get a list of all product ids, return all navigation filters on the `categories` attribute
- `http://localhost:5000/api/search/query?q=description:lettuce&filters=categories` - get a list of all products matching a search query of `lettuce`
- `http://localhost:5000/api/search/query?q=description:lettuce&filters=categories&debug=true` - get a list of all products matching a search query of `lettuce`, with debug information enabled
- `http://localhost:5000/api/search/query?q=description:lettuce&filters=categories&fields=description,categories` - return all `lettuce` matching products, navigation for the `categories` attribute, and for every match return the following attributes in addition to `_id`: `description` and `categories`
- `http://localhost:5000/api/search/query?q=description:lettuce&filters=categories&fields=description,categories&selectors=categories:Hydroponic,Leafy%20Greens` - same as above, but filter by a `categories` attribute (`categories` is navigated by `Hydroponic -> Leafy Greens`)

## DONE

- Done: Fix the int/float64 when the data is read from disk json
- Done: Attribute value filters
- Done: Ability do things like `filters=categories,*` to push categories to the top
- Done: Ability to do things like `filters=-description,*` to remove description, but keep everything else
- Done: Ability do things like `filters=categories,*,aeroponic` to push categories to the top and aeroponic to the bottom
- Done: Eliminate selector attributes that yeild no state change (i.e. all products already have this value)
- Done: Inert paths and subpaths support (related to the item above)
- Done: Numeric filters
- Done: Int values are rendered like 12.000000 in filters
- Done: Product ranking / sort etc; `sort=field:a` or `sort=field:d` or `sort=relevancy():d,price:a`
- Done: Pagination
- Done: Boosting records by record id: `promote=recId1,recId2`
- Done: Ability to delete a record
- Done: Update document properties without reading the document first
- Done: Ability to specify exact match mode for a string field
- Done: Optimize saved index to use binary format
- Done: Order attributes by number of hits, then alphabetically, unless order is specified
- Done: Sort products alphabetically or by insertion order in case otherwise they can be positioned arbitrarily
- Done: Limit the number of refinements to 5 + Other
- Done: Support the "..." selector to expand all hidden values

## Immediate priorities

- Personalization engine: Create a session (firebase)
- Include parsed configuration into the response

## ROADMAP (Near term)

- Personalization engine: Record a search/nav/product preference
- Personalization engine: Aggregator of preferences
- Personalization engine: feedback loop
- Bug: Indices get wiped out on some restarts
- Ability to force-reindex
- Kube ingest and search separation
- Boosting records by attribute value `promote=attribureName:attributeValue`
- Boosting filters
- Range queries over fields
- Limiting the number of values per attribute
- Marketing content and management tool
- Configuration tool
- Feedback loop
- Persistence in K8S: https://dzone.com/articles/kubernetes-copying-a-dataset-to-a-statefulsets-persistentvolume
- Filter sorting: alphabetic, by value, by number of hits; applies to filters themselves and their values
- Ability to limit the number of selections per filter
- Partial text search
- Degree of accuracy on text searches
- Optimize saved index to use binary format

## Thoughts on state

- On start all workers load up the most recent complete index

### Centralized indexation

- Create a separate endpoint to ingest data; write it to a volume under a ./genN folder, set a flag when new gen is ready
- Individual workers watch the generation flag, and reload the index

### Decentralized indexation

- Multiple ingestion points
- Write to a kafka topic
- Workers subscribe to this topic
- Complete index contains the offset in kafka topic
- Partitioning done by a partition key

### Alternate Decentralization

- All workers ingest data, notify other workers - gossip protocol is required
- One worker writes the index out, or all do, or they gossip who should

### Consistent centralization

- All workers route to the indexer (with retry)
- Indexer has an internal queue
- As the indexer goes through the queue, it produces versions of the index and notifies workers to pick it up
- Workers reload the index based on notifications, and perform a hot swap
- All workers constantly monitor the indexed version, and reload
- Indexer runs the same code as the worker nodes for consistency reasons
