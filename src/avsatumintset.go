package main

type KeyWestIntSet map[string]int

func (s KeyWestIntSet) Add(val string) {
	value, ok := s[val]
	if !ok {
		s[val] = 1
	} else {
		s[val] = value + 1
	}
}

func (s KeyWestIntSet) AddWithCount(val string, count int) {
	s[val] = count
}

func (s KeyWestIntSet) Contains(val string) bool {
	_, ok := s[val]
	return ok
}

func (s KeyWestIntSet) Values() []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	return keys
}
