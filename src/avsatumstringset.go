package main

type KeyWestStringSet map[string]bool

func (s KeyWestStringSet) Add(val string) {
	s[val] = true
}

func (s KeyWestStringSet) Remove(val string) {
	delete(s, val)
}

func (s KeyWestStringSet) Contains(val string) bool {
	_, ok := s[val]
	return ok
}

func (s KeyWestStringSet) Values() []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	return keys
}

func NewSet(args ...string) KeyWestStringSet {
	set := make(KeyWestStringSet)
	for _, v := range args {
		set[v] = true
	}
	return set
}
