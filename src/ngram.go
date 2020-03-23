package main

import (
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/kljensen/snowball"
)

type Ngrams struct {
	Weights map[string]int
}

func (n Ngrams) SortedKeys() []string {
	keys := make([]string, 0, len(n.Weights))
	for key := range n.Weights {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return n.Weights[keys[i]] > n.Weights[keys[j]] })
	return keys
}

var stopWords = NewSet("&", "with", "the", "and", "in", "of", "+", "-")

func split(word string) []string {
	word = strings.ToLower(word)
	fixUtf := func(r rune) rune {
		if r == utf8.RuneError {
			return -1
		}
		return r
	}

	items := regexp.MustCompile("[\\s]+").Split(word, -1)
	array := make([]string, 0)
	for _, item := range items {
		item = strings.Map(fixUtf, item)
		if !isPunct(item) {
			for {
				if endsWithPunct(item) && len(item) > 0 {
					item = item[0 : len(item)-1]
				} else {
					break
				}
			}

			for {
				if beginsWithPunct(item) && len(item) > 0 {
					item = item[1:len(item)]
				} else {
					break
				}
			}

			if len(item) > 0 {
				array = append(array, item)
			}
		}
	}
	return array
}

func endsWithPunct(s string) bool {
	if len(s) == 0 {
		return false
	}
	return isPunct(string(s[len(s)-1]))
}

func beginsWithPunct(s string) bool {
	if len(s) == 0 {
		return false
	}
	return isPunct(string(s[0]))
}

var punctuation = NewSet("™", "®", "�")

func isPunct(s string) bool {
	if punctuation.Contains(s) {
		return true
	}
	r, _ := regexp.MatchString(`^[^a-zA-Z0-9_:^ascii:]$`, s)
	return r
}

func GetNGrams(s string, minlen int, maxlen int) Ngrams {
	ng := Ngrams{Weights: make(map[string]int)}

	terms := split(s)
	l := len(terms)

	extractNGrams(terms, minlen, maxlen, ng)

	for i := 0; i < l; i++ {
		stemmed, err := stem(terms[i])
		if err == nil {
			terms[i] = stemmed
		}
	}

	extractNGrams(terms, minlen, maxlen, ng)

	//ng.Weights[s] = len(s)

	return ng
}
func extractNGrams(terms []string, minlen int, maxlen int, ng Ngrams) {
	l := len(terms)
	for i := 0; i < l; i++ {
		for j := i; j <= l; j++ {
			if minlen <= j-i && j-i <= maxlen {
				if stopWords.Contains(terms[i]) {
					continue
				}
				if len(terms[i]) == 0 {
					continue
				}
				if j > 0 && i != j-1 {
					if stopWords.Contains(terms[j-1]) {
						continue
					}
					if len(terms[j-1]) == 0 {
						continue
					}
				}

				p := strings.Join(terms[i:j], " ")
				if len(p) < 1 {
					continue
				}

				if val, ok := ng.Weights[p]; ok {
					ng.Weights[p] = val + 1
				} else {
					ng.Weights[p] = 1
				}
			}
		}
	}
}

func stem(word string) (string, error) {
	stemmed, err := snowball.Stem(word, "english", true)
	return stemmed, err
}
