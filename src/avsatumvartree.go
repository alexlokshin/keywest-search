package main

type KeyWestVarTree struct {
	Counter  int
	Children map[string]*KeyWestVarTree
}

func NewVarTreeSet() KeyWestVarTree {
	return KeyWestVarTree{Counter: 0, Children: make(map[string]*KeyWestVarTree)}
}

func (t *KeyWestVarTree) Add(val string) *KeyWestVarTree {
	child, ok := t.Children[val]
	if !ok {
		childTree := NewVarTreeSet()
		child = &childTree
		t.Children[val] = child
	}

	child.Counter = child.Counter + 1

	return child
}

func (t *KeyWestVarTree) AddCount(val string, counter int) *KeyWestVarTree {
	child, ok := t.Children[val]
	if !ok {
		childTree := NewVarTreeSet()
		child = &childTree
		t.Children[val] = child
	}

	child.Counter = child.Counter + counter

	return child
}

func (t *KeyWestVarTree) GetInertPath() (KeyWestIntSet, string) {
	leafs := KeyWestIntSet{}
	inertPath := ""

	current := t

	for {
		if len(current.Children) == 0 {
			break
		}

		if len(current.Children) == 1 {
			for k, v := range current.Children {
				if len(v.Children) > 0 {
					if len(inertPath) > 0 {
						inertPath += ">"
					}
					inertPath += k
				} else {
					leafs.AddWithCount(k, v.Counter)
				}
				current = v
				break
			}
		}

		if len(current.Children) > 1 {
			for k, v := range current.Children {
				leafs.AddWithCount(k, v.Counter)
			}
			break
		}
	}

	return leafs, inertPath
}
