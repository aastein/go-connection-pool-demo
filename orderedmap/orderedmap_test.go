package orderedmap

import (
	"fmt"
	"reflect"
	"testing"
)

func TestOrderedMap_Get(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*OrderedMap)
		key   interface{}
		want  interface{}
		want1 bool
	}{
		{
			name: "test get",
			setup: func(o *OrderedMap) {
				o.Set("key", "value")
			},
			key:   "key",
			want:  "value",
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			tt.setup(m)
			got, got1 := m.Get(tt.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OrderedMap.Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("OrderedMap.Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestOrderedMap_Set(t *testing.T) {
	tests := []struct {
		name  string
		key   interface{}
		value interface{}
		want  bool
		want2 bool
	}{
		{
			name:  "test set",
			key:   "key",
			value: "value",
			want:  true,
			want2: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			if got := m.Set(tt.key, tt.value); got != tt.want {
				t.Errorf("OrderedMap.Set() = %v, want %v", got, tt.want)
			}

			got, got1 := m.Get(tt.key)
			if !reflect.DeepEqual(got, tt.value) {
				t.Errorf("OrderedMap.Get() got = %v, want %v", got, tt.value)
			}
			if got1 != tt.want2 {
				t.Errorf("OrderedMap.Get() got1 = %v, want %v", got1, tt.want2)
			}
		})
	}
}

func TestOrderedMap_Delete(t *testing.T) {
	tests := []struct {
		name          string
		key           interface{}
		setup         func(*OrderedMap)
		wantDidDelete bool
		want2         bool
	}{
		{
			name: "test delete",
			setup: func(o *OrderedMap) {
				o.Set("key", "value")
			},
			key:           "key",
			wantDidDelete: true,
			want2:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			tt.setup(m)
			if gotDidDelete := m.Delete(tt.key); gotDidDelete != tt.wantDidDelete {
				t.Errorf("OrderedMap.Delete() = %v, want %v", gotDidDelete, tt.wantDidDelete)
			}
			if _, got := m.Get(tt.key); got != tt.want2 {
				t.Errorf("OrderedMap.Get() = %v, want %v", got, tt.want2)
			}
		})
	}
}

func TestOrderedMap_Front(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*OrderedMap)
		wantNil   bool
		wantKey   int
		wantValue int
	}{
		{
			name: "test front",
			setup: func(om *OrderedMap) {
				om.Set(0, 0)
			},
			wantKey:   0,
			wantValue: 0,
		},
		{
			name: "test front",
			setup: func(om *OrderedMap) {
			},
			wantNil:   true,
			wantKey:   0,
			wantValue: 0,
		},
		{
			name: "test front",
			setup: func(om *OrderedMap) {
				om.Set(0, 0)
				om.Set(1, 1)
				om.Delete(0)
			},
			wantKey:   1,
			wantValue: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			tt.setup(m)
			got := m.Front()
			if got == nil {
				if !tt.wantNil {
					t.Errorf("OrderedMap.Front() = %v, want %v", got, "not nill")
				}
			} else {
				if tt.wantKey != got.Key.(int) {
					t.Errorf("OrderedMap.Front() key = %v, want %v", got.Key.(int), tt.wantKey)
				}
				if tt.wantValue != got.Key.(int) {
					t.Errorf("OrderedMap.Front() value = %v, want %v", got.Value.(int), tt.wantValue)
				}
			}
		})
	}
}

func TestElement_Next(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*OrderedMap)
		wantKeys   []int
		wantValues []int
	}{
		{
			name: "test next",
			setup: func(om *OrderedMap) {
				om.Set(0, 0)
			},
			wantKeys:   []int{0},
			wantValues: []int{0},
		},
		{
			name: "test next 2",
			setup: func(om *OrderedMap) {
				om.Set(0, 0)
				om.Set(1, 1)
			},
			wantKeys:   []int{0, 1},
			wantValues: []int{0, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New()
			tt.setup(e)
			fmt.Println("fuck", e.Len())
			elem := e.Front()
			i := 0
			for ; elem != nil; i++ {
				fmt.Println("elem key", elem.Key)
				if tt.wantKeys[i] != elem.Key.(int) {
					t.Errorf("OrderedMap.Front() key = %v, want %v", elem.Key.(int), tt.wantKeys[i])
				}
				if tt.wantValues[i] != elem.Key.(int) {
					t.Errorf("OrderedMap.Front() value = %v, want %v", elem.Value.(int), tt.wantValues[i])
				}
				elem = elem.Next()
				if elem == nil {
					break
				}
			}
			if i+1 != len(tt.wantKeys) {
				t.Errorf("OrderedMap.Front() called %v times, expected %v", i+1, len(tt.wantValues))
			}
		})
	}
}
