package sheets

import (
	"strings"
	"testing"
)

var tsvTests = []struct {
	data   string
	result [][]string
}{
	{"a\tb", [][]string{[]string{"a", "b"}}},
	{"a\tb\tc\nd\te\tf", [][]string{
		[]string{"a", "b", "c"},
		[]string{"d", "e", "f"},
	}},
}

func TestTsvToArr(t *testing.T) {
	for _, tt := range tsvTests {
		got := TsvToArr(strings.NewReader(tt.data), "\t")
		if len(got) != len(tt.result) {
			t.Errorf("For \n%s\n, mismatched rows. wanted %v, but got %v", tt.data, tt.result, got)
		}

		for i, row := range got {
			wantRow := tt.result[i]
			if len(row) != len(wantRow) {
				t.Errorf("For \n%s\n row %d, mismatched cols. wanted %v, but got %v", tt.data, i, wantRow, row)
				break
			}

			for j, cell := range row {
				if cell != wantRow[j] {
					t.Errorf("For \n%s\n [%d][%d], wanted %v, but got %v", tt.data, i, j, wantRow[j], cell)
				}
			}
		}
	}
}
