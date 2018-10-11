package sheets

import (
	"testing"
)

var posTests = []struct {
	pos      CellPos
	expected string
}{
	{CellPos{0, 0}, "A1"},
	{CellPos{1, 0}, "A2"},
	{CellPos{0, 1}, "B1"},
	{CellPos{1, 1}, "B2"},
	{CellPos{10, 10}, "K11"},
	{CellPos{0, 25}, "Z1"},
	{CellPos{0, 26}, "AA1"},
	{CellPos{0, 27}, "AB1"},
	{CellPos{0, 52}, "BA1"},

	{CellPos{0, 624}, "XA1"},
	{CellPos{0, 650}, "YA1"},
	{CellPos{0, 675}, "YZ1"},
	{CellPos{0, 701}, "ZZ1"},
	{CellPos{0, 702}, "AAA1"},
}

func TestCellPosA1Notation(t *testing.T) {

	for _, tt := range posTests {
		got := tt.pos.A1Notation()
		if got != tt.expected {
			t.Errorf("Wanted %s, but got %s for %v", tt.expected, got, tt.pos)
		}
	}
}

var rangeTests = []struct {
	topLeft  CellPos
	width    int
	height   int
	expected string
}{
	{CellPos{}, 1, 1, "A1:A1"},
	{CellPos{}, 1, 2, "A1:A2"},
	{CellPos{}, 2, 2, "A1:B2"},
	{CellPos{0, 10}, 2, 2, "K1:L2"},
	{CellPos{10, 3}, 2, 3, "D11:E13"},
}

func TestRange(t *testing.T) {
	for _, tt := range rangeTests {
		var data [][]string
		for i := 0; i < tt.height; i++ {
			var row []string
			for j := 0; j < tt.width; j++ {
				row = append(row, "1")
			}
			data = append(data, row)
		}

		got := tt.topLeft.RangeForData(data).String()

		if got != tt.expected {
			t.Errorf("Wanted %s, but got %s for %+v, table: %v", tt.expected, got, tt, data)
		}

	}
}
