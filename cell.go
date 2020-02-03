package sheets

import (
	"fmt"
	"strings"
)

const (
	Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type CellPos struct {
	Row int
	Col int
}

func (c CellPos) A1Notation() string {
	return fmt.Sprintf("%s%d", aRangeLetter(c.Col), c.Row+1)
}

func (c CellPos) RangeForData(data [][]interface{}) CellRange {
	bottomLeft := CellPos{c.Row + len(data) - 1, c.Col + len(data[0]) - 1}

	return CellRange{Start: c, End: bottomLeft}
}

type CellRange struct {
	Start CellPos
	End   CellPos
}

func (a CellRange) String() string {
	return fmt.Sprintf("%s:%s", a.Start.A1Notation(), a.End.A1Notation())
}

type SheetRange struct {
	SheetName string
	Range     CellRange
}

func (s SheetRange) String() string {
	return fmt.Sprintf("%s!%s", s.SheetName, s.Range.String())
}

func DefaultRange(data [][]string) CellRange {
	bottomLeft := CellPos{len(data), len(data[0])}

	return CellRange{CellPos{}, bottomLeft}
}

func aRangeLetter(num int) string {
	base := len(Alphabet)
	start := num

	numDigits := 1

	for start >= base {
		// A1 is like base 26 with letters instead of digits,
		// except that "A", "AA", and "AAA" would all be 0 in base 26
		// and in A1 they are different numbers.
		// Subtract the base here so it behaves more like base 26.
		start -= base
		numDigits += 1
		base *= len(Alphabet)
	}

	base /= len(Alphabet)

	digits := make([]string, numDigits)
	for i := 0; i < numDigits-1; i += 1 {
		idx := start / base
		digits[i] = Alphabet[idx : idx+1]
		start = start - (idx * base)
		base = base / len(Alphabet)
	}

	digits[numDigits-1] = Alphabet[start : start+1]

	return strings.Join(digits, "")
}
