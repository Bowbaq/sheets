package sheets

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	sheets "google.golang.org/api/sheets/v4"
)

type Spreadsheet struct {
	Client *Client
	*sheets.Spreadsheet
}

type Sheet struct {
	*sheets.Sheet
	Spreadsheet *Spreadsheet
	Client      *Client
}

func (s *Spreadsheet) Id() string {
	return s.SpreadsheetId
}

func (s *Spreadsheet) Url() string {
	return s.SpreadsheetUrl
}

func (s *Spreadsheet) GetSheet(title string) *Sheet {
	query := strings.ToLower(title)
	for _, sheet := range s.Sheets {
		lowerTitle := strings.ToLower(sheet.Properties.Title)
		if lowerTitle == query {
			return &Sheet{sheet, s, s.Client}
		}
	}
	return nil
}

func (s *Spreadsheet) DuplicateSheet(title, newTitle string) (*Sheet, error) {
	origin := s.GetSheet(title)
	if origin == nil {
		return nil, errors.New("origin sheet does not exist")
	}

	var maxIndex int64
	for _, sheet := range s.Sheets {
		if sheet.Properties.Index > maxIndex {
			maxIndex = sheet.Properties.Index
		}
	}

	_, err := s.DoBatch(&sheets.Request{
		DuplicateSheet: &sheets.DuplicateSheetRequest{
			InsertSheetIndex: maxIndex + 1,
			NewSheetName:     newTitle,
			SourceSheetId:    origin.Properties.SheetId,
		},
	})
	if err != nil {
		return nil, err
	}

	duplicate := s.GetSheet(newTitle)
	if duplicate == nil {
		return nil, errors.New("duplicate sheet does not exist")
	}

	return duplicate, nil
}

func (s *Sheet) Title() string {
	return s.Properties.Title
}

func (s *Sheet) Resize(rows, cols int) error {
	return nil
}

func (*Sheet) TopLeft() CellPos {
	return CellPos{0, 0}
}

func (s *Sheet) Update(data [][]string) error {
	return s.UpdateFromPosition(data, s.TopLeft())
}

func (s *Sheet) GetContents() ([][]string, error) {
	if s.Data == nil {
		return nil, fmt.Errorf("No data fetched, only callable on sheets fetched with GetSpreadsheetWithData TODO: fetch!")
	}

	// Not sure where there would be multiple data
	data := s.Data[0]

	matrix := make([][]string, len(data.RowData))
	for rowNum, rowData := range data.RowData {
		row := make([]string, len(rowData.Values))
		for colIdx, value := range rowData.Values {
			if value.EffectiveValue != nil {
				row[colIdx] = value.EffectiveValue.StringValue
			} else {
				row[colIdx] = ""
			}
		}
		matrix[rowNum] = row
	}

	return matrix, nil
}

func (s *Sheet) UpdateFromPosition(data [][]string, start CellPos) error {
	// Convert to interfaces to satisfy the Google API
	converted := make([][]interface{}, 0)

	for _, row := range data {
		converted = append(converted, strToInterface(row))
	}

	return s.UpdateFromPositionIface(converted, start)
}

func (s *Sheet) UpdateFromPositionIface(data [][]interface{}, start CellPos) error {
	cellRange := start.RangeForData(data)

	sheetRange := fmt.Sprintf("%s!%s", s.Title(), cellRange.String())

	// TODO: Resize sheet
	vRange := &sheets.ValueRange{
		Range:  sheetRange,
		Values: data,
	}

	req := s.Client.Sheets.Spreadsheets.Values.Update(s.Spreadsheet.Id(), sheetRange, vRange)

	req.ValueInputOption("USER_ENTERED")
	_, err := req.Do()

	return err
}

func (s *Spreadsheet) DoBatch(reqs ...*sheets.Request) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	batchUpdateReq := sheets.BatchUpdateSpreadsheetRequest{
		Requests:                     reqs,
		IncludeSpreadsheetInResponse: true,
	}

	resp, err := s.Client.Sheets.Spreadsheets.BatchUpdate(s.Id(), &batchUpdateReq).Do()

	if err != nil {
		return nil, err
	}

	s.Spreadsheet = resp.UpdatedSpreadsheet

	return resp, nil
}

func (s *Spreadsheet) AddSheet(title string) (*Sheet, error) {
	sheet := s.GetSheet(title)

	if sheet != nil {
		return sheet, nil
	}

	props := sheets.SheetProperties{Title: title}
	addReq := sheets.Request{AddSheet: &sheets.AddSheetRequest{Properties: &props}}

	_, err := s.DoBatch(&addReq)
	if err != nil {
		return nil, err
	}

	sheet = s.GetSheet(title)

	if sheet == nil {
		return nil, fmt.Errorf("Unable to get sheet after adding it: %s", title)
	}

	return sheet, nil
}

func (s *Spreadsheet) Share(email string) error {
	return s.Client.ShareFile(s.Id(), email)
}

func (s *Spreadsheet) ShareNotify(email string) error {
	return s.Client.ShareFileNotify(s.Id(), email)
}

func TsvToArr(reader io.Reader, delimiter string) [][]string {
	scanner := bufio.NewScanner(reader)

	data := make([][]string, 0)

	for scanner.Scan() {
		pieces := strings.Split(scanner.Text(), delimiter)
		data = append(data, pieces)
	}

	return data
}

func strToInterface(strs []string) []interface{} {
	arr := make([]interface{}, len(strs))

	for i, s := range strs {
		arr[i] = s
	}
	return arr
}
