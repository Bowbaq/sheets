package sheets

import (
	"bufio"
	"os"

	"fmt"
	"io"
	"strings"

	retry "github.com/avast/retry-go"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
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

func (s *Spreadsheet) DeleteSheet(title string) error {
	query := strings.ToLower(title)
	for _, sheet := range s.Sheets {
		lowerTitle := strings.ToLower(sheet.Properties.Title)
		if lowerTitle == query {
			_, err := s.DoBatch(&sheets.Request{
				DeleteSheet: &sheets.DeleteSheetRequest{
					SheetId: sheet.Properties.SheetId,
				},
			})
			return err
		}
	}
	return errors.New("sheet does not exist")
}

func (s *Spreadsheet) DuplicateSheet(title, newTitle string) (*Sheet, error) {
	origin := s.GetSheet(title)
	if origin == nil {
		return nil, errors.New("origin sheet does not exist")
	}

	alreadyExists := s.GetSheet(newTitle)
	if alreadyExists != nil {
		return nil, errors.New("destination sheet already exist")
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
		if !isFakeDuplicateSheetError(err) {
			return nil, errors.Wrap(err, "couldn't duplicate sheet")
		}

		// Need to make sure that we've got the latest state of the sheet
		currentSheet, err := s.Client.GetSpreadsheet(s.Id())
		if err != nil {
			return nil, errors.Wrap(err, "error refreshing spreadsheet after fake duplicate error")
		}
		s.Spreadsheet = currentSheet.Spreadsheet
	}

	duplicate := s.GetSheet(newTitle)
	if duplicate == nil {
		return nil, errors.New("duplicate sheet does not exist")
	}

	return duplicate, nil
}

func isFakeDuplicateSheetError(err error) bool {
	rerr, ok := err.(retry.Error)
	if !ok {
		return false
	}

	var (
		firstErrorIsNotDuplicate = true
		hasSubsequentDuplicate   = false
	)
	for i, e := range rerr.WrappedErrors() {
		if gerr, ok := e.(*googleapi.Error); ok {
			if gerr.Code == 400 && strings.Contains(gerr.Message, "duplicateSheet") {
				if i == 0 {
					firstErrorIsNotDuplicate = false
				} else {
					hasSubsequentDuplicate = true
				}
			}
		}
		if e != nil {
			fmt.Fprintf(os.Stderr, "%d - %v\n", i, e)
		}
	}

	return firstErrorIsNotDuplicate && hasSubsequentDuplicate
}

func (s *Sheet) Title() string {
	return s.Properties.Title
}

func (s *Sheet) TopLeft() CellPos {
	return CellPos{0, 0}
}

func (s *Sheet) BottomRight() CellPos {
	if len(s.Data) == 0 {
		return s.TopLeft()
	}

	rows := 0
	cols := 0
	if len(s.Data[0].RowData) > 0 {
		rows = len(s.Data[0].RowData) - 1

		if len(s.Data[0].RowData[0].Values) > 0 {
			cols = len(s.Data[0].RowData[0].Values) - 1
		}
	}

	return CellPos{Row: rows, Col: cols}
}

func (s *Sheet) DataRange() SheetRange {
	return SheetRange{
		SheetName: s.Properties.Title,
		Range: CellRange{
			Start: s.TopLeft(),
			End:   s.BottomRight(),
		},
	}
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
			if value.EffectiveValue != nil && value.EffectiveValue.StringValue != nil {
				row[colIdx] = *value.EffectiveValue.StringValue
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

	return googleRetry(func() error {
		_, err := req.Do()
		return err
	})
}

func (s *Sheet) Append(data [][]interface{}) error {
	req := s.Client.Sheets.Spreadsheets.Values.Append(
		s.Spreadsheet.Id(),
		s.DataRange().String(),
		&sheets.ValueRange{
			Values: data,
		},
	)
	req.ValueInputOption("USER_ENTERED")

	return googleRetry(func() error {
		_, err := req.Do()
		return err
	})
}

func (s *Spreadsheet) DoBatch(reqs ...*sheets.Request) (*sheets.BatchUpdateSpreadsheetResponse, error) {
	batchUpdateReq := sheets.BatchUpdateSpreadsheetRequest{
		Requests:                     reqs,
		IncludeSpreadsheetInResponse: true,
	}

	var resp *sheets.BatchUpdateSpreadsheetResponse
	err := googleRetry(func() error {
		var rerr error
		resp, rerr = s.Client.Sheets.Spreadsheets.BatchUpdate(s.Id(), &batchUpdateReq).Do()
		return rerr
	})
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

func (s *Spreadsheet) ShareWithAnyone() error {
	return s.Client.ShareWithAnyone(s.Id())
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
