package sheets

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	drive "google.golang.org/api/drive/v3"
	sheets "google.golang.org/api/sheets/v4"
)

type Client struct {
	Sheets *sheets.Service
	Drive  *drive.Service
}

const (
	sheetMimeType = "application/vnd.google-apps.spreadsheet"
)

func (c *Client) ShareFile(fileID, email string) error {
	return c.shareFile(fileID, email, false)
}

func (c *Client) ShareFileNotify(fileID, email string) error {
	return c.shareFile(fileID, email, true)
}

func (c *Client) shareFile(fileID, email string, notify bool) error {
	perm := drive.Permission{
		EmailAddress: email,
		Role:         "writer",
		Type:         "user",
	}

	req := c.Drive.Permissions.Create(fileID, &perm).SendNotificationEmail(notify)

	_, err := req.Do()
	return err
}

func (c *Client) ListFiles(query string) ([]*drive.File, error) {
	r, err := c.Drive.Files.List().PageSize(10).
		Q(query).
		Fields("nextPageToken, files(id, name, mimeType)").Do()

	if err != nil {
		return nil, err
	}

	return r.Files, nil
}

func (c *Client) CopySpreadsheetFrom(fileID, newName string) (*Spreadsheet, error) {
	file, err := c.Drive.Files.Copy(fileID, &drive.File{
		Name: newName,
	}).Do()

	if err != nil {
		return nil, err
	}

	return c.GetSpreadsheet(file.Id)
}

func (c *Client) CreateSpreadsheetFromTsv(title string, reader io.Reader) (*Spreadsheet, error) {
	arr := TsvToArr(reader, "\t")
	return c.CreateSpreadsheetWithData(title, arr)
}

func (c *Client) CreateSpreadsheetFromCsv(title string, reader io.Reader, delimiter string) (*Spreadsheet, error) {
	arr := TsvToArr(reader, delimiter)
	return c.CreateSpreadsheetWithData(title, arr)
}

func (c *Client) CreateSpreadsheet(title string) (*Spreadsheet, error) {
	ssProps := &sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{Title: title},
	}
	ssInfo, err := c.Sheets.Spreadsheets.Create(ssProps).Do()
	if err != nil {
		return nil, err
	}

	ss := &Spreadsheet{
		Client:      c,
		Spreadsheet: ssInfo,
	}

	return ss, nil
}

func (c *Client) CreateSpreadsheetWithData(title string, data [][]string) (*Spreadsheet, error) {
	ss, err := c.CreateSpreadsheet(title)
	if err != nil {
		return nil, err
	}

	sheetname := "Sheet1"
	sheet := ss.GetSheet(sheetname)
	if sheet == nil {
		return nil, fmt.Errorf("Couldn't find sheet %s for %s", sheetname, ss.Id())
	}
	err = sheet.Update(data)

	return ss, err
}

func (c *Client) Delete(fileId string) error {
	req := c.Drive.Files.Delete(fileId)
	err := req.Do()
	return err
}

// Transfer ownership of the file
func (c *Client) TransferOwnership(fileID, email string) error {
	perm := drive.Permission{
		EmailAddress: email,
		Role:         "owner",
		Type:         "user",
	}

	req := c.Drive.Permissions.Create(fileID, &perm).TransferOwnership(true)
	_, err := req.Do()
	return err
}

func (c *Client) GetSpreadsheet(spreadsheetId string) (*Spreadsheet, error) {
	ssInfo, err := c.Sheets.Spreadsheets.Get(spreadsheetId).Do()

	if err != nil {
		return nil, err
	}

	return &Spreadsheet{c, ssInfo}, nil
}

func (c *Client) GetSpreadsheetWithData(spreadsheetId string) (*Spreadsheet, error) {
	ssInfo, err := c.Sheets.Spreadsheets.Get(spreadsheetId).IncludeGridData(true).Do()

	if err != nil {
		return nil, err
	}

	return &Spreadsheet{c, ssInfo}, nil
}

func getServiceAccountConfig(reader io.Reader) (*jwt.Config, error) {
	b, err := ioutil.ReadAll(reader)

	if err != nil {
		return nil, fmt.Errorf("Unable to read credentials file: %s", err)
	}

	config, err := google.JWTConfigFromJSON(b, sheets.SpreadsheetsScope, drive.DriveScope)
	if err != nil {
		return nil, fmt.Errorf("Unable parse JWT config: %s", err)
	}

	return config, nil
}

func NewServiceAccountClient(credsReader io.Reader) (*Client, error) {
	config, err := getServiceAccountConfig(credsReader)

	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	client := config.Client(ctx)

	sheetsSrv, err := sheets.New(client)
	if err != nil {
		return nil, err
	}

	driveSrv, err := drive.New(client)
	if err != nil {
		return nil, err
	}

	return &Client{sheetsSrv, driveSrv}, nil
}
