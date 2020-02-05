package sheets

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	retry "github.com/avast/retry-go"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
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

func (c *Client) ShareWithAnyone(fileID string) error {
	perm := drive.Permission{
		Role: "writer",
		Type: "anyone",

		AllowFileDiscovery: false,
	}

	return googleRetry(func() error {
		_, err := c.Drive.Permissions.Create(fileID, &perm).Do()
		return err
	})
}

func (c *Client) shareFile(fileID, email string, notify bool) error {
	perm := drive.Permission{
		EmailAddress: email,
		Role:         "writer",
		Type:         "user",
	}

	req := c.Drive.Permissions.Create(fileID, &perm)
	if notify {
		req.SendNotificationEmail(true)
	}

	return googleRetry(func() error {
		_, err := req.Do()
		return err
	})
}

func (c *Client) ListFiles(query string) ([]*drive.File, error) {
	var resp *drive.FileList
	err := googleRetry(func() error {
		var rerr error
		resp, rerr = c.Drive.Files.List().PageSize(10).
			Q(query).
			Fields("nextPageToken, files(id, name, mimeType)").Do()

		return rerr
	})
	if err != nil {
		return nil, err
	}

	return resp.Files, nil
}

func (c *Client) CopySpreadsheetFrom(fileID, newName string) (*Spreadsheet, error) {
	var file *drive.File
	err := googleRetry(func() error {
		var rerr error
		file, rerr = c.Drive.Files.Copy(fileID, &drive.File{
			Name: newName,
		}).Do()

		return rerr
	})
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
	var ssInfo *sheets.Spreadsheet
	err := googleRetry(func() error {
		var rerr error
		ssInfo, rerr = c.Sheets.Spreadsheets.Create(ssProps).Do()

		return rerr
	})
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

	return googleRetry(func() error {
		return req.Do()
	})
}

// Transfer ownership of the file
func (c *Client) TransferOwnership(fileID, email string) error {
	perm := drive.Permission{
		EmailAddress: email,
		Role:         "owner",
		Type:         "user",
	}
	req := c.Drive.Permissions.Create(fileID, &perm).TransferOwnership(true)

	return googleRetry(func() error {
		_, err := req.Do()
		return err
	})
}

func (c *Client) GetSpreadsheet(spreadsheetId string) (*Spreadsheet, error) {
	var ssInfo *sheets.Spreadsheet
	err := googleRetry(func() error {
		var rerr error
		ssInfo, rerr = c.Sheets.Spreadsheets.Get(spreadsheetId).Do()

		return rerr
	})
	if err != nil {
		return nil, err
	}

	return &Spreadsheet{c, ssInfo}, nil
}

func (c *Client) GetSpreadsheetWithData(spreadsheetId string) (*Spreadsheet, error) {
	var ssInfo *sheets.Spreadsheet
	err := googleRetry(func() error {
		var rerr error
		ssInfo, rerr = c.Sheets.Spreadsheets.Get(spreadsheetId).IncludeGridData(true).Do()

		return rerr
	})
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

func googleRetry(f func() error) error {
	return retry.Do(
		f,
		retry.Delay(30*time.Second),
		retry.Attempts(5),
		retry.RetryIf(func(err error) bool {
			if gerr, ok := err.(*googleapi.Error); ok {
				switch {
				case gerr.Code == 429:
					return true

				case (gerr.Code >= 500 && gerr.Code <= 599):
					return true

				case gerr.Code == 403 && gerr.Message == "Rate Limit Exceeded":
					return true

				default:
					return false
				}
			}

			return false
		}),
	)
}
