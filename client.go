package sheets

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"time"

	retry "github.com/avast/retry-go"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	sheets "google.golang.org/api/sheets/v4"
)

type Client struct {
	JWTConfig *jwt.Config

	Sheets *sheets.Service
	Drive  *drive.Service

	options []googleapi.CallOption
}

func NewServiceAccountClientFromReader(creds io.Reader) (*Client, error) {
	jwtJSON, err := ioutil.ReadAll(creds)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read credentials")
	}

	config, err := google.JWTConfigFromJSON(jwtJSON, sheets.SpreadsheetsScope, drive.DriveScope)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse JWT config")
	}

	return NewClientFromConfig(config)
}

func NewImpersonatingServiceAccountClient(creds io.Reader, userEmail string) (*Client, error) {
	jwtJSON, err := ioutil.ReadAll(creds)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read credentials")
	}

	config, err := google.JWTConfigFromJSON(jwtJSON, sheets.SpreadsheetsScope, drive.DriveScope)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse JWT config")
	}
	config.Subject = userEmail

	return NewClientFromConfig(config)
}

func NewClientFromConfig(config *jwt.Config) (*Client, error) {
	client := config.Client(context.Background())

	sheetsSrv, err := sheets.New(client)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't initialize sheets client")
	}

	driveSrv, err := drive.New(client)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't initialize drive client")
	}

	return &Client{
		JWTConfig: config,

		Sheets: sheetsSrv,
		Drive:  driveSrv,
	}, nil
}

func (c *Client) AddOptions(opts ...googleapi.CallOption) {
	c.options = append(c.options, opts...)
}

func (c *Client) ListFiles(query string) ([]*drive.File, error) {
	var resp *drive.FileList
	err := googleRetry(func() error {
		var rerr error
		resp, rerr = c.Drive.Files.List().PageSize(10).
			Q(query).
			Fields("nextPageToken, files(id, name, mimeType)").Do(c.options...)

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
		}).Do(c.options...)

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
		ssInfo, rerr = c.Sheets.Spreadsheets.Create(ssProps).Do(c.options...)

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

func (c *Client) GetSpreadsheet(spreadsheetId string) (*Spreadsheet, error) {
	var ssInfo *sheets.Spreadsheet
	err := googleRetry(func() error {
		var rerr error
		ssInfo, rerr = c.Sheets.Spreadsheets.Get(spreadsheetId).Do(c.options...)

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
		ssInfo, rerr = c.Sheets.Spreadsheets.Get(spreadsheetId).IncludeGridData(true).Do(c.options...)

		return rerr
	})
	if err != nil {
		return nil, err
	}

	return &Spreadsheet{c, ssInfo}, nil
}

func (c *Client) Delete(fileId string) error {
	req := c.Drive.Files.Delete(fileId)

	return googleRetry(func() error {
		return req.Do(c.options...)
	})
}

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
		_, err := c.Drive.Permissions.Create(fileID, &perm).Do(c.options...)
		return err
	})
}

func (c *Client) shareFile(fileID, email string, notify bool) error {
	perm := drive.Permission{
		EmailAddress: email,
		Role:         "writer",
		Type:         "user",
	}
	req := c.Drive.Permissions.Create(fileID, &perm).SendNotificationEmail(notify)

	return googleRetry(func() error {
		_, err := req.Do(c.options...)
		return err
	})
}

func (c *Client) Revoke(fileID, email string) error {
	var permissions *drive.PermissionList
	err := googleRetry(func() error {
		var rerr error
		permissions, rerr = c.Drive.Permissions.List(fileID).Fields("nextPageToken, permissions(id, emailAddress, type, role)").Do(c.options...)

		return rerr
	})
	if err != nil {
		return errors.Wrapf(err, "couldn't list permissions for %s", fileID)
	}

	for _, p := range permissions.Permissions {
		if p.EmailAddress != email {
			continue
		}

		return googleRetry(func() error {
			return c.Drive.Permissions.Delete(fileID, p.Id).Do(c.options...)
		})
	}

	return nil
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
		_, err := req.Do(c.options...)
		return err
	})
}

func googleRetry(f func() error) error {
	return retry.Do(
		f,
		retry.Delay(15*time.Second),
		retry.Attempts(5),
		retry.RetryIf(func(err error) bool {
			// Retry network errors, sometimes Google's API craps out
			if _, ok := err.(*net.OpError); ok {
				return true
			}
			if strings.Contains(err.Error(), "connection reset by peer") {
				return true
			}
			if err == io.EOF {
				return true
			}

			// Retry more specific Google API errors
			if gerr, ok := err.(*googleapi.Error); ok {
				switch {
				// Too many requests
				case gerr.Code == 429:
					return true

				// Too many requests as a 403
				case gerr.Code == 403 && gerr.Message == "Rate Limit Exceeded":
					return true

				// Server error. This may lead to duplicates, calling code must check for that
				case (gerr.Code >= 500 && gerr.Code <= 599):
					return true
				}
			}

			return false
		}),
	)
}
