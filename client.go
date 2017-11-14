package sheets

import (
  "context"
  "fmt"
  "io"
  "io/ioutil"

  "golang.org/x/oauth2/jwt"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/drive/v3"
)

type Client struct {
	Sheets *sheets.Service
	Drive *drive.Service
}

const (
	sheetMimeType = "application/vnd.google-apps.spreadsheet"
)

func (c *Client) ShareFile(fileID, email string) error {
  perm := drive.Permission{
    EmailAddress: email,
    Role: "writer",
    Type: "user",
  }

  req := c.Drive.Permissions.Create(fileID, &perm).SendNotificationEmail(false)


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

func (c *Client) CreateSpreadsheetFromTsv(title string, reader io.Reader) (*Spreadsheet, error) {
  arr := TsvToArr(reader)
  return c.CreateSpreadsheet(title, arr)
}

func (c *Client) CreateSpreadsheet(title string, data [][]string) (*Spreadsheet, error) {
  ssProps := &sheets.Spreadsheet{
    Properties: &sheets.SpreadsheetProperties{Title: title},
  }
  ssInfo, err := c.Sheets.Spreadsheets.Create(ssProps).Do()
  if err != nil {
    return nil, err
  }

  ss := &Spreadsheet{
    Client: c,
    Spreadsheet: ssInfo,
  }

  cellRange := DefaultRange(data)
  sheetname := ""
  err = ss.Import(sheetname, data, cellRange)

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
    Role: "owner",
    Type: "user",
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

