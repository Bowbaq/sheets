package sheets

import (
	"strings"
	"testing"
)

var configTests = []struct {
	config      string
	errExpected bool
}{
	{"{}", true},
	{`{
  "type": "service_account",
  "project_id": "testproject-123456",
  "private_key_id": "abcdef",
  "private_key": "-----BEGIN PRIVATE KEY-----\nnotarealkey\n-----END PRIVATE KEY-----\n",
  "client_email": "robot@testproject-123456.iam.gserviceaccount.com",
  "client_id": "115842414406405072982",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/robot%40testproject-123456.iam.gserviceaccount.com"
}`, false},
}

func TestNewServiceAccountClient(t *testing.T) {

	for _, tt := range configTests {
		_, err := NewServiceAccountClient(strings.NewReader(tt.config))

		if tt.errExpected && err == nil {
			t.Error("Expected error, but got none")
		}

		if !tt.errExpected && err != nil {
			t.Error("Unexpected error: %v", err)
		}
	}
}
