package odoo

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"sync"
	"time"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
)

var OdooManager, _ = NewSessionManger()

const sessionExpiryHouse = 6

type SessionManager struct {
	client    *http.Client
	lastLogin time.Time
	lock      sync.Mutex
}

func NewSessionManger() (*SessionManager, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &SessionManager{
		client: &http.Client{
			Jar:     jar,
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (session *SessionManager) odooLogin() error {
	session.lock.Lock()
	defer session.lock.Unlock()
	payload := map[string]any{
		"jsonrpc": "2.0",
		"params": map[string]any{
			"db":       config.ConfigGlobal.OdooDB,
			"login":    config.ConfigGlobal.OdooUsername,
			"password": config.ConfigGlobal.OdooPassword,
		},
	}
	loginUrl := config.ConfigGlobal.OdooURL + "web/session/authenticate"
	jsonData, _ := json.Marshal(payload)
	request, _ := http.NewRequest("POST", loginUrl, bytes.NewBuffer(jsonData))
	request.Header.Set("Content-Type", "application/json")

	response, err := session.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	var result struct {
		Result struct {
			UID int `json:"uid"`
		} `json:"result"`
	}

	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return err
	}

	if result.Result.UID == 0 {
		return errors.New("Odoo Login Failed")
	}
	session.lastLogin = time.Now().UTC()
	return nil
}

func (session *SessionManager) ensureSession() error {
	if session.lastLogin.IsZero() {
		return session.odooLogin()
	}

	if time.Since(session.lastLogin) > time.Duration(sessionExpiryHouse)*time.Hour {
		return session.odooLogin()
	}
	return nil
}

func (session *SessionManager) NewRequest(method, endpoint string, payload map[string]any) (*http.Response, error) {
	if err := session.ensureSession(); err != nil {
		return nil, err
	}

	url := config.ConfigGlobal.OdooURL + endpoint
	jsonData, err := json.Marshal(payload)
	if (err) != nil {
		return nil, err
	}
	request, err := http.NewRequest(method, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json")
	response, err := session.client.Do(request)
	if err != nil {
		if err := session.odooLogin(); err != nil {
			return nil, err
		}
		return session.client.Do(request)
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		response.Body.Close()
		if err := session.odooLogin(); err != nil {
			return nil, err
		}
		return session.client.Do(request)
	}
	return response, nil
}
