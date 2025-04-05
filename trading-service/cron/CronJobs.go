package cron

import (
	"banka1.com/db"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gofiber/fiber/v2/log"
	"gorm.io/gorm"

	"banka1.com/types"
	"github.com/robfig/cron/v3"
)

type Employee struct {
	ID          int      `json:"id"`
	FirstName   string   `json:"firstName"`
	LastName    string   `json:"lastName"`
	Email       string   `json:"email"`
	Department  string   `json:"department"`
	Position    string   `json:"position"`
	Active      bool     `json:"active"`
	Permissions []string `json:"permissions"`
}

type APIResponse struct {
	Success bool       `json:"success"`
	Data    []Employee `json:"data"`
}

// cron posao koji resetuje limit agentu svakog dana u 23 59
func StartScheduler() {
	createNewActuaries()
	c := cron.New(cron.WithSeconds())

	_, err := c.AddFunc("0 59 23 * * *", func() {
		resetDailyLimits()
	})

	_, err = c.AddFunc("0 1 0 * * *", func() {
		expireOldOptionContracts()
	})

	_, err = c.AddFunc("0/15 * * * * * ", func() {
		createNewActuaries()
	})

	if err != nil {
		log.Errorf("Greska pri pokretanju cron job-a:", err)
		return
	}

	c.Start()
}

func expireOldOptionContracts() {
	now := time.Now()

	var contracts []types.OptionContract
	if err := db.DB.Where("settlement_at < ? AND status = ?", now, "active").Find(&contracts).Error; err != nil {
		log.Errorf("Greška pri pronalaženju ugovora za expirovanje: %v", err)
		return
	}

	for _, contract := range contracts {
		contract.Status = "expired"
		if err := db.DB.Save(&contract).Error; err != nil {
			log.Errorf("Greška pri expirovanju ugovora ID %d: %v", contract.ID, err)
		} else {
			log.Infof("Ugovor ID %d označen kao 'expired'", contract.ID)
		}
	}
}

func resetDailyLimits() {
	db.DB.Model(&types.Actuary{}).Where("role = ?", "agent").Update("usedLimit", 0)
}

func createNewActuaries() {
	data, err := GetActuaries()

	if err != nil {
		return
	}

	if len(data.Data) == 0 {
		return
	}
	for _, actuaryData := range data.Data {
		newActuary := employeeToActuary(actuaryData)

		var existingActuary types.Actuary
		err := db.DB.Where("user_id = ?", newActuary.UserID).First(&existingActuary).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if result := db.DB.Create(&newActuary); result.Error != nil {
					log.Errorf("Error creating actuary %v: %v", newActuary, result.Error)
				} else {
					log.Infof("Created new actuary: %v", newActuary)
				}
			} else {
				// Some other error occurred
				log.Errorf("Error checking actuary existence: %v", err)
			}
		} else {
		}
	}
}

func employeeToActuary(employee Employee) types.Actuary {
	actuary := types.Actuary{
		UserID:      uint(employee.ID),
		Role:        employee.Department,
		FullName:    employee.FirstName + " " + employee.LastName,
		Email:       employee.Email,
		LimitAmount: 100000,
	}
	return actuary
}

func GetActuaries() (*APIResponse, error) {
	basePath := os.Getenv("USER_SERVICE")
	url := basePath + "/api/users/employees/actuaries"

	resp, err := http.Get(url)
	if err != nil {
		log.Infof("Failed to fetch %s: %v\n", url, err)
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != 200 {
		log.Infof("Error fetching %s: HTTP %d\n", url, resp.StatusCode)
		return nil, err
	}

	var apiResponse *APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		log.Infof("Failed to parse JSON: %v\n", err)
		return nil, err
	}
	return apiResponse, nil
}
