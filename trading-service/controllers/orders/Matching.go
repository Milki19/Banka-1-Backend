package orders

import (
	"banka1.com/middlewares"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"banka1.com/db"
	"banka1.com/types"
	"gorm.io/gorm"
)

var (
	securityLocks = make(map[uint]*sync.Mutex)
	locksMu       sync.Mutex
)

// Funkcija koja vraća uvek isti mutex po securityID
func getLock(securityID uint) *sync.Mutex {
	locksMu.Lock()
	defer locksMu.Unlock()

	if _, exists := securityLocks[securityID]; !exists {
		securityLocks[securityID] = &sync.Mutex{}
	}
	return securityLocks[securityID]
}

func MatchOrder(order types.Order) {
	go func() {
		if order.AON {
			if !CanExecuteAll(order) {
				fmt.Println("AON: Nema dovoljno za celokupan order")
				return
			}
		}

		if !canPreExecute(order) {
			fmt.Println("Nije ispunjen uslov za order")
			return
		}

		for order.RemainingParts != nil && *order.RemainingParts > 0 {
			tx := db.DB.Begin()
			quantityToExecute := 1
			if *order.RemainingParts < quantityToExecute {
				quantityToExecute = *order.RemainingParts
			}

			price := getOrderPrice(order)

			token, err := middlewares.NewOrderToken(order.Direction, order.UserID, order.AccountID, price)
			url := fmt.Sprintf("%s/orders/execute/%s", os.Getenv("BANKING_SERVICE"), token)
			var hadError bool

			if err == nil {
				agent := fiber.Post(url)

				statusCode, _, errs := agent.Bytes()

				if len(errs) != 0 || statusCode != 200 {
					hadError = true
				}

			} else {
				hadError = true
			}

			if hadError {
				fmt.Printf("Nalog %v nije izvršen\n", order.ID)
				break
			} else {
				executePartial(order, quantityToExecute, price, tx)

				*order.RemainingParts -= quantityToExecute

				// SKIDANJE unita ako je kupovina (smanjuje se dostupnost hartija)
				if order.Direction == "buy" {
					var security types.Security
					if err := db.DB.First(&security, order.SecurityID).Error; err == nil {
						security.Volume -= int64(quantityToExecute)
						if security.Volume < 0 {
							security.Volume = 0
						}
						tx.Save(&security)
					}
					//fmt.Printf("Volume pre: %d | Smanjujem za %d\n", security.Volume, quantityToExecute)
				}

				if order.RemainingParts == nil || *order.RemainingParts == 0 {
					order.IsDone = true
					order.Status = "done"
				}

				tx.Model(&types.Order{}).Where("id = ?", order.ID).Updates(map[string]interface{}{
					"remaining_parts": *order.RemainingParts,
					"is_done":         order.IsDone,
					"status":          order.Status,
				})

				if err := tx.Commit().Error; err != nil {
					fmt.Printf("Nalog %v nije izvršen: %v\n", order.ID, err)
					tx.Rollback()
					break
				} else {
					fmt.Printf("Nalog %v izvršen\n", order.ID)
				}
				db.DB.First(&order, order.ID)
			}

			delay := calculateDelay(order)
			time.Sleep(delay)
		}
	}()
}

func getListingPrice(order types.Order) float64 {
	var security types.Security
	err := db.DB.First(&security, order.SecurityID).Error
	if err != nil {
		fmt.Printf("Security nije pronadjen za ID %d: %v\n", order.SecurityID, err)
		return -1.0
	}

	var listing types.Listing
	err = db.DB.Where("ticker = ?", security.Ticker).First(&listing).Error
	if err != nil {
		fmt.Printf("Listing nije pronadjen za Ticker %s: %v\n", security.Ticker, err)
		return -1.0
	}

	if order.Direction == "sell" {
		return float64(listing.Bid)
	} else {
		return float64(listing.Ask)
	}
}

func getOrderPrice(order types.Order) float64 {
	if strings.ToUpper(order.OrderType) == "MARKET" {
		var security types.Security
		db.DB.First(&security, order.SecurityID)
		return security.LastPrice
	}
	if order.StopPricePerUnit != nil {
		return *order.StopPricePerUnit
	}
	if order.LimitPricePerUnit != nil {
		return *order.LimitPricePerUnit
	}
	return 0.0
}

func executePartial(order types.Order, quantity int, price float64, tx *gorm.DB) {
	lock := getLock(order.SecurityID)
	lock.Lock()
	defer lock.Unlock()

	var match types.Order
	direction := "buy"
	if order.Direction == "buy" {
		direction = "sell"
	} else {
		direction = "buy"
	}
	db.DB.Where("security_id = ? AND direction = ? AND status = ? AND is_done = ?", order.SecurityID, direction, "approved", false).
		Order("last_modified").
		First(&match)

	if match.ID == 0 {
		fmt.Println("Nema dostupnog ordera za matchovanje")
		return
	}

	if match.AON && (match.RemainingParts == nil || *match.RemainingParts < quantity) {
		fmt.Println("Matchovani order je AON i ne može da se izvrši u celosti")
		return
	}

	if match.Margin {
		var actuary types.Actuary
		if err := db.DB.Where("user_id = ?", match.UserID).First(&actuary).Error; err != nil || actuary.Department != "agent" {
			fmt.Println("Matchovani margin order nema validnog aktuara")
			return
		}
		initialMargin := price * float64(quantity) * 0.3 * 1.1
		if actuary.LimitAmount-actuary.UsedLimit < initialMargin {
			fmt.Println("Matchovani margin order nema dovoljno limita")
			return
		}
	}

	if match.UserID == order.UserID {
		fmt.Println("Preskočen self-match")
		return
	}

	if !canPreExecute(match) {
		fmt.Println("Preskočen match sa nedovoljnim uslovima")
		return
	}

	matchQuantity := quantity
	if match.RemainingParts != nil && *match.RemainingParts < quantity {
		matchQuantity = *match.RemainingParts
	}

	txn := types.Transaction{
		OrderID:      order.ID,
		BuyerID:      getBuyerID(order, match),
		SellerID:     getSellerID(order, match),
		SecurityID:   order.SecurityID,
		Quantity:     matchQuantity,
		PricePerUnit: price,
		TotalPrice:   price * float64(matchQuantity),
	}
	if err := tx.Create(&txn).Error; err != nil {
		fmt.Printf("Greska pri kreiranju transakcije: %v\n", err)
	}

	*order.RemainingParts -= matchQuantity
	*match.RemainingParts -= matchQuantity
	if *match.RemainingParts == 0 {
		match.IsDone = true
		match.Status = "done"
	}
	//tx.Save(&order)
	//tx.Save(&match)
	if err := tx.Save(&order).Error; err != nil {
		fmt.Printf("Greska pri save za order: %v\n", err)
	}
	if err := tx.Save(&match).Error; err != nil {
		fmt.Printf("Greska pri save za match: %v\n", err)
	}

	updatePortfolio(getBuyerID(order, match), order.SecurityID, matchQuantity, tx)
	updatePortfolio(getSellerID(order, match), order.SecurityID, -matchQuantity, tx)

	if order.Margin {
		var actuary types.Actuary
		if err := db.DB.Where("user_id = ?", order.UserID).First(&actuary).Error; err == nil {
			initialMargin := price * float64(matchQuantity) * 0.3 * 1.1
			actuary.UsedLimit += initialMargin
			tx.Save(&actuary)
		}
	}

	fmt.Printf("Match success: Order %d ↔ Order %d za %d @ %.2f\n", order.ID, match.ID, matchQuantity, price)
}

func updatePortfolio(userID uint, securityID uint, delta int, tx *gorm.DB) {
	var portfolio types.Portfolio

	err := tx.Where("user_id = ? AND security_id = ?", userID, securityID).First(&portfolio).Error
	if err != nil {
		if delta > 0 {
			portfolio = types.Portfolio{
				UserID:        userID,
				SecurityID:    securityID,
				Quantity:      delta,
				PurchasePrice: 0,
			}
			//fmt.Printf("Kreiram portfolio za user: %d, security: %d, quantity: %d\n", userID, securityID, delta)
			if err := tx.Create(&portfolio).Error; err != nil {
				fmt.Printf("Greška pri kreiranju portfolia: %v\n", err)
			}
		}
		return
	}

	portfolio.Quantity += delta
	if portfolio.Quantity <= 0 {
		if err := tx.Delete(&portfolio).Error; err != nil {
			fmt.Printf("Greška pri brisanju portfolia: %v\n", err)
		}
	} else {
		//fmt.Printf("Ažuriram portfolio → User: %d | Security: %d | Nova količina: %d\n", userID, securityID, portfolio.Quantity)
		if err := tx.Save(&portfolio).Error; err != nil {
			fmt.Printf("Greška pri ažuriranju portfolia: %v\n", err)
		}
	}
}

func calculateDelay(order types.Order) time.Duration {
	delaySeconds := rand.Intn(10) + 1
	if order.AfterHours {
		return time.Duration(delaySeconds+1800) * time.Second
	}
	return time.Duration(delaySeconds) * time.Second
}

func getExecutableParts(order types.Order) int {
	var matchingOrders []types.Order
	direction := "buy"
	if order.Direction == "buy" {
		direction = "sell"
	} else {
		direction = "buy"
	}

	db.DB.Where("security_id = ? AND direction = ? AND status = ? AND is_done = ?", order.SecurityID, direction, "approved", false).Find(&matchingOrders)
	totalAvailable := 0
	for _, o := range matchingOrders {
		if o.RemainingParts != nil {
			totalAvailable += *o.RemainingParts
		}
	}

	return totalAvailable
}

func CanExecuteAll(order types.Order) bool {
	return getExecutableParts(order) >= *order.RemainingParts
}

func CanExecuteAny(order types.Order) bool {
	return getExecutableParts(order) > 0
}

func canPreExecute(order types.Order) bool {
	price := getListingPrice(order)
	if price < 0 {
		return false
	}
	if strings.ToUpper(order.OrderType) == "LIMIT" {
		if order.Direction == "sell" {
			return price >= *order.LimitPricePerUnit
		} else {
			return price <= *order.LimitPricePerUnit
		}
	} else if strings.ToUpper(order.OrderType) == "STOP" {
		if order.Direction == "sell" {
			return price <= *order.StopPricePerUnit
		} else {
			return price >= *order.StopPricePerUnit
		}
	} else if strings.ToUpper(order.OrderType) == "STOP-LIMIT" {
		if order.Direction == "sell" {
			return price <= *order.StopPricePerUnit && price >= *order.LimitPricePerUnit
		} else {
			return price >= *order.StopPricePerUnit && price <= *order.LimitPricePerUnit
		}
	}
	return true
}

func getBuyerID(a, b types.Order) uint {
	if a.Direction == "buy" {
		return a.UserID
	}
	return b.UserID
}

func getSellerID(a, b types.Order) uint {
	if a.Direction == "sell" {
		return a.UserID
	}
	return b.UserID
}
