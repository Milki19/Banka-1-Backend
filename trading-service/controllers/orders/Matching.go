package orders

import (
	"banka1.com/broker"
	"banka1.com/dto"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
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
	orderLocks    = make(map[uint]*sync.Mutex)
	orderLocksMu  sync.Mutex
)

func CalculateFee(order types.Order, total float64) float64 {
	switch strings.ToUpper(order.OrderType) {
	case "MARKET":
		fee := total * 0.14
		if fee > 7 {
			return 7
		}
		return fee
	case "LIMIT":
		fee := total * 0.24
		if fee > 12 {
			return 12
		}
		return fee
	default:
		return 0
	}
}

// Funkcija koja vraća uvek isti mutex po securityID
func getLock(securityID uint) *sync.Mutex {
	locksMu.Lock()
	defer locksMu.Unlock()

	if _, exists := securityLocks[securityID]; !exists {
		securityLocks[securityID] = &sync.Mutex{}
	}
	return securityLocks[securityID]
}

func getOrderLock(orderID uint) *sync.Mutex {
	orderLocksMu.Lock()
	defer orderLocksMu.Unlock()

	if _, exists := orderLocks[orderID]; !exists {
		orderLocks[orderID] = &sync.Mutex{}
	}
	return orderLocks[orderID]
}

func MatchOrder(order types.Order) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Gorutina pukla u MatchOrder! Panic: %v\n", r)
			}
		}()
		// Zaključavanje po ORDER ID – sprečava paralelno izvršavanje istog ordera
		orderLock := getOrderLock(order.ID)
		orderLock.Lock()
		defer orderLock.Unlock()

		if err := db.DB.First(&order, order.ID).Error; err != nil {
			fmt.Printf("Greska pri refetch ordera %d: %v\n", order.ID, err)
			return
		}

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
			fmt.Printf("Novi krug matchovanja za Order %d | Remaining: %d\n", order.ID, *order.RemainingParts)

			tx := db.DB.Begin()

			// Ponovo proveri order iz baze unutar transakcije
			if err := tx.First(&order, order.ID).Error; err != nil {
				fmt.Printf("Order nije pronađen u transakciji: %v\n", err)
				tx.Rollback()
				break
			}

			price := getOrderPrice(order)

			orderCopy := order

			matchQuantity := executePartial(&orderCopy, price, tx)
			if order.RemainingParts != nil {
				fmt.Printf("Nakon executePartial: remaining=%d\n", *orderCopy.RemainingParts)
			} else {
				fmt.Printf("order.RemainingParts je NIL nakon executePartial\n")
			}

			if matchQuantity == 0 {
				fmt.Printf("BREAK: Nije pronađen validan match za Order %d, remaining=%d\n", order.ID, *order.RemainingParts)
				tx.Rollback()
				break
			}
			//TO-DO PROVERITI OVO ISPOD
			//executePartial(order, quantityToExecute, price, tx)

			//if order.RemainingParts == nil || *order.RemainingParts == 0 {
			//	order.IsDone = true
			//	order.Status = "done"
			//}

			// SKIDANJE unita ako je kupovina (smanjuje se dostupnost hartija)
			//if order.Direction == "buy" {
			//	var security types.Security
			//	if err := tx.First(&security, order.SecurityID).Error; err == nil {
			//		security.Volume -= int64(quantityToExecute)
			//		if security.Volume < 0 {
			//			security.Volume = 0
			//		}
			//		tx.Save(&security)
			//	}
			//}

			// Ažuriraj order u bazi (unutar transakcije)
			if err := tx.Model(&types.Order{}).Where("id = ?", order.ID).Update("remaining_parts", *order.RemainingParts).Error; err != nil {
				fmt.Printf("Greska pri upisu remaining_parts: %v\n", err)
				tx.Rollback()
				break
			}

			//if err := tx.Commit().Error; err != nil {
			//	fmt.Printf("Nalog %v nije izvršen: %v\n", order.ID, err)
			//	tx.Rollback()
			//	break
			//}

			// Ažuriraj volume preko helper funkcije
			if err := UpdateAvailableVolumeTx(tx, order.SecurityID); err != nil {
				fmt.Printf("Greska pri UpdateAvailableVolume: %v\n", err)
				tx.Rollback()
				break
			}

			//// SKIDANJE unita ako je kupovina (smanjuje se dostupnost hartija)
			//if order.Direction == "buy" {
			//	var security types.Security
			//	if err := tx.First(&security, order.SecurityID).Error; err == nil {
			//		security.Volume -= int64(matchQuantity)
			//		if security.Volume < 0 {
			//			security.Volume = 0
			//		}
			//		tx.Save(&security)
			//	}
			//}

			//// Ažuriraj RemainingParts u bazi (bez is_done/status!)
			//if err := tx.Model(&types.Order{}).Where("id = ?", order.ID).Update("remaining_parts", *order.RemainingParts).Error; err != nil {
			//	fmt.Printf("Greska pri upisu remaining_parts: %v\n", err)
			//	tx.Rollback()
			//	break
			//}

			if err := tx.Commit().Error; err != nil {
				fmt.Printf("Nalog %v nije izvršen: %v\n", order.ID, err)
				tx.Rollback()
				break
			}

			// Refetch ponovo da zna koliko još ima
			if err := db.DB.First(&order, order.ID).Error; err != nil {
				fmt.Printf("Greska pri refetch ordera %d nakon commit-a: %v\n", order.ID, err)
				break
			}

			if order.RemainingParts == nil {
				fmt.Printf("RemainingParts je NIL nakon commita, orderID = %d\n", order.ID)
			} else {
				fmt.Printf("Order %d refetch: remaining = %d\n", order.ID, *order.RemainingParts)
			}

			if order.RemainingParts == nil || *order.RemainingParts <= 0 {
				fmt.Printf("Order %d je već izvršen ili nema više delova za obradu\n", order.ID)
				break
			}

			fmt.Printf("Pauza pre sledećeg pokušaja za Order %d\n", order.ID)
			delay := calculateDelay(order)
			time.Sleep(delay)
		}

		// Konačna provera na kraju svih mečeva
		if order.RemainingParts != nil && *order.RemainingParts == 0 {
			db.DB.Model(&types.Order{}).Where("id = ?", order.ID).Updates(map[string]interface{}{
				"is_done": true,
				"status":  "done",
			})
			fmt.Printf("Order %d označen kao završen nakon svih mečeva\n", order.ID)
		} else {
			fmt.Printf("Order %d ostaje neizvršen | Remaining: %d\n", order.ID, *order.RemainingParts)
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

func executePartial(order1 *types.Order, price float64, tx *gorm.DB) int {
	lock := getLock(order1.SecurityID)
	lock.Lock()
	defer lock.Unlock()

	if order1.Status == "done" || order1.RemainingParts == nil || *order1.RemainingParts <= 0 {
		fmt.Printf("Order %d je već završen ili nema remaining parts\n", order1.ID)
		return 0
	}

	direction := "buy"
	if strings.ToLower(order1.Direction) == "buy" {
		direction = "sell"
	} else {
		direction = "buy"
	}

	fmt.Printf("Pokušavam da pronađem match za Order %d...\n", order1.ID)

	var matches []types.Order

	err := tx.Debug().
		Model(&types.Order{}).
		Where("security_id = ? AND direction = ? AND user_id != ? AND status = 'approved' AND NOT is_done",
			order1.SecurityID, direction, order1.UserID).
		Order("last_modified").
		Find(&matches).Error

	if err != nil {
		fmt.Printf("Neuspelo dohvatanje matching order-a: %v", err)
		return 0
	}
	if order1.AON {
		totalAvailable := 0
		selectedMatches := []types.Order{}

		for _, match := range matches {
			if match.RemainingParts == nil || *match.RemainingParts <= 0 {
				continue
			}
			if match.AccountID == 0 {
				continue
			}
			totalAvailable += *match.RemainingParts
			selectedMatches = append(selectedMatches, match)
			if totalAvailable >= *order1.RemainingParts {
				break
			}
		}

		if totalAvailable < *order1.RemainingParts {
			fmt.Println("Nema dovoljno available matches za AON order", order1.ID)
			return 0
		}

		fmt.Printf("Pronađeno dovoljno match-eva za AON order %d\n", order1.ID)

		matchQty := *order1.RemainingParts
		remainingToFill := matchQty

		for _, match := range selectedMatches {
			currentMatchQty := min(ptrSafe(match.RemainingParts), remainingToFill)

			err := tx.Debug().Transaction(func(tx *gorm.DB) error {
				txn := types.Transaction{
					OrderID:      order1.ID,
					BuyerID:      getBuyerID(*order1, match),
					SellerID:     getSellerID(*order1, match),
					SecurityID:   order1.SecurityID,
					Quantity:     currentMatchQty,
					PricePerUnit: price,
					TotalPrice:   price * float64(currentMatchQty),
				}
				if err := tx.Debug().Create(&txn).Error; err != nil {
					fmt.Printf("Greska pri kreiranju transakcije: %v\n", err)
					return err
				}

				if match.RemainingParts == nil {
					tmp := match.Quantity
					match.RemainingParts = &tmp
				}
				*match.RemainingParts -= currentMatchQty

				if *match.RemainingParts == 0 {
					match.IsDone = true
					match.Status = "done"
				}

				if err := tx.Save(&match).Error; err != nil {
					return err
				}

				if err := updatePortfolio(getBuyerID(*order1, match), order1.SecurityID, currentMatchQty, price, tx); err != nil {
					return err
				}

				if err := updatePortfolio(getSellerID(*order1, match), order1.SecurityID, -currentMatchQty, price, tx); err != nil {
					return err
				}

				if isAgent(getBuyerID(*order1, match)) {
					var actuary types.Actuary
					if err := tx.Where("user_id = ?", order1.UserID).First(&actuary).Error; err == nil {
						initialMargin := price * float64(matchQty)
						actuary.UsedLimit += initialMargin
						if err := tx.Save(&actuary).Error; err != nil {
							fmt.Printf("Greska pri save UsedLimit za order agenta: %v\n", err)
						} else {
							fmt.Printf("Agent order.UserID=%d - povećan UsedLimit za %.2f\n", order1.UserID, initialMargin)
						}
					}
				}

				uid := fmt.Sprintf("ORDER-match-%d-%d", order1.ID, time.Now().UnixNano())
				total := price * float64(currentMatchQty)
				fee := CalculateFee(*order1, total)
				initiationDto := dto.OrderTransactionInitiationDTO{
					Uid:             uid,
					SellerAccountId: getSellerAccountID(*order1, match),
					BuyerAccountId:  getBuyerAccountID(*order1, match),
					Amount:          total,
					Fee:             fee,
					Direction:       order1.Direction,
				}

				fmt.Println("Šaljem OrderTransactionInitiationDTO za svakog seller-a...")
				fmt.Printf("Order Transaction Init: BuyerAccountID=%d, SellerAccountID=%d, Amount=%.2f, Fee=%.2f\n",
					initiationDto.BuyerAccountId, initiationDto.SellerAccountId, initiationDto.Amount, initiationDto.Fee)

				err := broker.SendOrderTransactionInit(&initiationDto)
				if err != nil {
					fmt.Printf("Greska pri slanju OrderTransactionInitiationDTO: %v\n", err)
					return err
				}

				return nil
			})

			if err != nil {
				fmt.Printf("Greška pri izvršavanju transakcije za match: %v\n", err)
				continue
			}

			remainingToFill -= currentMatchQty
			if remainingToFill <= 0 {
				break
			}
		}

		if order1.RemainingParts == nil {
			tmp := order1.Quantity
			order1.RemainingParts = &tmp
		}
		*order1.RemainingParts = 0

		if err := tx.Model(&types.Order{}).
			Where("id = ?", order1.ID).
			Updates(map[string]interface{}{
				"remaining_parts": 0,
				"is_done":         true,
				"status":          "done",
			}).Error; err != nil {
			fmt.Printf("Greska pri upisu remaining_parts za AON order: %v\n", err)
		}

		return matchQty
	} else {
		for _, match := range matches {
			order := *order1
			order.RemainingParts = ptr(*order.RemainingParts)

			if match.AccountID == 0 {
				fmt.Println("Matchovani order ima account_id = 0, preskačem ga...")
				continue
			}
			// Po specifikaciji, MARKET BUY ne proverava matchov limit
			if !(strings.ToUpper(order.OrderType) == "MARKET" && strings.ToLower(order.Direction) == "buy") {
				if !canPreExecute(match) {
					fmt.Println("Preskočen match sa nedovoljnim uslovima")
					continue
				}
			}

			marginOrder := order
			if !order.Margin && match.Margin == true {
				marginOrder = match
			}

			if marginOrder.Margin {
				var actuary types.Actuary
				if err := tx.Where("user_id = ?", marginOrder.UserID).First(&actuary).Error; err != nil {
					fmt.Println("Matchovani margin order nema validnog aktuara")
					continue
				}
				if marginOrder.RemainingParts == nil {
					fmt.Println("RemainingParts je nil u margin logici")
					continue
				}
				initialMargin := price * float64(*marginOrder.RemainingParts) * 0.3 * 1.1
				if actuary.LimitAmount-actuary.UsedLimit < initialMargin {
					fmt.Println("Matchovani margin order nema dovoljno limita")
					continue
				}
			}

			matchQty := min(
				ptrSafe(order.RemainingParts),
				ptrSafe(match.RemainingParts),
			)

			if matchQty <= 0 {
				fmt.Printf("Nevalidan matchQty = %d za Order %d\n", matchQty, order.ID)
				continue
			}

			err = tx.Debug().Transaction(func(tx *gorm.DB) error {
				txn := types.Transaction{
					OrderID:      order.ID,
					BuyerID:      getBuyerID(order, match),
					SellerID:     getSellerID(order, match),
					SecurityID:   order.SecurityID,
					Quantity:     matchQty,
					PricePerUnit: price,
					TotalPrice:   price * float64(matchQty),
				}
				if err := tx.Debug().Create(&txn).Error; err != nil {
					fmt.Printf("Greska pri kreiranju transakcije: %v\n", err)
					return err
				}

				if order.RemainingParts == nil {
					tmp := order.Quantity
					order.RemainingParts = &tmp
				}
				*order.RemainingParts -= matchQty

				if match.RemainingParts == nil {
					tmp := match.Quantity
					match.RemainingParts = &tmp
				}
				*match.RemainingParts -= matchQty

				if *match.RemainingParts == 0 {
					match.IsDone = true
					match.Status = "done"
				}

				if err := tx.Save(&order).Error; err != nil {
					fmt.Printf("Greska pri save za order: %v\n", err)
					return err
				}
				if err := tx.Save(&match).Error; err != nil {
					fmt.Printf("Greska pri save za match: %v\n", err)
					return err
				}

				if err := updatePortfolio(getBuyerID(order, match), order.SecurityID, matchQty, price, tx); err != nil {
					fmt.Printf("Greska pri updatePortfolio za buyer-a: %v\n", err)
					return err
				}

				if err := updatePortfolio(getSellerID(order, match), order.SecurityID, -matchQty, price, tx); err != nil {
					fmt.Printf("Greska pri updatePortfolio za seller-a: %v\n", err)
					return err
				}

				if order.Margin {
					var actuary types.Actuary
					if err := tx.Where("user_id = ?", order.UserID).First(&actuary).Error; err == nil {
						initialMargin := price * float64(matchQty) * 0.3 * 1.1
						actuary.UsedLimit += initialMargin
						tx.Save(&actuary)
					}
				}

				if isAgent(getBuyerID(*order1, match)) {
					var actuary types.Actuary
					if err := tx.Where("user_id = ?", order.UserID).First(&actuary).Error; err == nil {
						initialMargin := price * float64(matchQty)
						actuary.UsedLimit += initialMargin
						if err := tx.Save(&actuary).Error; err != nil {
							fmt.Printf("Greska pri save UsedLimit za order agenta: %v\n", err)
						} else {
							fmt.Printf("Agent order.UserID=%d - povećan UsedLimit za %.2f\n", order.UserID, initialMargin)
						}
					}
				}

				uid := fmt.Sprintf("ORDER-match-%d-%d", order.ID, time.Now().Unix())
				total := price * float64(matchQty)
				fee := CalculateFee(order, total)
				initiationDto := dto.OrderTransactionInitiationDTO{
					Uid:             uid,
					SellerAccountId: getSellerAccountID(order, match),
					BuyerAccountId:  getBuyerAccountID(order, match),
					Amount:          total,
					Fee:             fee,
					Direction:       order.Direction,
				}

				fmt.Println("Šaljem OrderTransactionInitiationDTO preko brokera...")

				err := broker.SendOrderTransactionInit(&initiationDto)
				if err != nil {
					fmt.Printf("Greska pri slanju OrderTransactionInitiationDTO preko brokera: %v\n", err)
					return err
				}

				return nil
			})

			if err != nil {
				continue
			}

			fmt.Printf("Match success: Order %d ↔ Order %d za %d @ %.2f\n", order.ID, match.ID, matchQty, price)
			*order1 = order
			return matchQty
		}
	}
	return 0
}

func updatePortfolio(userID uint, securityID uint, delta int, price float64, tx *gorm.DB) error {
	var portfolio types.Portfolio
	err := tx.Where("user_id = ? AND security_id = ?", userID, securityID).First(&portfolio).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		if delta > 0 {
			portfolio = types.Portfolio{
				UserID:        userID,
				SecurityID:    securityID,
				Quantity:      delta,
				PurchasePrice: price,
			}
			if err := tx.Create(&portfolio).Error; err != nil {
				return fmt.Errorf("Portfolio greška u create: user=%d, security=%d, delta=%d | %w\n", userID, securityID, delta, err)
			} else {
				fmt.Printf("Portfolio kreiran: user=%d, security=%d, quantity=%d\n", userID, securityID, delta)
			}
		} else {
			return fmt.Errorf("Nema postojeći portfolio za korisnika %d i security %d, a pokušaj da se oduzme delta=%d\n", userID, securityID, delta)
		}
		return nil
	}

	if err != nil {
		fmt.Printf("Greska pri dohvatanju portfolia: user=%d, security=%d | %v\n", userID, securityID, err)
		return err
	}

	portfolio.Quantity += delta
	if portfolio.Quantity <= 0 {
		err = tx.Delete(&portfolio).Error
		if err != nil {
			fmt.Printf("Portfolio greška pri brisanju: user=%d, security=%d | %v\n", userID, securityID, err)
		} else {
			fmt.Printf("Portfolio obrisan: user=%d, security=%d\n", userID, securityID)
		}
	} else {
		err = tx.Save(&portfolio).Error
		if err != nil {
			fmt.Printf("Portfolio greška pri update: user=%d, security=%d | %v\n", userID, securityID, err)
		} else {
			fmt.Printf("Portfolio ažuriran: user=%d, security=%d, quantity=%d\n", userID, securityID, portfolio.Quantity)
		}
	}
	return err
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
	if strings.ToLower(order.Direction) == "buy" {
		direction = "sell"
	} else {
		direction = "buy"
	}

	db.DB.Where("security_id = ? AND direction = ? AND status = ? AND is_done = ?", order.SecurityID, direction, "approved", false).Find(&matchingOrders)
	totalAvailable := 0
	for _, o := range matchingOrders {
		if o.RemainingParts != nil && canPreExecute(o) {
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
	if !IsSettlementDateValid(&order) {
		return false
	}

	if strings.ToUpper(order.OrderType) == "LIMIT" {
		if order.LimitPricePerUnit == nil {
			fmt.Println("LIMIT order bez LimitPricePerUnit")
			return false
		}
		price := getListingPrice(order)
		if price < 0 {
			return false
		}
		if order.Direction == "sell" {
			return price >= *order.LimitPricePerUnit
		} else {
			return price <= *order.LimitPricePerUnit
		}
	} else if strings.ToUpper(order.OrderType) == "STOP" {
		if order.StopPricePerUnit == nil {
			fmt.Println("STOP order bez StopPricePerUnit")
			return false
		}
		price := getListingPrice(order)
		if price < 0 {
			return false
		}
		if order.Direction == "sell" {
			return price <= *order.StopPricePerUnit
		} else {
			return price >= *order.StopPricePerUnit
		}
	} else if strings.ToUpper(order.OrderType) == "STOP-LIMIT" {
		if order.LimitPricePerUnit == nil || order.StopPricePerUnit == nil {
			fmt.Println("STOP-LIMIT order bez neophodnih cena")
			return false
		}
		price := getListingPrice(order)
		if price < 0 {
			return false
		}
		if order.Direction == "sell" {
			return price <= *order.StopPricePerUnit && price >= *order.LimitPricePerUnit
		} else {
			return price >= *order.StopPricePerUnit && price <= *order.LimitPricePerUnit
		}
	} else if strings.ToUpper(order.OrderType) == "MARKET" {
		return true
	}
	return true
}

func getBuyerID(a, b types.Order) uint {
	if strings.ToLower(a.Direction) == "buy" {
		return a.UserID
	}
	return b.UserID
}

func getSellerID(a, b types.Order) uint {
	if strings.ToLower(a.Direction) == "sell" {
		return a.UserID
	}
	return b.UserID
}

func ptrSafe(ptr *int) int {
	if ptr == nil {
		return 0
	}
	return *ptr
}

func IsSettlementDateValid(order *types.Order) bool {
	if order.Security.ID == 0 {
		var security types.Security
		if err := db.DB.First(&security, order.SecurityID).Error; err != nil {
			fmt.Printf("Nije pronađena hartija %d za order %d: %v\n", order.SecurityID, order.ID, err)
			return false
		}
		order.Security = security
	}

	if order.Security.SettlementDate != nil {
		parsed, err := time.Parse("2006-01-02", *order.Security.SettlementDate)
		if err != nil {
			fmt.Printf("Nevalidan settlementDate za hartiju %d: %v\n", order.Security.ID, err)
			return false
		}

		// Poredi samo po danima, ne po satu
		now := time.Now().Truncate(24 * time.Hour)
		parsed = parsed.Truncate(24 * time.Hour)

		if parsed.Before(now) {
			fmt.Printf("Hartiji %d je istekao settlementDate: %s\n", order.Security.ID, *order.Security.SettlementDate)
			return false
		}
	}

	return true
}

func UpdateAvailableVolume(securityID uint) error {
	return UpdateAvailableVolumeTx(db.DB, securityID)
}

func UpdateAvailableVolumeTx(tx *gorm.DB, securityID uint) error {
	var total sql.NullInt64

	// Direktno koristi RAW SQL da izbegnemo GORM probleme sa pointerima i imenovanjem
	query := `
		SELECT SUM(remaining_parts)
		FROM "order"
		WHERE security_id = ?
		  AND lower(direction) = 'sell'
		  AND lower(status) = 'approved'
		  AND COALESCE(is_done, false) = false
	`

	err := tx.Raw(query, securityID).Scan(&total).Error
	if err != nil {
		return fmt.Errorf("greska pri izvrsavanju SUM upita: %w", err)
	}

	final := int64(0)
	if total.Valid {
		final = total.Int64
	}

	// Ažuriraj volume u security tabeli
	return tx.Model(&types.Security{}).
		Where("id = ?", securityID).
		Update("volume", final).Error
}

func getBuyerAccountID(a, b types.Order) uint {
	buyerID := a.UserID
	buyerAccountID := a.AccountID
	if strings.ToLower(a.Direction) != "buy" {
		buyerID = b.UserID
		buyerAccountID = b.AccountID
	}

	if isAgent(buyerID) {
		fmt.Println("Buyer je agent, preusmeravam BuyerAccountId na (bankovni racun)")
		return 112
	}

	return buyerAccountID
}

func getSellerAccountID(a, b types.Order) uint {
	sellerID := b.UserID
	sellerAccountID := b.AccountID
	if strings.ToLower(a.Direction) == "sell" {
		sellerID = a.UserID
		sellerAccountID = a.AccountID
	}

	if isAgent(sellerID) {
		fmt.Println("Seller je agent, preusmeravam SellerAccountId na (bankovni racun)")
		return 112
	}

	return sellerAccountID
}

func CanSell(userID, securityID uint, requestedQty int) (bool, int, error) {
	var sec types.Security
	if err := db.DB.First(&sec, securityID).Error; err != nil {
		return false, 0, err
	}

	var portfolio types.Portfolio
	if err := db.DB.Where("user_id = ? AND security_id = ?", userID, securityID).First(&portfolio).Error; err != nil {
		return false, 0, err
	}

	// Suma svih već POSTOJEĆIH neizvršenih SELL naloga
	var reserved int64
	err := db.DB.Model(&types.Order{}).
		Select("COALESCE(SUM(remaining_parts), 0)").
		Where("user_id = ? AND security_id = ? AND lower(direction) = 'sell' AND lower(status) = 'approved' AND COALESCE(is_done, false) = false", userID, securityID).
		Scan(&reserved).Error
	if err != nil {
		return false, 0, err
	}

	// Izračunaj slobodno dostupne privatne hartije
	available := portfolio.Quantity - portfolio.PublicCount - int(reserved)

	if requestedQty > available {
		return false, available, nil
	}
	return true, available, nil
}

func isAgent(userID uint) bool {
	var actuary types.Actuary
	if err := db.DB.Where("user_id = ?", userID).First(&actuary).Error; err != nil {
		return false
	}
	return strings.ToLower(actuary.Department) == "agent"
}
