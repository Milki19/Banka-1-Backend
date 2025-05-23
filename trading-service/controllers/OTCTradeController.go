package controllers

import (
	"banka1.com/broker"
	"banka1.com/db"
	"banka1.com/dto"
	"banka1.com/middlewares"
	"banka1.com/saga"
	"banka1.com/types"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type UpdateOTCTradeRequest struct {
	Quantity       int     `json:"quantity" validate:"required,gt=0"`
	PricePerUnit   float64 `json:"price_per_unit" validate:"required,gt=0"`
	Premium        float64 `json:"premium" validate:"required,gte=0"`
	SettlementDate string  `json:"settlement_date" validate:"required"`
}

type OTCTradeController struct {
	validator *validator.Validate
}

func NewOTCTradeController() *OTCTradeController {
	return &OTCTradeController{
		validator: validator.New(),
	}
}

type CreateOTCTradeRequest struct {
	OwnerID        string  `json:"ownerId" validate:"required"`
	PortfolioID    *uint   `json:"portfolioId,omitempty"`
	Ticker         *string `json:"ticker,omitempty"`
	Quantity       int     `json:"quantity"     validate:"required,gt=0"`
	PricePerUnit   float64 `json:"pricePerUnit" validate:"required,gt=0"`
	Premium        float64 `json:"premium"      validate:"required,gte=0"`
	SettlementDate string  `json:"settlementDate" validate:"required"`
}

func (c *OTCTradeController) CreateOTCTrade(ctx *fiber.Ctx) error {
	var req CreateOTCTradeRequest
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{false, "", "Nevalidan JSON format"})
	}
	if err := c.validator.Struct(req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{false, "", err.Error()})
	}
	settlementDate, err := time.Parse("2006-01-02", req.SettlementDate)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{false, "", "Nevalidan format datuma, očekivano YYYY-MM-DD"})
	}
	localUserID := uint(ctx.Locals("user_id").(float64))
	localUserIDStr := strconv.FormatUint(uint64(localUserID), 10)

	if req.PortfolioID != nil {
		var portfolio types.Portfolio
		if err := db.DB.Preload("Security").First(&portfolio, *req.PortfolioID).Error; err != nil {
			return ctx.Status(404).JSON(types.Response{false, "", "Portfolio nije pronađen"})
		}
		if portfolio.UserID == localUserID {
			return ctx.Status(403).JSON(types.Response{false, "", "Ne možete praviti ponudu za svoje akcije"})
		}
		if portfolio.PublicCount < req.Quantity {
			return ctx.Status(400).JSON(types.Response{false, "", "Nedovoljno javno dostupnih akcija"})
		}
		trade := types.OTCTrade{
			PortfolioID:   req.PortfolioID,
			SecurityID:    &portfolio.SecurityID,
			LocalSellerID: &portfolio.UserID,
			LocalBuyerID:  &localUserID,
			Ticker:        portfolio.Security.Ticker,
			Quantity:      req.Quantity,
			PricePerUnit:  req.PricePerUnit,
			Premium:       req.Premium,
			SettlementAt:  settlementDate,
			ModifiedBy:    localUserIDStr,
			Status:        "pending",
		}
		if err := db.DB.Create(&trade).Error; err != nil {
			return ctx.Status(500).JSON(types.Response{false, "", "Greška pri čuvanju ponude"})
		}
		return ctx.Status(201).JSON(types.Response{true, fmt.Sprintf("Interna ponuda kreirana: %d", trade.ID), ""})
	}

	if req.Ticker == nil {
		return ctx.Status(400).JSON(types.Response{false, "", "Ticker je obavezan za međubankarsku ponudu"})
	}
	prefix := req.OwnerID[:3]
	foreignID := req.OwnerID[3:]
	routingNum, err := strconv.Atoi(prefix)
	if err != nil {
		return ctx.Status(400).JSON(types.Response{false, "", "Neispravan ownerId format"})
	}

	ibReq := CreateInterbankOTCOfferRequest{
		Ticker:         *req.Ticker,
		Quantity:       req.Quantity,
		PricePerUnit:   req.PricePerUnit,
		Premium:        req.Premium,
		SettlementDate: req.SettlementDate,
		SellerRouting:  routingNum,
		SellerID:       foreignID,
	}

	if err := c.validator.Struct(ibReq); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{false, "", err.Error()})
	}

	const myRouting = 111
	offerDTO := dto.InterbankOtcOfferDTO{
		Stock:          dto.StockDescription{Ticker: ibReq.Ticker},
		SettlementDate: settlementDate.Format(time.RFC3339),
		PricePerUnit:   dto.MonetaryValue{Currency: "USD", Amount: ibReq.PricePerUnit},
		Premium:        dto.MonetaryValue{Currency: "USD", Amount: ibReq.Premium},
		Amount:         ibReq.Quantity,
		BuyerID:        dto.ForeignBankId{RoutingNumber: myRouting, ID: localUserIDStr},
		SellerID:       dto.ForeignBankId{RoutingNumber: ibReq.SellerRouting, ID: ibReq.SellerID},
		LastModifiedBy: dto.ForeignBankId{RoutingNumber: myRouting, ID: localUserIDStr},
	}

	url := fmt.Sprintf("%s/negotiations", os.Getenv("BANK4_BASE_URL"))
	bodyBytes, err := json.Marshal(offerDTO)
	if err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Greška pri serializaciji zahteva"})
	}

	bodyReader := bytes.NewReader(bodyBytes)

	httpReq, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Greška pri kreiranju HTTP zahteva"})
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Api-Key", os.Getenv("BANK4_API_KEY"))

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return ctx.Status(502).JSON(types.Response{false, "", "Greška pri komunikaciji sa Bankom 4"})
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return ctx.Status(resp.StatusCode).JSON(types.Response{false, fmt.Sprintf("Bank4: %s", string(body)), ""})
	}

	var fbid dto.ForeignBankId
	if err := json.NewDecoder(resp.Body).Decode(&fbid); err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Neuspešno parsiranje odgovora Banke 4"})
	}

	modifiedBy := fmt.Sprintf("%d%s", myRouting, localUserIDStr)

	trade := types.OTCTrade{
		RemoteRoutingNumber: &fbid.RoutingNumber,
		RemoteNegotiationID: &fbid.ID,
		RemoteSellerID:      &ibReq.SellerID,
		RemoteBuyerID:       &localUserIDStr,
		Ticker:              ibReq.Ticker,
		Quantity:            ibReq.Quantity,
		PricePerUnit:        ibReq.PricePerUnit,
		Premium:             ibReq.Premium,
		SettlementAt:        settlementDate,
		ModifiedBy:          modifiedBy,
		Status:              "pending",
	}
	if err := db.DB.Create(&trade).Error; err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Greška pri čuvanju međubankarske ponude"})
	}

	return ctx.Status(201).JSON(types.Response{
		Success: true,
		Data:    fmt.Sprintf("Interbank ponuda kreirana, negoID=%s", fbid.ID),
	})
}

func (c *OTCTradeController) CounterOfferOTCTrade(ctx *fiber.Ctx) error {
	id := ctx.Params("id")
	var req UpdateOTCTradeRequest
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).
			JSON(types.Response{Success: false, Data: "", Error: "Nevalidan JSON format"})
	}
	if err := c.validator.Struct(req); err != nil {
		return ctx.Status(fiber.StatusBadRequest).
			JSON(types.Response{Success: false, Data: "", Error: err.Error()})
	}

	settlementDate, err := time.Parse("2006-01-02", req.SettlementDate)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).
			JSON(types.Response{Success: false, Data: "", Error: "Nevalidan format datuma. Očekivano YYYY-MM-DD"})
	}

	localUserID := uint(ctx.Locals("user_id").(float64))
	localUserIDStr := strconv.FormatUint(uint64(localUserID), 10)

	var trade types.OTCTrade
	if err := db.DB.Preload("Portfolio").First(&trade, id).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).
			JSON(types.Response{Success: false, Data: "", Error: "Ponuda nije pronađena"})
	}

	if trade.ModifiedBy == localUserIDStr {
		return ctx.Status(fiber.StatusForbidden).
			JSON(types.Response{Success: false, Data: "", Error: "Ne možete uzastopno menjati ponudu"})
	}

	if trade.RemoteNegotiationID == nil {
		if trade.PortfolioID == nil {
			return ctx.Status(fiber.StatusBadRequest).
				JSON(types.Response{Success: false, Data: "", Error: "Interna greška: nema PortfolioID"})
		}
		var portfolio types.Portfolio
		if err := db.DB.First(&portfolio, *trade.PortfolioID).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).
				JSON(types.Response{Success: false, Data: "", Error: "Greška pri proveri portfolija"})
		}
		if portfolio.PublicCount < req.Quantity {
			return ctx.Status(fiber.StatusBadRequest).
				JSON(types.Response{Success: false, Data: "", Error: "Nedovoljno javno dostupnih akcija"})
		}
		trade.Quantity = req.Quantity
		trade.PricePerUnit = req.PricePerUnit
		trade.Premium = req.Premium
		trade.SettlementAt = settlementDate
		trade.LastModified = time.Now().Unix()
		trade.ModifiedBy = localUserIDStr
		trade.Status = "pending"

		if err := db.DB.Save(&trade).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).
				JSON(types.Response{Success: false, Data: "", Error: "Greška prilikom čuvanja kontraponude"})
		}
		return ctx.Status(fiber.StatusOK).
			JSON(types.Response{Success: true, Data: fmt.Sprintf("Kontraponuda uspešno poslata (interna): %d", trade.ID), Error: ""})
	}

	const myRouting = 111

	var buyerFB, sellerFB dto.ForeignBankId
	if trade.RemoteBuyerID != nil && *trade.RemoteBuyerID == fmt.Sprintf("%d%s", myRouting, localUserIDStr) {
		buyerFB = dto.ForeignBankId{RoutingNumber: myRouting, ID: localUserIDStr}
		sellerFB = dto.ForeignBankId{RoutingNumber: *trade.RemoteRoutingNumber, ID: *trade.RemoteSellerID}
	} else if trade.RemoteSellerID != nil && *trade.RemoteSellerID == fmt.Sprintf("%d%s", myRouting, localUserIDStr) {
		buyerFB = dto.ForeignBankId{RoutingNumber: *trade.RemoteRoutingNumber, ID: *trade.RemoteBuyerID}
		sellerFB = dto.ForeignBankId{RoutingNumber: myRouting, ID: localUserIDStr}
	} else {
		return ctx.Status(fiber.StatusForbidden).
			JSON(types.Response{Success: false, Data: "", Error: "Niste učesnik ove međubankarske ponude"})
	}
	//STAVLJENO OVDE SAMO RADI TESTIRANJA,ODKOMENTARISATI ISPOD NA PRAVOM MESTU I OBRISATI OVO KADA SVE BUDE GOTOVO
	modifiedBy := fmt.Sprintf("%d%s", myRouting, localUserIDStr)
	trade.Quantity = req.Quantity
	trade.PricePerUnit = req.PricePerUnit
	trade.Premium = req.Premium
	trade.SettlementAt = settlementDate
	trade.LastModified = time.Now().Unix()
	trade.ModifiedBy = modifiedBy
	trade.Status = "pending"

	if err := db.DB.Save(&trade).Error; err != nil {
		return ctx.Status(500).
			JSON(types.Response{Success: false, Data: "", Error: "Greška pri čuvanju međubankarske kontraponude"})
	}

	offer := dto.InterbankOtcOfferDTO{
		Stock:          dto.StockDescription{Ticker: trade.Ticker},
		SettlementDate: settlementDate.Format(time.RFC3339),
		PricePerUnit:   dto.MonetaryValue{Currency: "USD", Amount: req.PricePerUnit},
		Premium:        dto.MonetaryValue{Currency: "USD", Amount: req.Premium},
		Amount:         req.Quantity,
		BuyerID:        buyerFB,
		SellerID:       sellerFB,
		LastModifiedBy: dto.ForeignBankId{RoutingNumber: myRouting, ID: localUserIDStr},
	}

	body, err := json.Marshal(offer)
	if err != nil {
		return ctx.Status(500).
			JSON(types.Response{Success: false, Data: "", Error: "Greška pri serializaciji zahteva"})
	}

	url := fmt.Sprintf("%s/negotiations/%d/%s",
		os.Getenv("BANK4_BASE_URL"),
		444,
		*trade.RemoteNegotiationID,
	)
	httpReq, _ := http.NewRequest("PUT", url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Api-Key", os.Getenv("BANK4_API_KEY"))

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return ctx.Status(502).
			JSON(types.Response{Success: false, Data: "", Error: "Greška pri komunikaciji sa Bankom 4"})
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return ctx.Status(409).
			JSON(types.Response{Success: false, Data: "", Error: "Nije vaš red za kontra‑ponudu."})
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return ctx.Status(resp.StatusCode).
			JSON(types.Response{Success: false, Data: "", Error: fmt.Sprintf("Bank4: %s", string(respBody))})
	}

	//trade.Quantity = req.Quantity
	//trade.PricePerUnit = req.PricePerUnit
	//trade.Premium = req.Premium
	//trade.SettlementAt = settlementDate
	//trade.LastModified = time.Now().Unix()
	//trade.ModifiedBy = localUserIDStr
	//trade.Status = "pending"
	//
	//if err := db.DB.Save(&trade).Error; err != nil {
	//	return ctx.Status(500).
	//		JSON(types.Response{Success: false, Data: "", Error: "Greška pri čuvanju međubankarske kontraponude"})
	//}

	return ctx.Status(fiber.StatusOK).
		JSON(types.Response{Success: true, Data: fmt.Sprintf("Interbank kontraponuda poslata za međubankarski negotioationID: %d", trade.RemoteNegotiationID), Error: ""})
}

func parseUintPtr(s *string) (uint, error) {
	if s == nil {
		return 0, errors.New("empty")
	}
	v, err := strconv.ParseUint(*s, 10, 64)
	return uint(v), err
}

func parseSellerID(t *types.OTCTrade) (uint, error) {
	if t.LocalSellerID != nil {
		return *t.LocalSellerID, nil
	}
	return parseUintPtr(t.RemoteSellerID)
}

func parseBuyerID(t *types.OTCTrade) (uint, error) {
	if t.LocalBuyerID != nil {
		return *t.LocalBuyerID, nil
	}
	return parseUintPtr(t.RemoteBuyerID)
}

func (c *OTCTradeController) AcceptOTCTrade(ctx *fiber.Ctx) error {
	id := ctx.Params("id")
	userID := uint(ctx.Locals("user_id").(float64))

	var trade types.OTCTrade
	if err := db.DB.Preload("Portfolio").First(&trade, id).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(types.Response{false, "", "Ponuda nije pronađena"})
	}

	if trade.ModifiedBy == fmt.Sprintf("%d", userID) {
		return ctx.Status(fiber.StatusForbidden).JSON(types.Response{
			Success: false,
			Error:   "Ne možete uzastopno menjati ponudu",
		})
	}

	if trade.Status == "accepted" || trade.Status == "executed" {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{false, "", "Ova ponuda je već prihvaćena ili realizovana"})
	}

	if trade.RemoteNegotiationID == nil {
		var portfolio types.Portfolio
		if err := db.DB.First(&portfolio, trade.PortfolioID).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri dohvatanju portfolija",
			})
		}

		var existingContracts []types.OptionContract
		if err := db.DB.
			Where("seller_id = ? AND portfolio_id = ? AND is_exercised = false AND status = ?",
				trade.LocalSellerID, portfolio.ID, "active").
			Find(&existingContracts).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri proveri postojećih ugovora",
			})
		}

		usedQuantity := 0
		for _, contract := range existingContracts {
			usedQuantity += contract.Quantity
		}

		if usedQuantity+trade.Quantity > portfolio.PublicCount {
			return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
				Success: false,
				Error:   "Nemate dovoljno raspoloživih akcija za prihvatanje ove ponude",
			})
		}

		trade.Status = "accepted"
		trade.LastModified = time.Now().Unix()
		trade.ModifiedBy = strconv.FormatUint(uint64(userID), 10)
		if err := db.DB.Save(&trade).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri ažuriranju ponude",
			})
		}

		contract := types.OptionContract{
			OTCTradeID:   trade.ID,
			BuyerID:      trade.LocalBuyerID,
			SellerID:     trade.LocalSellerID,
			PortfolioID:  trade.PortfolioID,
			SecurityID:   trade.SecurityID,
			Quantity:     trade.Quantity,
			StrikePrice:  trade.PricePerUnit,
			Premium:      trade.Premium,
			SettlementAt: trade.SettlementAt,
			Status:       "active",
			CreatedAt:    time.Now().Unix(),
		}

		buyerID := int64(*contract.BuyerID)

		buyerAccounts, err := broker.GetAccountsForUser(buyerID)
		if err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Neuspešno dohvatanje računa kupca",
			})
		}

		sellerID := int64(*contract.SellerID)
		sellerAccounts, err := broker.GetAccountsForUser(sellerID)
		if err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Neuspešno dohvatanje računa prodavca",
			})
		}

		var buyerAccountID, sellerAccountID int64 = -1, -1

		for _, acc := range buyerAccounts {
			if acc.CurrencyType == "USD" {
				buyerAccountID = acc.ID
				break
			}
		}

		for _, acc := range sellerAccounts {
			if acc.CurrencyType == "USD" {
				sellerAccountID = acc.ID
				break
			}
		}

		if buyerAccountID == -1 || sellerAccountID == -1 {
			return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
				Success: false,
				Error:   "Kupac ili prodavac nema USD račun",
			})
		}

		var buyerAccount *dto.Account
		for _, acc := range buyerAccounts {
			if acc.ID == buyerAccountID {
				buyerAccount = &acc
				break
			}
		}
		if buyerAccount == nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri pronalaženju kupčevog računa",
			})
		}

		if buyerAccount.Balance < contract.Premium {
			return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
				Success: false,
				Error:   "Kupčev račun nema dovoljno sredstava za plaćanje premije",
			})
		}

		if err := db.DB.Create(&contract).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri kreiranju ugovora " + err.Error(),
			})
		}

		premiumDTO := &dto.OTCPremiumFeeDTO{
			BuyerAccountId:  uint(buyerAccountID),
			SellerAccountId: uint(sellerAccountID),
			Amount:          contract.Premium,
		}

		if err := broker.SendOTCPremium(premiumDTO); err != nil {
			_ = db.DB.Delete(&contract)
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri plaćanju premije",
			})
		}
		return ctx.Status(fiber.StatusOK).JSON(types.Response{true, fmt.Sprintf("Ponuda uspešno prihvaćena. Kreiran ugovor: %d", contract.ID), ""})
	}

	url := fmt.Sprintf("%s/negotiations/%d/%s/accept",
		os.Getenv("BANK4_BASE_URL"),
		444,
		*trade.RemoteNegotiationID,
	)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Greška pri kreiranju zahteva ka banci 4"})
	}
	req.Header.Set("X-Api-Key", os.Getenv("BANK4_API_KEY"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ctx.Status(502).JSON(types.Response{false, "", "Greška pri komunikaciji sa bankom 4"})
	}
	defer resp.Body.Close()

	if resp.StatusCode == 409 {
		return ctx.Status(409).JSON(types.Response{false, "", "Nije vaš red ili su pregovori zatvoreni"})
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		body, _ := io.ReadAll(resp.Body)
		return ctx.Status(resp.StatusCode).JSON(types.Response{false, "", fmt.Sprintf("Bank4: %s", string(body))})
	}

	var off struct {
		ID struct {
			RoutingNumber int    `json:"routingNumber"`
			ID            string `json:"id"`
		} `json:"id"`
		Stock struct {
			Ticker string `json:"ticker"`
		} `json:"stock"`
		PricePerUnit struct {
			Currency string  `json:"currency"`
			Amount   float64 `json:"amount"`
		} `json:"pricePerUnit"`
		SettlementDate string             `json:"settlementDate"`
		Amount         int                `json:"amount"`
		NegotiationId  *dto.ForeignBankId `json:"negotiationId,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&off); err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Neuspešno parsiranje odgovora banke4"})
	}

	trade.Status = "accepted"
	if trade.ModifiedBy == *trade.RemoteBuyerID {
		trade.ModifiedBy = *trade.RemoteSellerID
	} else {
		trade.ModifiedBy = *trade.RemoteBuyerID
	}
	if err := db.DB.Save(&trade).Error; err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Greška pri ažuriranju ponude"})
	}

	settleAt, err := time.Parse(time.RFC3339, off.SettlementDate)
	if err != nil {
		settleAt = trade.SettlementAt
	}
	contract := types.OptionContract{
		OTCTradeID:          trade.ID,
		RemoteContractID:    &off.ID.ID,
		RemoteBuyerID:       trade.RemoteBuyerID,
		RemoteSellerID:      trade.RemoteSellerID,
		Quantity:            off.Amount,
		StrikePrice:         off.PricePerUnit.Amount,
		RemoteNegotiationID: trade.RemoteNegotiationID,
		Premium:             trade.Premium,
		UID:                 off.ID.ID,
		SettlementAt:        settleAt,
		Status:              "active",
		CreatedAt:           time.Now().Unix(),
	}
	if err := db.DB.Create(&contract).Error; err != nil {
		return ctx.Status(500).JSON(types.Response{false, "", "Greška pri kreiranju ugovora"})
	}
	return ctx.Status(200).JSON(types.Response{true, "Interbank ponuda prihvaćena", ""})

}

func (c *OTCTradeController) ExecuteOptionContract(ctx *fiber.Ctx) error {
	id := ctx.Params("id")
	userID := uint(ctx.Locals("user_id").(float64))

	var contract types.OptionContract
	if err := db.DB.First(&contract, id).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(types.Response{
			Success: false,
			Error:   "Ugovor nije pronađen",
		})
	}

	if contract.BuyerID == nil || *contract.BuyerID != userID {
		return ctx.Status(fiber.StatusForbidden).JSON(types.Response{
			Success: false,
			Error:   "Nemate pravo da izvršite ovaj ugovor",
		})
	}

	if contract.IsExercised {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Ovaj ugovor je već iskorišćen",
		})
	}

	if contract.SettlementAt.Before(time.Now()) {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Ugovor je istekao",
		})
	}

	buyerID := int64(*contract.BuyerID)
	sellerID := int64(*contract.SellerID)

	buyerAccounts, err := broker.GetAccountsForUser(buyerID)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Neuspešno dohvatanje računa kupca",
		})
	}

	sellerAccounts, err := broker.GetAccountsForUser(sellerID)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Neuspešno dohvatanje računa prodavca",
		})
	}

	var sellerPortfolio types.Portfolio
	if err := db.DB.First(&sellerPortfolio, contract.PortfolioID).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Neuspešno dohvatanje portfolija",
		})
	}

	if contract.Quantity > sellerPortfolio.PublicCount {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Nema dovoljno raspoloživih akcija za izvršavanje ovog ugovora",
		})
	}

	var buyerAccountID, sellerAccountID int64 = -1, -1

	var buyerAccount *dto.Account
	for _, acc := range buyerAccounts {
		if acc.CurrencyType == "USD" {
			buyerAccountID = acc.ID
			buyerAccount = &acc
			break
		}
	}

	for _, acc := range sellerAccounts {
		if acc.CurrencyType == "USD" {
			sellerAccountID = acc.ID
			break
		}
	}

	if buyerAccountID == -1 || sellerAccountID == -1 {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Kupac ili prodavac nema USD račun",
		})
	}

	if buyerAccount.Balance < (contract.StrikePrice * float64(contract.Quantity)) {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Kupčev račun nema dovoljno sredstava za izvršavanje ugovora",
		})
	}

	uid := fmt.Sprintf("OTC-%d-%d", contract.ID, time.Now().Unix())

	dto := &types.OTCTransactionInitiationDTO{
		Uid:             uid,
		SellerAccountId: uint(sellerAccountID),
		BuyerAccountId:  uint(buyerAccountID),
		Amount:          contract.StrikePrice * float64(contract.Quantity),
	}

	contract.UID = uid

	if err := db.DB.Save(&contract).Error; err != nil {
		go broker.FailOTC(uid, "Greška prilikom čuvanja statusa ugovora")
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom čuvanja statusa ugovora",
		})
	}

	if err := db.DB.Model(&types.OTCTrade{}).Where("id = ?", contract.OTCTradeID).Update("status", "completed").Error; err != nil {
		go broker.FailOTC(uid, "Greška pri ažuriranju OTC ponude")
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška pri ažuriranju OTC ponude",
		})
	}

	if err := saga.StateManager.UpdatePhase(db.DB, uid, types.PhaseInit); err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom kreiranja OTC transakcije",
		})
	}

	if err := broker.SendOTCTransactionInit(dto); err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom slanja OTC transakcije",
		})
	}

	return ctx.Status(fiber.StatusOK).JSON(types.Response{
		Success: true,
		Data:    "Ugovor uspešno realizovan",
	})
}

func (c *OTCTradeController) GetActiveOffers(ctx *fiber.Ctx) error {
	userID := uint(ctx.Locals("user_id").(float64))
	userIDStr := strconv.FormatUint(uint64(userID), 10)

	const myRouting = 111
	composite := fmt.Sprintf("%d%s", myRouting, userIDStr)

	var trades []types.OTCTrade
	if err := db.DB.
		Preload("Portfolio.Security").
		Where("status = ?", "pending").
		Where(
			"(local_buyer_id  = ? OR local_seller_id  = ?) OR (remote_buyer_id = ? OR remote_seller_id = ?)",
			userID, userID,
			composite, composite,
		).
		Find(&trades).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom dohvatanja aktivnih ponuda",
		})
	}

	return ctx.JSON(types.Response{
		Success: true,
		Data:    trades,
	})
}

func (c *OTCTradeController) GetUserOptionContracts(ctx *fiber.Ctx) error {
	userID := uint(ctx.Locals("user_id").(float64))
	userIDStr := strconv.FormatUint(uint64(userID), 10)

	const myRouting = 111
	composite := fmt.Sprintf("%d%s", myRouting, userIDStr)

	var contracts []types.OptionContract
	if err := db.DB.
		Preload("Portfolio.Security").
		Preload("OTCTrade.Portfolio.Security").
		Where(`(buyer_id = ? OR seller_id = ? OR remote_buyer_id = ? OR remote_seller_id = ?)`,
			userID, userID, composite, composite).
		Find(&contracts).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom dohvatanja ugovora",
		})
	}
	var out []dto.OptionContractDTO
	for _, oc := range contracts {
		ticker := oc.OTCTrade.Ticker

		var secName *string
		if oc.Portfolio != nil && oc.Portfolio.Security.Name != "" {
			secName = &oc.Portfolio.Security.Name
		}

		out = append(out, dto.OptionContractDTO{
			ID:                  oc.ID,
			PortfolioID:         oc.PortfolioID,
			BuyerID:             oc.BuyerID,
			SellerID:            oc.SellerID,
			Ticker:              ticker,
			SecurityName:        secName,
			StrikePrice:         oc.StrikePrice,
			Premium:             oc.Premium,
			Quantity:            oc.Quantity,
			SettlementDate:      oc.SettlementAt.Format(time.RFC3339),
			IsExercised:         oc.IsExercised,
			RemoteRoutingNumber: oc.OTCTrade.RemoteRoutingNumber,
			RemoteBuyerID:       oc.RemoteBuyerID,
			RemoteSellerID:      oc.RemoteSellerID,
		})
	}

	return ctx.JSON(types.Response{
		Success: true,
		Data:    out,
	})
}

func (c *OTCTradeController) RejectOTCTrade(ctx *fiber.Ctx) error {
	id := ctx.Params("id")
	localUserID := uint(ctx.Locals("user_id").(float64))
	localUserIDStr := strconv.FormatUint(uint64(localUserID), 10)

	var trade types.OTCTrade
	if err := db.DB.First(&trade, id).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(types.Response{
			Success: false,
			Error:   "Ponuda nije pronađena",
		})
	}
	if trade.ModifiedBy == fmt.Sprintf("%d", localUserID) {
		return ctx.Status(fiber.StatusForbidden).JSON(types.Response{
			Success: false,
			Error:   "Ne možete uzastopno menjati ponudu",
		})
	}
	if trade.Status != "pending" {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Ponuda više nije aktivna",
		})
	}

	const myRouting = 111
	if trade.RemoteNegotiationID == nil {
		trade.Status = "rejected"
		trade.LastModified = time.Now().Unix()
		trade.ModifiedBy = fmt.Sprintf("%d%s", myRouting, localUserIDStr)

		if err := db.DB.Save(&trade).Error; err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Greška pri odbijanju ponude",
			})
		}

		return ctx.JSON(types.Response{
			Success: true,
			Data:    "Domaća ponuda je uspešno odbijena",
		})
	}

	baseURL := os.Getenv("BANK4_BASE_URL")
	url := fmt.Sprintf("%s/negotiations/%d/%s",
		baseURL,
		444,
		*trade.RemoteNegotiationID,
	)

	httpReq, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška pri kreiranju HTTP zahteva ka banci 4",
		})
	}
	httpReq.Header.Set("X-Api-Key", os.Getenv("BANK4_API_KEY"))

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return ctx.Status(fiber.StatusBadGateway).JSON(types.Response{
			Success: false,
			Error:   "Greška pri komunikaciji sa bankom 4",
		})
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return ctx.Status(resp.StatusCode).JSON(types.Response{
			Success: false,
			Error:   fmt.Sprintf("Bank4: %s", string(body)),
		})
	}
	trade.Status = "rejected"
	trade.LastModified = time.Now().Unix()
	trade.ModifiedBy = fmt.Sprintf("%d%s", myRouting, localUserIDStr)

	if err := db.DB.Save(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška pri ažuriranju ponude nakon odbijanja",
		})
	}

	return ctx.JSON(types.Response{
		Success: true,
		Data:    "Međubankarska ponuda je uspešno odbijena",
	})
}

type PortfolioControllerr struct{}

func NewPortfolioControllerr() *PortfolioControllerr {
	return &PortfolioControllerr{}
}

func InitPortfolioRoutess(app *fiber.App) {
	portfolioController := NewPortfolioControllerr()

	app.Get("/portfolio/public", middlewares.Auth, portfolioController.GetOurAndInterPublicPortfolios)
}

func GetPublicStocks(ctx *fiber.Ctx) error {
	const myRoutingNumber = 111

	var portfolios []types.Portfolio
	if err := db.DB.Preload("Security").Where("public_count > 0").Find(&portfolios).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom dohvatanja portfolija",
		})
	}

	grouped := map[string][]dto.SellerStockEntry{}

	for _, p := range portfolios {
		if !strings.EqualFold(p.Security.Type, "Stock") {
			continue
		}

		ticker := p.Security.Ticker
		entry := dto.SellerStockEntry{
			Seller: dto.ForeignBankId{
				RoutingNumber: myRoutingNumber,
				ID:            fmt.Sprintf("%d", p.UserID),
			},
			Amount: p.PublicCount,
		}

		grouped[ticker] = append(grouped[ticker], entry)
	}

	var result dto.PublicStocksResponse
	for ticker, sellers := range grouped {
		result = append(result, dto.PublicStock{
			Stock:   dto.StockDescription{Ticker: ticker},
			Sellers: sellers,
		})
	}

	return ctx.JSON(result)
}

type UnifiedPublicPortfolio struct {
	Ticker       string          `json:"ticker"`
	Quantity     int             `json:"quantity"`
	Price        *float64        `json:"price,omitempty"`
	SecurityName *string         `json:"name,omitempty"`
	PortfolioID  *uint           `json:"portfolioId,omitempty"`
	OwnerID      string          `json:"ownerId"`
	Security     *types.Security `json:"security,omitempty"`
}

func fetchForeignPublicStocks() ([]UnifiedPublicPortfolio, error) {
	foreignURL := os.Getenv("BANK4_PUBLIC_STOCK_URL")
	req, err := http.NewRequest("GET", foreignURL, nil)
	if err != nil {
		return nil, fmt.Errorf("Greška pri kreiranju zahteva ka banci 4: %w", err)
	}
	req.Header.Set("X-Api-Key", os.Getenv("BANK4_API_KEY"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Greška prilikom slanja zahteva ka banci 4: %w", err)
	}
	defer resp.Body.Close()

	var foreignStocks dto.PublicStocksResponse
	if err := json.NewDecoder(resp.Body).Decode(&foreignStocks); err != nil {
		return nil, fmt.Errorf("Neuspešno parsiranje podataka iz banke 4: %w", err)
	}

	var result []UnifiedPublicPortfolio
	for _, ps := range foreignStocks {
		var sec types.Security
		var namePtr *string
		var secPtr *types.Security
		if err := db.DB.Where("ticker = ?", ps.Stock.Ticker).First(&sec).Error; err == nil {
			tmp := sec
			namePtr = &sec.Name
			secPtr = &tmp
		}

		for _, seller := range ps.Sellers {
			ownerID := fmt.Sprintf("%d%s", seller.Seller.RoutingNumber, seller.Seller.ID)
			result = append(result, UnifiedPublicPortfolio{
				Ticker:       ps.Stock.Ticker,
				Quantity:     seller.Amount,
				Price:        nil,
				SecurityName: namePtr,
				PortfolioID:  nil,
				OwnerID:      ownerID,
				Security:     secPtr,
			})
		}
	}

	return result, nil
}

func (c *PortfolioControllerr) GetOurAndInterPublicPortfolios(ctx *fiber.Ctx) error {
	userID := uint(ctx.Locals("user_id").(float64))

	var localPortfolios []types.Portfolio
	if err := db.DB.
		Where("public_count > 0 AND user_id != ?", userID).
		Preload("Security").
		Find(&localPortfolios).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
			Success: false,
			Error:   "Greška prilikom dohvatanja domaćih portfolija",
		})
	}

	var result []UnifiedPublicPortfolio
	for _, p := range localPortfolios {
		if !strings.EqualFold(p.Security.Type, "Stock") {
			continue
		}
		price := p.PurchasePrice
		name := p.Security.Name
		ownerID := strconv.FormatUint(uint64(p.UserID), 10)

		result = append(result, UnifiedPublicPortfolio{
			Ticker:       p.Security.Ticker,
			Quantity:     p.PublicCount,
			Price:        &price,
			SecurityName: &name,
			PortfolioID:  &p.ID,
			OwnerID:      ownerID,
			Security:     &p.Security,
		})
	}

	foreign, err := fetchForeignPublicStocks()
	if err != nil {
		fmt.Println("!!!!!!!!!!!Greška u dohvatanju stranih portfolija:", err)
	} else {
		result = append(result, foreign...)
	}

	return ctx.JSON(types.Response{
		Success: true,
		Data:    result,
	})
}

type CreateInterbankOTCOfferRequest struct {
	Ticker         string  `json:"ticker" validate:"required"`
	Quantity       int     `json:"quantity" validate:"required,gt=0"`
	PricePerUnit   float64 `json:"pricePerUnit" validate:"required,gt=0"`
	Premium        float64 `json:"premium" validate:"required,gte=0"`
	SettlementDate string  `json:"settlementDate" validate:"required"`
	SellerRouting  int     `json:"sellerRouting" validate:"required"`
	SellerID       string  `json:"sellerId" validate:"required"`
}

func (c *OTCTradeController) GetInterbankNegotiation(ctx *fiber.Ctx) error {
	routingStr := ctx.Params("routingNumber")
	negID := ctx.Params("id")

	routingNum, err := strconv.Atoi(routingStr)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(types.Response{
			Success: false,
			Error:   "Neispravan routingNumber",
		})
	}
	fmt.Println(routingNum)
	var t types.OTCTrade
	if err := db.DB.
		Where("remote_negotiation_id = ?", negID).
		First(&t).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(types.Response{
			Success: false,
			Error:   "Negotiation not found",
		})
	}

	var buyerFB, sellerFB dto.ForeignBankId
	const myRouting = 111

	if t.LocalBuyerID != nil && t.LocalSellerID != nil {
		buyerFB = dto.ForeignBankId{RoutingNumber: myRouting, ID: fmt.Sprint(*t.LocalBuyerID)}
		sellerFB = dto.ForeignBankId{RoutingNumber: myRouting, ID: fmt.Sprint(*t.LocalSellerID)}
	} else {
		if t.RemoteRoutingNumber == nil || t.RemoteBuyerID == nil || t.RemoteSellerID == nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(types.Response{
				Success: false,
				Error:   "Nepotpuni podaci za međubankarsku ponudu",
			})
		}
		buyerFB = dto.ForeignBankId{RoutingNumber: *t.RemoteRoutingNumber, ID: *t.RemoteBuyerID}
		sellerFB = dto.ForeignBankId{RoutingNumber: *t.RemoteRoutingNumber, ID: *t.RemoteSellerID}
	}

	lmb := t.ModifiedBy
	lmbRouting, _ := strconv.Atoi(lmb[:3])
	lastMod := dto.ForeignBankId{
		RoutingNumber: lmbRouting,
		ID:            lmb[3:],
	}

	nt := dto.OtcNegotiation{
		Stock:          dto.StockDescription{Ticker: t.Ticker},
		SettlementDate: t.SettlementAt.Format(time.RFC3339),
		PricePerUnit:   dto.MonetaryValue{Currency: "USD", Amount: t.PricePerUnit},
		Premium:        dto.MonetaryValue{Currency: "USD", Amount: t.Premium},
		BuyerID:        buyerFB,
		SellerID:       sellerFB,
		Amount:         t.Quantity,
		LastModifiedBy: lastMod,
		IsOngoing:      t.Status == "pending",
	}

	return ctx.JSON(types.Response{
		Success: true,
		Data:    nt,
	})
}

// FUNKCIJE ZA RUTE KOJE BANKA 4 SALJE KA NAMA
func (c *OTCTradeController) CreateInterbankNegotiation(ctx *fiber.Ctx) error {
	//DODATI PROVERU DA LI POSTOJI PORTFOLIO SA TIM VLASNIKOMOM I TIM TICKEROM
	var off dto.InterbankOtcOfferDTO
	if err := ctx.BodyParser(&off); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Nevalidan JSON format",
		})
	}
	if err := c.validator.Struct(off); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}
	settlementAt, err := time.Parse(time.RFC3339, off.SettlementDate)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Nevalidan settlementDate, očekuje se RFC3339 format",
		})
	}
	negID := uuid.New().String()

	remoteBuyer := fmt.Sprintf("%d%s", off.BuyerID.RoutingNumber, off.BuyerID.ID)
	remoteSeller := fmt.Sprintf("%d%s", off.SellerID.RoutingNumber, off.SellerID.ID)

	modifiedBy := fmt.Sprintf("%d%s", off.LastModifiedBy.RoutingNumber, off.LastModifiedBy.ID)

	trade := types.OTCTrade{
		RemoteRoutingNumber: &off.BuyerID.RoutingNumber,
		RemoteNegotiationID: &negID,
		RemoteBuyerID:       &remoteBuyer,
		RemoteSellerID:      &remoteSeller,
		Ticker:              off.Stock.Ticker,
		Quantity:            off.Amount,
		PricePerUnit:        off.PricePerUnit.Amount,
		Premium:             off.Premium.Amount,
		SettlementAt:        settlementAt,
		Status:              "pending",
		ModifiedBy:          modifiedBy,
	}
	if err := db.DB.Create(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Greška pri čuvanju međubankarske ponude",
		})
	}

	ourRouting := 111
	return ctx.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success": true,
		"data": dto.ForeignBankId{
			RoutingNumber: ourRouting,
			ID:            negID,
		},
	})
}

func (c *OTCTradeController) CounterInterbankNegotiation(ctx *fiber.Ctx) error {
	routingStr := ctx.Params("routingNumber")
	negID := ctx.Params("id")
	routingNum, err := strconv.Atoi(routingStr)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Neispravan routingNumber",
		})
	}
	fmt.Println(routingNum)
	var off dto.InterbankOtcOfferDTO
	if err := ctx.BodyParser(&off); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Nevalidan JSON format",
		})
	}
	if err := c.validator.Struct(off); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	var trade types.OTCTrade
	if err := db.DB.
		Where("remote_negotiation_id = ?", negID).
		First(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Ponuda nije pronađena",
		})
	}

	settlementAt, err := time.Parse(time.RFC3339, off.SettlementDate)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Neispravan settlementDate, očekuje se RFC3339 format",
		})
	}

	currActor := fmt.Sprintf("%d%s", off.LastModifiedBy.RoutingNumber, off.LastModifiedBy.ID)
	if trade.ModifiedBy == currActor {
		return ctx.Status(fiber.StatusConflict).JSON(fiber.Map{
			"success": false,
			"error":   "Nije vaš red za kontra-ponudu",
		})
	}

	trade.Quantity = off.Amount
	trade.PricePerUnit = off.PricePerUnit.Amount
	trade.Premium = off.Premium.Amount
	trade.SettlementAt = settlementAt
	//routing := 444
	trade.ModifiedBy = fmt.Sprintf("%d%s", off.LastModifiedBy.RoutingNumber, off.LastModifiedBy.ID)

	if err := db.DB.Save(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Greška pri čuvanju kontra-ponude",
		})
	}

	return ctx.Status(fiber.StatusOK).JSON(fiber.Map{
		"success": true,
		"data":    fmt.Sprintf("Interbank konter-ponuda primljena: %s/%s", routingStr, negID),
	})
}

func (c *OTCTradeController) CloseInterbankNegotiation(ctx *fiber.Ctx) error {
	routingStr := ctx.Params("routingNumber")
	negID := ctx.Params("id")

	routingNum, err := strconv.Atoi(routingStr)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Neispravan routingNumber",
		})
	}

	var trade types.OTCTrade
	if err := db.DB.
		Where("remote_negotiation_id = ?", negID).
		First(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Negotation not found",
		})
	}

	if trade.Status != "pending" {
		return ctx.Status(fiber.StatusConflict).JSON(fiber.Map{
			"success": false,
			"error":   "Negotiation is already closed or resolved",
		})
	}
	//popravi last modified
	trade.Status = "rejected"
	trade.LastModified = time.Now().Unix()
	trade.ModifiedBy = fmt.Sprintf("%d%s", routingNum, negID)

	if err := db.DB.Save(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Greška pri zatvaranju ponude",
		})
	}

	return ctx.Status(fiber.StatusOK).JSON(fiber.Map{
		"success": true,
		"data":    fmt.Sprintf("Negotiation %d/%s closed", routingNum, negID),
	})
}

func (c *OTCTradeController) AcceptInterbankNegotiation(ctx *fiber.Ctx) error {
	routingStr := ctx.Params("routingNumber")
	negID := ctx.Params("id")

	routingNum, err := strconv.Atoi(routingStr)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Neispravan routingNumber",
		})
	}
	fmt.Println(routingNum)
	var trade types.OTCTrade
	if err := db.DB.
		Where("remote_negotiation_id = ?", negID).
		First(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Negotiation not found",
		})
	}

	if trade.Status != "pending" {
		return ctx.Status(fiber.StatusConflict).JSON(fiber.Map{
			"success": false,
			"error":   "Pregovori su zatvoreni ili ponuda već prihvaćena",
		})
	}

	var actor string
	if trade.ModifiedBy == *trade.RemoteBuyerID {
		actor = *trade.RemoteSellerID
	} else {
		actor = *trade.RemoteBuyerID
	}

	trade.Status = "accepted"
	trade.ModifiedBy = actor
	if err := db.DB.Save(&trade).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Greška pri ažuriranju ponude",
		})
	}

	contractID := uuid.New().String()
	contract := types.OptionContract{
		OTCTradeID:          trade.ID,
		RemoteContractID:    &contractID,
		RemoteBuyerID:       trade.RemoteBuyerID,
		RemoteSellerID:      trade.RemoteSellerID,
		Quantity:            trade.Quantity,
		StrikePrice:         trade.PricePerUnit,
		Premium:             trade.Premium,
		SettlementAt:        trade.SettlementAt,
		RemoteNegotiationID: trade.RemoteNegotiationID,
		Status:              "active",
		CreatedAt:           time.Now().Unix(),
	}
	if err := db.DB.Create(&contract).Error; err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Greška pri kreiranju ugovora",
		})
	}

	ourRouting := 111
	resp := struct {
		ID             dto.ForeignBankId    `json:"id"`
		Stock          dto.StockDescription `json:"stock"`
		PricePerUnit   dto.MonetaryValue    `json:"pricePerUnit"`
		SettlementDate string               `json:"settlementDate"`
		Amount         int                  `json:"amount"`
		NegotiationID  dto.ForeignBankId    `json:"negotiationId"`
	}{
		ID: dto.ForeignBankId{
			RoutingNumber: ourRouting,
			ID:            contractID,
		},
		Stock:          dto.StockDescription{Ticker: trade.Ticker},
		PricePerUnit:   dto.MonetaryValue{Currency: "USD", Amount: trade.PricePerUnit},
		SettlementDate: trade.SettlementAt.Format(time.RFC3339),
		Amount:         trade.Quantity,
		NegotiationID: dto.ForeignBankId{
			RoutingNumber: 111,
			ID:            *trade.RemoteNegotiationID,
		},
	}

	return ctx.JSON(fiber.Map{
		"success": true,
		"data":    resp,
	})
}

func InitOTCTradeRoutes(app *fiber.App) {
	app.Get("/public-stock", middlewares.RequireInterbankApiKey, GetPublicStocks)
	otcController := NewOTCTradeController()
	otc := app.Group("/otctrade", middlewares.Auth)
	otc.Post("/offer", middlewares.RequirePermission("user.customer.otc_trade"), otcController.CreateOTCTrade)
	otc.Put("/offer/:id/counter", middlewares.RequirePermission("user.customer.otc_trade"), otcController.CounterOfferOTCTrade)
	otc.Put("/offer/:id/accept", middlewares.RequirePermission("user.customer.otc_trade"), otcController.AcceptOTCTrade)
	otc.Put("/offer/:id/reject", middlewares.RequirePermission("user.customer.otc_trade"), otcController.RejectOTCTrade)
	otc.Put("/option/:id/execute", middlewares.RequirePermission("user.customer.otc_trade"), otcController.ExecuteOptionContract)
	otc.Get("/offer/active", otcController.GetActiveOffers)
	otc.Get("/option/contracts", otcController.GetUserOptionContracts)

	app.Get("/negotiations/:routingNumber/:id", otcController.GetInterbankNegotiation)
	app.Post("/negotiations", middlewares.RequireInterbankApiKey, otcController.CreateInterbankNegotiation)
	app.Put("/negotiations/:routingNumber/:id", middlewares.RequireInterbankApiKey, otcController.CounterInterbankNegotiation)
	app.Delete("/negotiations/:routingNumber/:id", middlewares.RequireInterbankApiKey, otcController.CloseInterbankNegotiation)
	app.Get("/negotiations/:routingNumber/:id/accept", middlewares.RequireInterbankApiKey, otcController.AcceptInterbankNegotiation)

}
