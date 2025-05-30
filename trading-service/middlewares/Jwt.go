package middlewares

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/dgrijalva/jwt-go"
	// "strings"
)

func keyFunc(token *jwt.Token) (interface{}, error) {
	return getSigningKey()
}

func getSigningKey() ([]byte, error) {
	secret := os.Getenv("JWT_SECRET") // Preuzimanje tajnog ključa iz env
	decodedKey, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, err
	}
	return decodedKey, nil
}

func readToken(tokenString string) (*jwt.Token, jwt.MapClaims, error) {
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, keyFunc)
	if err != nil {
		return nil, claims, err
	}
	return token, claims, nil
}

func NewOrderToken(direction string, userID uint, accountID uint, amount float64, fee float64) (string, error) {
	key, err := getSigningKey()
	if err != nil {
		return "", err
	}

	tokenString, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"direction": direction,
		"userId":    userID,
		"accountId": accountID,
		"amount":    fmt.Sprintf("%f", amount),
		"fee":       fmt.Sprintf("%f", fee),
	}).SignedString(key)

	if err != nil {
		return "", err
	}

	return tokenString, nil
}

func NewOrderTokenDirect(uid string, buyerAccountId uint, sellerAccountId uint, amount float64) (string, error) {
	key, err := getSigningKey()
	if err != nil {
		return "", err
	}

	tokenString, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid":             uid,
		"buyerAccountId":  buyerAccountId,
		"sellerAccountId": sellerAccountId,
		"amount":          fmt.Sprintf("%f", amount),
	}).SignedString(key)

	if err != nil {
		return "", err
	}

	return tokenString, nil
}
