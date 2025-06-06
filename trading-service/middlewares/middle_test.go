package middlewares

import (
    "encoding/base64"
    "net/http/httptest"
    "os"
    "testing"

    "github.com/gofiber/fiber/v2"
    "github.com/dgrijalva/jwt-go"
    "github.com/stretchr/testify/assert"
)

func setupTestEnv() {
    // Set a test JWT secret
    testSecret := base64.StdEncoding.EncodeToString([]byte("test-secret-key"))
    os.Setenv("JWT_SECRET", testSecret)
}

func TestGetSigningKey(t *testing.T) {
    setupTestEnv()
    
    key, err := getSigningKey()
    assert.NoError(t, err)
    assert.NotNil(t, key)
    assert.Equal(t, []byte("test-secret-key"), key)
}

func TestNewOrderToken(t *testing.T) {
    setupTestEnv()
    
    // Create a new order token
    token, err := NewOrderToken("buy", 1, 2, 100.5, 1.5)
    assert.NoError(t, err)
    assert.NotEmpty(t, token)
    
    // Verify the token can be parsed
    parsedToken, claims, err := readToken(token)
    assert.NoError(t, err)
    assert.True(t, parsedToken.Valid)
    
    // Verify the claims
    assert.Equal(t, "buy", claims["direction"])
    assert.Equal(t, float64(1), claims["userId"])
    assert.Equal(t, float64(2), claims["accountId"])
    assert.Equal(t, "100.500000", claims["amount"])
    assert.Equal(t, "1.500000", claims["fee"])
}

func TestAuthMiddleware(t *testing.T) {
    setupTestEnv()
    
    // Create a new app
    app := fiber.New()
    
    // Test route with Auth middleware
    app.Get("/protected", Auth, func(c *fiber.Ctx) error {
        return c.SendString("Protected content")
    })
    
    // Create a valid token for testing
    claims := jwt.MapClaims{
        "id":          1,
        "position":    "manager",
        "department":  "trading",
        "permissions": []interface{}{"create_order", "view_account"},
        "isAdmin":     true,
        "isEmployed":  true,
    }
    
    key, _ := getSigningKey()
    tokenString, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
    
    // Test with valid token
    req := httptest.NewRequest("GET", "/protected", nil)
    req.Header.Set("Authorization", "Bearer "+tokenString)
    resp, err := app.Test(req)
    assert.NoError(t, err)
    assert.Equal(t, 200, resp.StatusCode)
    
    // Test with no token
    req = httptest.NewRequest("GET", "/protected", nil)
    resp, err = app.Test(req)
    assert.NoError(t, err)
    assert.Equal(t, 401, resp.StatusCode)
}

func TestDepartmentCheck(t *testing.T) {
    setupTestEnv()
    
    // Create a new app
    app := fiber.New()
    
    // Test route with Auth and DepartmentCheck middlewares
    app.Get("/trading-only", Auth, DepartmentCheck("trading"), func(c *fiber.Ctx) error {
        return c.SendString("Trading department content")
    })
    
    // Create tokens with different departments
    createTestToken := func(dept string) string {
        claims := jwt.MapClaims{
            "id":         1,
            "department": dept,
        }
        key, _ := getSigningKey()
        token, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
        return token
    }
    
    // Test with correct department
    req := httptest.NewRequest("GET", "/trading-only", nil)
    req.Header.Set("Authorization", "Bearer "+createTestToken("trading"))
    resp, err := app.Test(req)
    assert.NoError(t, err)
    assert.Equal(t, 200, resp.StatusCode)
    
    // Test with wrong department
    req = httptest.NewRequest("GET", "/trading-only", nil)
    req.Header.Set("Authorization", "Bearer "+createTestToken("finance"))
    resp, err = app.Test(req)
    assert.NoError(t, err)
    assert.Equal(t, 403, resp.StatusCode)
}

func TestRequirePermission(t *testing.T) {
    setupTestEnv()
    
    // Create a new app
    app := fiber.New()
    
    // Test route with Auth and RequirePermission middlewares
    app.Get("/create-order", Auth, RequirePermission("create_order"), func(c *fiber.Ctx) error {
        return c.SendString("Order creation allowed")
    })
    
    // Create tokens with different permissions
    createPermissionToken := func(permissions []interface{}) string {
        claims := jwt.MapClaims{
            "id":          1,
            "permissions": permissions,
        }
        key, _ := getSigningKey()
        token, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
        return token
    }
    
    // Test with required permission
    req := httptest.NewRequest("GET", "/create-order", nil)
    req.Header.Set("Authorization", "Bearer "+createPermissionToken([]interface{}{"create_order", "view_account"}))
    resp, err := app.Test(req)
    assert.NoError(t, err)
    assert.Equal(t, 200, resp.StatusCode)
    
    // Test without required permission
    req = httptest.NewRequest("GET", "/create-order", nil)
    req.Header.Set("Authorization", "Bearer "+createPermissionToken([]interface{}{"view_account"}))
    resp, err = app.Test(req)
    assert.NoError(t, err)
    assert.Equal(t, 403, resp.StatusCode)
}