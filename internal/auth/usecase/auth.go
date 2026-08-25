package usecase

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/security"
	"golang.org/x/crypto/bcrypt"
)

const (
	minPasswordLength = 8
	// bcrypt использует только первые 72 байта пароля; более длинный пароль
	// молча усекается, поэтому его лучше отклонить явно.
	maxPasswordLength = 72
	maxEmailLength    = 254
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidEmail       = errors.New("invalid email")
	ErrWeakPassword       = fmt.Errorf("password must contain at least %d characters", minPasswordLength)
	ErrPasswordTooLong    = fmt.Errorf("password must not exceed %d bytes", maxPasswordLength)
)

// dummyHash используется, чтобы время ответа не зависело от существования
// пользователя: иначе разница в задержке позволяет перебором собрать список
// зарегистрированных адресов.
var dummyHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

type AuthUsecase struct {
	users     repository.UserRepository
	jwtSecret string
	jwtTTL    time.Duration
}

type AuthResult struct {
	UserID string
	Email  string
	Token  string
}

func NewAuthUsecase(users repository.UserRepository, jwtSecret string, jwtTTL time.Duration) *AuthUsecase {
	return &AuthUsecase{users: users, jwtSecret: jwtSecret, jwtTTL: jwtTTL}
}

func (u *AuthUsecase) Register(ctx context.Context, email, password string) (AuthResult, error) {
	email = normalizeEmail(email)
	if err := validateEmail(email); err != nil {
		return AuthResult{}, err
	}
	if err := validatePassword(password); err != nil {
		return AuthResult{}, err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return AuthResult{}, fmt.Errorf("hash password: %w", err)
	}

	now := time.Now().UTC()
	user := domain.User{
		ID:           uuid.NewString(),
		Email:        email,
		PasswordHash: string(hash),
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := u.users.Create(ctx, user); err != nil {
		return AuthResult{}, err
	}

	return u.issue(user)
}

func (u *AuthUsecase) Login(ctx context.Context, email, password string) (AuthResult, error) {
	email = normalizeEmail(email)

	user, err := u.users.FindByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			// Хеширование выполняется и для несуществующего пользователя,
			// чтобы уравнять время ответа.
			_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
			return AuthResult{}, ErrInvalidCredentials
		}
		return AuthResult{}, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return AuthResult{}, ErrInvalidCredentials
	}

	return u.issue(user)
}

func (u *AuthUsecase) ValidateToken(ctx context.Context, tokenValue string) (AuthResult, error) {
	claims, err := security.ParseJWT(tokenValue, u.jwtSecret)
	if err != nil {
		return AuthResult{}, ErrInvalidCredentials
	}

	// Токен может быть валиден по подписи, но принадлежать удалённому пользователю.
	user, err := u.users.FindByID(ctx, claims.UserID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return AuthResult{}, ErrInvalidCredentials
		}
		return AuthResult{}, err
	}

	return AuthResult{UserID: user.ID, Email: user.Email, Token: tokenValue}, nil
}

func (u *AuthUsecase) issue(user domain.User) (AuthResult, error) {
	token, err := security.IssueJWT(user.ID, user.Email, u.jwtSecret, u.jwtTTL)
	if err != nil {
		return AuthResult{}, err
	}
	return AuthResult{UserID: user.ID, Email: user.Email, Token: token}, nil
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func validateEmail(email string) error {
	if email == "" || len(email) > maxEmailLength {
		return ErrInvalidEmail
	}
	if _, err := mail.ParseAddress(email); err != nil {
		return ErrInvalidEmail
	}
	return nil
}

func validatePassword(password string) error {
	if len(password) < minPasswordLength {
		return ErrWeakPassword
	}
	if len(password) > maxPasswordLength {
		return ErrPasswordTooLong
	}
	return nil
}
