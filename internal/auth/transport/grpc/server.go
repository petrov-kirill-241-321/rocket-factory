package grpc

import (
	"context"
	"errors"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/auth/usecase"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	rocketpb.UnimplementedAuthServiceServer
	auth *usecase.AuthUsecase
}

func NewServer(auth *usecase.AuthUsecase) *Server {
	return &Server{auth: auth}
}

func (s *Server) ValidateToken(ctx context.Context, req *rocketpb.ValidateTokenRequest) (*rocketpb.ValidateTokenResponse, error) {
	result, err := s.auth.ValidateToken(ctx, req.GetToken())
	if err != nil {
		if errors.Is(err, usecase.ErrInvalidCredentials) {
			return &rocketpb.ValidateTokenResponse{Valid: false}, nil
		}
		return nil, status.Error(codes.Internal, "validate token")
	}

	return &rocketpb.ValidateTokenResponse{
		Valid:  true,
		UserId: result.UserID,
		Email:  result.Email,
	}, nil
}
