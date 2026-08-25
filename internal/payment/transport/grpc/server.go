package grpc

import (
	"context"
	"errors"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/usecase"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	rocketpb.UnimplementedPaymentServiceServer
	payments *usecase.PaymentUsecase
}

func NewServer(payments *usecase.PaymentUsecase) *Server {
	return &Server{payments: payments}
}

func (s *Server) CreatePayment(ctx context.Context, req *rocketpb.CreatePaymentRequest) (*rocketpb.Payment, error) {
	if req.GetOrderId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id and user_id are required")
	}

	// Поле amount в запросе намеренно игнорируется: сумма определяется заказом.
	payment, err := s.payments.Create(ctx, usecase.CreatePaymentInput{
		OrderID:        req.GetOrderId(),
		UserID:         req.GetUserId(),
		IdempotencyKey: req.GetIdempotencyKey(),
		Simulation:     simulationFromProto(req.GetSimulation()),
	})
	if err != nil {
		switch {
		case errors.Is(err, usecase.ErrOrderNotFound):
			return nil, status.Error(codes.NotFound, "order not found")
		case errors.Is(err, usecase.ErrOrderNotPayable):
			return nil, status.Error(codes.FailedPrecondition, "order is not payable")
		case errors.Is(err, usecase.ErrActivePaymentExists):
			return nil, status.Error(codes.AlreadyExists, "order already has an active payment")
		case errors.Is(err, usecase.ErrIdempotencyInFlight):
			return nil, status.Error(codes.Aborted, "request with the same idempotency key is in flight")
		default:
			return nil, status.Error(codes.Internal, "create payment")
		}
	}
	return toProtoPayment(payment), nil
}

func (s *Server) GetPayment(ctx context.Context, req *rocketpb.GetPaymentRequest) (*rocketpb.Payment, error) {
	if req.GetPaymentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "payment_id is required")
	}

	payment, err := s.payments.Get(ctx, req.GetPaymentId())
	if err != nil {
		if errors.Is(err, usecase.ErrPaymentNotFound) {
			return nil, status.Error(codes.NotFound, "payment not found")
		}
		return nil, status.Error(codes.Internal, "get payment")
	}
	return toProtoPayment(payment), nil
}

func simulationFromProto(value rocketpb.PaymentSimulation) string {
	if value == rocketpb.PaymentSimulation_PAYMENT_SIMULATION_FAILURE {
		return usecase.SimulationFailure
	}
	return usecase.SimulationSuccess
}

func toProtoPayment(payment domain.Payment) *rocketpb.Payment {
	return &rocketpb.Payment{
		Id:        payment.ID,
		OrderId:   payment.OrderID,
		UserId:    payment.UserID,
		Amount:    payment.Amount.String(),
		Status:    statusToProto(payment.Status),
		CreatedAt: timestamppb.New(payment.CreatedAt),
		UpdatedAt: timestamppb.New(payment.UpdatedAt),
	}
}

func statusToProto(value string) rocketpb.PaymentStatus {
	switch value {
	case domain.StatusPending:
		return rocketpb.PaymentStatus_PAYMENT_STATUS_PENDING
	case domain.StatusSucceeded:
		return rocketpb.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	case domain.StatusFailed:
		return rocketpb.PaymentStatus_PAYMENT_STATUS_FAILED
	default:
		return rocketpb.PaymentStatus_PAYMENT_STATUS_UNSPECIFIED
	}
}
