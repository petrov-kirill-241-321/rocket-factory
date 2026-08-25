package grpc

import (
	"context"
	"errors"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/inventory/usecase"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	rocketpb.UnimplementedInventoryServiceServer
	inventory *usecase.InventoryUsecase
}

func NewServer(inventory *usecase.InventoryUsecase) *Server {
	return &Server{inventory: inventory}
}

func (s *Server) CheckAvailability(ctx context.Context, req *rocketpb.CheckAvailabilityRequest) (*rocketpb.CheckAvailabilityResponse, error) {
	availability, err := s.inventory.CheckAvailability(ctx, fromProtoItems(req.GetItems()))
	if err != nil {
		if isValidationError(err) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, "check availability")
	}

	resp := &rocketpb.CheckAvailabilityResponse{
		Available: true,
		Items:     make([]*rocketpb.InventoryAvailability, 0, len(availability)),
	}
	for _, item := range availability {
		resp.Items = append(resp.Items, &rocketpb.InventoryAvailability{
			Sku:       item.SKU,
			Requested: int32(item.Requested),
			Available: int32(item.Available),
			Enough:    item.Enough,
		})
		if !item.Enough {
			resp.Available = false
			resp.Reason = "not enough inventory"
		}
	}
	return resp, nil
}

// ReserveItems — синхронный резерв для сценариев, где нужен немедленный ответ.
// Основной путь остаётся событийным: order_created из Kafka.
func (s *Server) ReserveItems(ctx context.Context, req *rocketpb.ReserveItemsRequest) (*rocketpb.ReserveItemsResponse, error) {
	if req.GetOrderId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id and user_id are required")
	}

	// Идемпотентность синхронного вызова обеспечивается уникальным индексом
	// reservations(order_id): передавать сюда произвольный ключ в колонку
	// processed_events.event_id (тип uuid) было ошибкой — она падала на любом
	// значении, не являющемся UUID.
	reservation, err := s.inventory.ReserveForOrder(ctx,
		req.GetOrderId(), req.GetUserId(), fromProtoItems(req.GetItems()),
		repository.EventContext{},
	)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrReservationExists):
			return nil, status.Error(codes.AlreadyExists, "reservation for this order already exists")
		case isValidationError(err):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Error(codes.Internal, "reserve inventory")
		}
	}

	return &rocketpb.ReserveItemsResponse{
		ReservationId: reservation.ID,
		Status:        reservationStatusToProto(reservation.Status),
		Reason:        reservation.Reason,
	}, nil
}

func isValidationError(err error) bool {
	return errors.Is(err, domain.ErrEmptyReservation) ||
		errors.Is(err, domain.ErrInvalidQuantity) ||
		errors.Is(err, domain.ErrInvalidSKU)
}

func fromProtoItems(items []*rocketpb.OrderItem) []domain.ReservationItem {
	out := make([]domain.ReservationItem, 0, len(items))
	for _, item := range items {
		out = append(out, domain.ReservationItem{
			SKU:      item.GetSku(),
			Name:     item.GetName(),
			Quantity: int(item.GetQuantity()),
		})
	}
	return out
}

func reservationStatusToProto(value string) rocketpb.ReservationStatus {
	switch value {
	case domain.ReservationStatusReserved, domain.ReservationStatusCommitted:
		return rocketpb.ReservationStatus_RESERVATION_STATUS_RESERVED
	case domain.ReservationStatusFailed, domain.ReservationStatusReleased:
		return rocketpb.ReservationStatus_RESERVATION_STATUS_FAILED
	default:
		return rocketpb.ReservationStatus_RESERVATION_STATUS_UNSPECIFIED
	}
}
