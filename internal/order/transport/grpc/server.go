package grpc

import (
	"context"
	"errors"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/kafka"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/repository"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/usecase"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	rocketpb.UnimplementedOrderServiceServer
	orders *usecase.OrderUsecase
}

func NewServer(orders *usecase.OrderUsecase) *Server {
	return &Server{orders: orders}
}

func (s *Server) GetOrder(ctx context.Context, req *rocketpb.GetOrderRequest) (*rocketpb.Order, error) {
	if req.GetOrderId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id and user_id are required")
	}

	order, err := s.orders.Get(ctx, req.GetOrderId(), req.GetUserId())
	if err != nil {
		if errors.Is(err, usecase.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, "order not found")
		}
		return nil, status.Error(codes.Internal, "get order")
	}
	return toProtoOrder(order), nil
}

func (s *Server) ListUserOrders(ctx context.Context, req *rocketpb.ListUserOrdersRequest) (*rocketpb.ListUserOrdersResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	orders, err := s.orders.List(ctx, req.GetUserId(), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Error(codes.Internal, "list orders")
	}

	response := &rocketpb.ListUserOrdersResponse{Orders: make([]*rocketpb.Order, 0, len(orders))}
	for _, order := range orders {
		response.Orders = append(response.Orders, toProtoOrder(order))
	}
	return response, nil
}

// UpdateOrderStatus — административный вход для ручной коррекции статуса.
// Требует source_event_id: он попадает в processed_events и делает повторный
// вызов с тем же идентификатором безопасным.
func (s *Server) UpdateOrderStatus(ctx context.Context, req *rocketpb.UpdateOrderStatusRequest) (*rocketpb.UpdateOrderStatusResponse, error) {
	statusValue := statusFromProto(req.GetStatus())
	if req.GetOrderId() == "" || statusValue == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id and a known status are required")
	}
	if req.GetSourceEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_event_id is required for idempotency")
	}

	update, err := s.orders.ApplyEventStatus(ctx, kafka.Event{
		EventID:   req.GetSourceEventId(),
		EventType: "grpc_status_update",
		OrderID:   req.GetOrderId(),
	}, statusValue, "order-service-grpc")
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrOrderNotFound):
			return nil, status.Error(codes.NotFound, "order not found")
		case errors.Is(err, repository.ErrDuplicateEvent):
			return nil, status.Error(codes.AlreadyExists, "status update with this source_event_id is already applied")
		case errors.Is(err, domain.ErrUnknownStatus):
			return nil, status.Error(codes.InvalidArgument, "unknown order status")
		default:
			return nil, status.Error(codes.Internal, "update order")
		}
	}
	if !update.Applied {
		return nil, status.Errorf(codes.FailedPrecondition,
			"order is already in a later state (%s)", update.PreviousStatus)
	}

	return &rocketpb.UpdateOrderStatusResponse{Order: toProtoOrder(update.Order)}, nil
}

func toProtoOrder(order domain.Order) *rocketpb.Order {
	items := make([]*rocketpb.OrderItem, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, &rocketpb.OrderItem{
			Sku:       item.SKU,
			Name:      item.Name,
			Quantity:  int32(item.Quantity),
			UnitPrice: item.UnitPrice.String(),
		})
	}
	return &rocketpb.Order{
		Id:          order.ID,
		UserId:      order.UserID,
		Status:      statusToProto(order.Status),
		Items:       items,
		TotalAmount: order.TotalAmount.String(),
		CreatedAt:   timestamppb.New(order.CreatedAt),
		UpdatedAt:   timestamppb.New(order.UpdatedAt),
	}
}

func statusToProto(value string) rocketpb.OrderStatus {
	switch value {
	case domain.StatusCreated:
		return rocketpb.OrderStatus_ORDER_STATUS_CREATED
	case domain.StatusInventoryReserved:
		return rocketpb.OrderStatus_ORDER_STATUS_INVENTORY_RESERVED
	case domain.StatusInventoryFailed:
		return rocketpb.OrderStatus_ORDER_STATUS_INVENTORY_FAILED
	case domain.StatusPaymentPending:
		return rocketpb.OrderStatus_ORDER_STATUS_PAYMENT_PENDING
	case domain.StatusPaid:
		return rocketpb.OrderStatus_ORDER_STATUS_PAID
	case domain.StatusProductionStarted:
		return rocketpb.OrderStatus_ORDER_STATUS_PRODUCTION_STARTED
	case domain.StatusCompleted:
		return rocketpb.OrderStatus_ORDER_STATUS_COMPLETED
	case domain.StatusFailed:
		return rocketpb.OrderStatus_ORDER_STATUS_FAILED
	default:
		return rocketpb.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}

func statusFromProto(value rocketpb.OrderStatus) string {
	switch value {
	case rocketpb.OrderStatus_ORDER_STATUS_CREATED:
		return domain.StatusCreated
	case rocketpb.OrderStatus_ORDER_STATUS_INVENTORY_RESERVED:
		return domain.StatusInventoryReserved
	case rocketpb.OrderStatus_ORDER_STATUS_INVENTORY_FAILED:
		return domain.StatusInventoryFailed
	case rocketpb.OrderStatus_ORDER_STATUS_PAYMENT_PENDING:
		return domain.StatusPaymentPending
	case rocketpb.OrderStatus_ORDER_STATUS_PAID:
		return domain.StatusPaid
	case rocketpb.OrderStatus_ORDER_STATUS_PRODUCTION_STARTED:
		return domain.StatusProductionStarted
	case rocketpb.OrderStatus_ORDER_STATUS_COMPLETED:
		return domain.StatusCompleted
	case rocketpb.OrderStatus_ORDER_STATUS_FAILED:
		return domain.StatusFailed
	default:
		return ""
	}
}
