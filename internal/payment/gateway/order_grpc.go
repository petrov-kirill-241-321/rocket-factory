package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/grpcx"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/payment/usecase"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const callTimeout = 3 * time.Second

// OrderGRPCGateway читает заказ в order-service.
type OrderGRPCGateway struct {
	conn   *grpc.ClientConn
	client rocketpb.OrderServiceClient
}

// NewOrderGRPCGateway создаёт клиента. grpc.NewClient подключается лениво,
// поэтому payment-service стартует независимо от готовности order-service.
func NewOrderGRPCGateway(addr string) (*OrderGRPCGateway, error) {
	conn, err := grpcx.NewClient(addr)
	if err != nil {
		return nil, fmt.Errorf("create order service client: %w", err)
	}
	return &OrderGRPCGateway{conn: conn, client: rocketpb.NewOrderServiceClient(conn)}, nil
}

func (g *OrderGRPCGateway) GetOrder(ctx context.Context, orderID, userID string) (usecase.OrderSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	order, err := g.client.GetOrder(ctx, &rocketpb.GetOrderRequest{OrderId: orderID, UserId: userID})
	if err != nil {
		// Отсутствующий заказ — это 404 для клиента, а не 500.
		// Раньше ошибка транспорта попадала в общий обработчик и превращалась
		// во внутреннюю ошибку сервиса.
		if status.Code(err) == codes.NotFound {
			return usecase.OrderSnapshot{}, usecase.ErrOrderNotFound
		}
		return usecase.OrderSnapshot{}, fmt.Errorf("get order via grpc: %w", err)
	}

	amount, err := money.Parse(order.GetTotalAmount())
	if err != nil {
		return usecase.OrderSnapshot{}, fmt.Errorf("parse order total %q: %w", order.GetTotalAmount(), err)
	}

	return usecase.OrderSnapshot{
		ID:          order.GetId(),
		UserID:      order.GetUserId(),
		Status:      orderStatusString(order.GetStatus()),
		TotalAmount: amount,
	}, nil
}

func (g *OrderGRPCGateway) Close() error {
	return g.conn.Close()
}

func orderStatusString(status rocketpb.OrderStatus) string {
	switch status {
	case rocketpb.OrderStatus_ORDER_STATUS_CREATED:
		return "created"
	case rocketpb.OrderStatus_ORDER_STATUS_INVENTORY_RESERVED:
		return "inventory_reserved"
	case rocketpb.OrderStatus_ORDER_STATUS_INVENTORY_FAILED:
		return "inventory_failed"
	case rocketpb.OrderStatus_ORDER_STATUS_PAYMENT_PENDING:
		return "payment_pending"
	case rocketpb.OrderStatus_ORDER_STATUS_PAID:
		return "paid"
	case rocketpb.OrderStatus_ORDER_STATUS_PRODUCTION_STARTED:
		return "production_started"
	case rocketpb.OrderStatus_ORDER_STATUS_COMPLETED:
		return "completed"
	case rocketpb.OrderStatus_ORDER_STATUS_FAILED:
		return "failed"
	default:
		return ""
	}
}
