package grpc

import (
	"context"
	"errors"

	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/domain"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/production/usecase"
	rocketpb "github.com/petrov-kirill-241-321/rocket-factory/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	rocketpb.UnimplementedProductionServiceServer
	production *usecase.ProductionUsecase
}

func NewServer(production *usecase.ProductionUsecase) *Server {
	return &Server{production: production}
}

func (s *Server) GetProductionTask(ctx context.Context, req *rocketpb.GetProductionTaskRequest) (*rocketpb.ProductionTask, error) {
	if req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}

	task, err := s.production.Get(ctx, req.GetTaskId())
	if err != nil {
		if errors.Is(err, usecase.ErrTaskNotFound) {
			return nil, status.Error(codes.NotFound, "production task not found")
		}
		return nil, status.Error(codes.Internal, "get production task")
	}
	return toProtoTask(task), nil
}

func toProtoTask(task domain.Task) *rocketpb.ProductionTask {
	resp := &rocketpb.ProductionTask{
		Id:        task.ID,
		OrderId:   task.OrderID,
		UserId:    task.UserID,
		Status:    statusToProto(task.Status),
		CreatedAt: timestamppb.New(task.CreatedAt),
		UpdatedAt: timestamppb.New(task.UpdatedAt),
	}
	if task.StartedAt != nil {
		resp.StartedAt = timestamppb.New(*task.StartedAt)
	}
	if task.CompletedAt != nil {
		resp.CompletedAt = timestamppb.New(*task.CompletedAt)
	}
	return resp
}

func statusToProto(value string) rocketpb.ProductionStatus {
	switch value {
	case domain.StatusStarted:
		return rocketpb.ProductionStatus_PRODUCTION_STATUS_STARTED
	case domain.StatusCompleted:
		return rocketpb.ProductionStatus_PRODUCTION_STATUS_COMPLETED
	case domain.StatusFailed:
		return rocketpb.ProductionStatus_PRODUCTION_STATUS_FAILED
	default:
		return rocketpb.ProductionStatus_PRODUCTION_STATUS_UNSPECIFIED
	}
}
