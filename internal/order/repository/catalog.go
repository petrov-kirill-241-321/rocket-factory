package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/money"
	"github.com/petrov-kirill-241-321/rocket-factory/internal/order/domain"
)

// PostgresCatalogRepository читает каталог товаров.
// Цена берётся отсюда, а не из тела запроса клиента.
type PostgresCatalogRepository struct {
	db *pgxpool.Pool
}

func NewPostgresCatalogRepository(db *pgxpool.Pool) *PostgresCatalogRepository {
	return &PostgresCatalogRepository{db: db}
}

// FindActiveBySKUs возвращает активные позиции каталога одним запросом.
// Выборка по одному SKU в цикле давала N+1 обращений к БД на каждый заказ.
func (r *PostgresCatalogRepository) FindActiveBySKUs(ctx context.Context, skus []string) (map[string]domain.CatalogItem, error) {
	result := make(map[string]domain.CatalogItem, len(skus))
	if len(skus) == 0 {
		return result, nil
	}

	rows, err := r.db.Query(ctx, `
		select sku, name, unit_price::text
		from catalog_items
		where active and sku = any($1)
	`, skus)
	if err != nil {
		return nil, fmt.Errorf("load catalog items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			item      domain.CatalogItem
			unitPrice string
		)
		if err := rows.Scan(&item.SKU, &item.Name, &unitPrice); err != nil {
			return nil, fmt.Errorf("scan catalog item: %w", err)
		}
		item.UnitPrice, err = money.ParsePositive(unitPrice)
		if err != nil {
			return nil, fmt.Errorf("parse catalog price for %s: %w", item.SKU, err)
		}
		result[item.SKU] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate catalog items: %w", err)
	}
	return result, nil
}
