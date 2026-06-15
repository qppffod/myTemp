package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/qppffod/myTemp/sdk"
	"github.com/qppffod/myTemp/sdk/workflow"
)

func main() {
	ctx := context.Background()

	client, err := sdk.NewClient("localhost:7233")
	if err != nil {
		log.Fatal("Worker client connection failed:", err)
	}

	worker := sdk.NewWorker(client, "test")

	worker.RegisterWorkflow(TestWorkflow)
	worker.RegisterActivity(CheckStock)
	worker.RegisterActivity(ChargeCard)
	worker.RegisterActivity(Ship)

	log.Println("Worker is running...")
	worker.Run(ctx)
}

type PizzaOrder struct {
	OrderID int
	Items   []string
}

type StockResult struct {
	Available bool
	Item      string
}

type ChargeResult struct {
	Charged bool
	Amount  int
}

func TestWorkflow(c *workflow.Context, order PizzaOrder) error {

	// sequential, typed data flows from one activity to the next.
	var stock StockResult
	err := workflow.ExecuteActivity(c, "CheckStock", order).Get(&stock)
	if err != nil {
		log.Printf("stock check failed, but continuing: %v", err)
		return nil
	}
	if stock.Available {
		log.Printf("Stock is correct: %s", stock.Item)
	} else {
		return fmt.Errorf("not availabe")
	}

	// A -> B: CheckStock's typed result is the input to ChargeCard.
	var charge ChargeResult
	err = workflow.ExecuteActivity(c, "ChargeCard", stock).Get(&charge)
	if err != nil {
		return err
	}
	if charge.Charged {
		log.Printf("Charged successfully: %d", charge.Amount)
	} else {
		return fmt.Errorf("charge failed")
	}

	// B -> C: ChargeCard's typed result is the input to Ship.
	var tracking string
	err = workflow.ExecuteActivity(c, "Ship", charge).Get(&tracking)
	if err != nil {
		return err
	}
	log.Printf("Shipped: %s", tracking)

	return nil

	// parallel execution
	// f1 := workflow.ExecuteActivity(c, "SendEmail", []byte("test"))
	// f2 := workflow.ExecuteActivity(c, "ReserveTable", []byte("test"))
	// f1.Get(nil)
	// f2.Get(nil)
}

func CheckStock(ctx context.Context, order PizzaOrder) (StockResult, error) {
	fmt.Printf("CheckStock: order %d\n", order.OrderID)
	item := ""
	if len(order.Items) > 0 {
		item = order.Items[0]
	}
	// return StockResult{Available: true, Item: item}, nil
	return StockResult{Available: false, Item: item}, errors.New("out of stock")
}

func ChargeCard(ctx context.Context, stock StockResult) (ChargeResult, error) {
	fmt.Printf("ChargeCard: %s available=%v\n", stock.Item, stock.Available)
	return ChargeResult{Charged: stock.Available, Amount: 100}, nil
}

func Ship(ctx context.Context, charge ChargeResult) (string, error) {
	fmt.Printf("Ship: charged=%v amount=%d\n", charge.Charged, charge.Amount)
	return "TRACK-12345", nil
}
