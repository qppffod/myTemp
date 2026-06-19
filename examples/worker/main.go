package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

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
	worker.RegisterWorkflow(TestTimerWorkflow)
	worker.RegisterWorkflow(ApprovalWorkflow)
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

func TestTimerWorkflow(c *workflow.Context, order PizzaOrder) error {
	var stock StockResult
	err := workflow.ExecuteActivity(c, "CheckStock", order).Get(&stock)
	if err != nil {
		return fmt.Errorf("out of stock: %w", err)
	}

	workflow.Sleep(c, time.Second*30)

	var tracking string
	err = workflow.ExecuteActivity(c, "Ship", ChargeResult{true, 100}).Get(&tracking)
	if err != nil {
		return err
	}
	log.Printf("Shipped: %s", tracking)
	return nil
}

func TestWorkflow(c *workflow.Context, order PizzaOrder) error {

	// sequential, typed data flows from one activity to the next.
	var stock StockResult
	err := workflow.ExecuteActivity(c, "CheckStock", order).Get(&stock)
	if err != nil {
		return fmt.Errorf("out of stock: %w", err)
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

var chargeAttempts int

func CheckStock(ctx context.Context, order PizzaOrder) (StockResult, error) {
	fmt.Printf("CheckStock: order %d\n", order.OrderID)
	item := ""
	if len(order.Items) > 0 {
		item = order.Items[0]
	}

	return StockResult{Available: true, Item: item}, nil
}

func ChargeCard(ctx context.Context, stock StockResult) (ChargeResult, error) {
	chargeAttempts++
	log.Printf("ChargeCard attempt %d at %s", chargeAttempts, time.Now().Format("15:04:05"))

	if chargeAttempts < 3 {
		return ChargeResult{}, errors.New("flaky API 503")
	}

	log.Printf("ChargeCard succeeded on attempt %d", chargeAttempts)
	return ChargeResult{Charged: true, Amount: 100}, nil
}

func Ship(ctx context.Context, charge ChargeResult) (string, error) {
	fmt.Printf("Ship: charged=%v amount=%d\n", charge.Charged, charge.Amount)
	return "TRACK-12345", nil
}

// ================================================================================

type Decision struct {
	Approved bool
}

func ApprovalWorkflow(c *workflow.Context, order PizzaOrder) error {
	var stock StockResult
	err := workflow.ExecuteActivity(c, "CheckStock", order).Get(&stock)
	if err != nil {
		return err
	}
	log.Printf("Submitted for approval, waiting for signal...")

	// suspended here until an "approval" signal arrives
	var decision Decision
	workflow.ReceiveSignal(c, "approval", &decision)

	if !decision.Approved {
		log.Printf("Rejected")
		return fmt.Errorf("approval rejected")
	}

	log.Printf("Approved! Proceeding to ship.")
	var tracking string
	err = workflow.ExecuteActivity(c, "Ship", ChargeResult{true, 100}).Get(&tracking)
	if err != nil {
		return err
	}
	log.Printf("Shipped: %s", tracking)
	return nil
}
