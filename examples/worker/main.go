package main

import (
	"context"
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

func TestWorkflow(c *workflow.Context, order PizzaOrder) {

	// sequential
	var stock []byte
	workflow.ExecuteActivity(c, "CheckStock", nil).Get(&stock)
	var charge []byte
	workflow.ExecuteActivity(c, "ChargeCard", nil).Get(&charge)
	workflow.ExecuteActivity(c, "Ship", nil).Get(nil)

	// parallel execution
	// f1 := workflow.ExecuteActivity(c, "SendEmail", []byte("test"))
	// f2 := workflow.ExecuteActivity(c, "ReserveTable", []byte("test"))
	// f1.Get(nil)
	// f2.Get(nil)
}

func CheckStock(ctx context.Context, input []byte) []byte {
	fmt.Println("CheckStock")
	return []byte("CheckStock DATA")
}

func ChargeCard(ctx context.Context, input []byte) []byte {
	fmt.Println("ChargeCard")
	return []byte("ChargeCard DATA")
}

func Ship(ctx context.Context, input []byte) []byte {
	fmt.Println("Ship")
	return []byte("Ship DATA")
}
