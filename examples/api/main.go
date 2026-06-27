package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/qppffod/myTemp/sdk"
)

func main() {
	client, err := sdk.NewClient("localhost:7233")
	if err != nil {
		log.Fatal("API client connection failed:", err)
	}

	mux := http.NewServeMux()

	// mux.HandleFunc("POST /test", TestHandler(client))
	mux.HandleFunc("POST /test", TestHandler(client))
	mux.HandleFunc("POST /approve", ApproveHandler(client))
	mux.HandleFunc("POST /order", OrderHandler(client))

	server := http.Server{
		Addr:    ":3000",
		Handler: mux,
	}

	log.Println("Server is running on port:", server.Addr)
	log.Fatal(server.ListenAndServe())
}

type PizzaOrder struct {
	OrderID int
	Items   []string
}

func TestHandler(c *sdk.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var order PizzaOrder
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			w.Write([]byte(fmt.Sprintf("json decoder error: %s", err.Error())))
			return
		}

		orderBytes, err := json.Marshal(order)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("json marshal error: %s", err.Error())))
			return
		}

		runID, err := c.StartNewWorkflow(r.Context(), "test-order22", "ApprovalWorkflow", "test", orderBytes)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Failed to get runID from StartWorkflow: %s", err.Error())))
			return
		}

		w.Write([]byte(fmt.Sprintf("runID:%s", runID)))
	}
}

func OrderHandler(c *sdk.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var order PizzaOrder
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, fmt.Sprintf("json decoder error: %s", err.Error()), 400)
			return
		}

		orderBytes, err := json.Marshal(order)
		if err != nil {
			http.Error(w, fmt.Sprintf("json marshal error: %s", err.Error()), 500)
			return
		}

		workflowID := fmt.Sprintf("order-%d", order.OrderID)
		runID, err := c.StartNewWorkflow(r.Context(), workflowID, "TestWorkflow", "test", orderBytes)
		if err != nil {
			http.Error(w, fmt.Sprintf("StartNewWorkflow failed: %s", err.Error()), 500)
			return
		}

		w.Write([]byte(fmt.Sprintf("workflowID:%s runID:%s", workflowID, runID)))
	}
}

type Decision struct {
	Approved bool
}

func ApproveHandler(c *sdk.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		decision := Decision{Approved: true}
		data, _ := json.Marshal(decision)
		err := c.SignalWorkflow(r.Context(), "test-order22", "", "approval", data)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte("approved"))
	}
}
