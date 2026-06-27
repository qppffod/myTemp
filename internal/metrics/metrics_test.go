package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestNewRegistersCollectors verifies New registers its collectors on the
// supplied registry and that a counter increments under the expected name.
func TestNewRegistersCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.WorkflowsStarted.Inc()
	m.TaskPolls.WithLabelValues("workflow", "hit").Inc()
	m.TaskPolls.WithLabelValues("workflow", "miss").Inc()

	want := `
		# HELP myengine_workflows_started_total Total number of workflow executions started.
		# TYPE myengine_workflows_started_total counter
		myengine_workflows_started_total 1
	`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "myengine_workflows_started_total"); err != nil {
		t.Fatalf("unexpected metric output: %v", err)
	}

	if got := testutil.ToFloat64(m.TaskPolls.WithLabelValues("workflow", "hit")); got != 1 {
		t.Fatalf("task_polls{result=hit} = %v, want 1", got)
	}
}

func TestNewIsIdempotentPerRegistry(t *testing.T) {
	_ = New(prometheus.NewRegistry())
	_ = New(prometheus.NewRegistry())
}
