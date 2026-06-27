// Package metrics holds the engine's Prometheus instrumentation. It owns the
// domain (business) counters emitted by the history layer and the gRPC handlers;
// the standard Go runtime / process collectors and the gRPC server metrics are
// registered separately (see cmd/engine and internal/frontend).
//
// Metrics are deliberately additive to the existing observability story: the
// engine does not log successful lifecycle events because they are already
// durable rows in the events table, but their aggregate counts are exactly what
// you want as metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "myengine"

// Metrics bundles the engine's domain counters. Construct it with New, which
// registers every collector on the supplied registry. Counters are incremented
// only after the owning transaction commits, so they reflect durable truth
// rather than rolled-back attempts.
type Metrics struct {
	WorkflowsStarted    prometheus.Counter
	WorkflowsTerminated *prometheus.CounterVec
	ActivitiesScheduled prometheus.Counter
	ActivitiesFinished  *prometheus.CounterVec
	ActivityRetries     prometheus.Counter
	TimersStarted       prometheus.Counter
	TimersFired         prometheus.Counter
	SignalsDelivered    prometheus.Counter
	LeasesReclaimed     prometheus.Counter
	TaskPolls           *prometheus.CounterVec
}

// New builds the domain collectors and registers them on reg, returning a
// non-nil *Metrics so call sites never need nil-guards (tests can pass
// metrics.New(prometheus.NewRegistry())). It panics if a collector is
// registered twice on the same registry, which only happens on a wiring bug.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		WorkflowsStarted: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "workflows_started_total",
			Help:      "Total number of workflow executions started.",
		}),
		WorkflowsTerminated: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "workflows_terminated_total",
			Help:      "Total number of workflow executions that reached a terminal state, labelled by outcome.",
		}, []string{"outcome"}),
		ActivitiesScheduled: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "activities_scheduled_total",
			Help:      "Total number of activities scheduled by workflow executions.",
		}),
		ActivitiesFinished: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "activities_finished_total",
			Help:      "Total number of activities that reached a terminal state, labelled by outcome (failed counts only attempt exhaustion).",
		}, []string{"outcome"}),
		ActivityRetries: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "activity_retries_total",
			Help:      "Total number of activity attempts that failed and were rescheduled for retry.",
		}),
		TimersStarted: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "timers_started_total",
			Help:      "Total number of durable timers started by workflow executions.",
		}),
		TimersFired: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "timers_fired_total",
			Help:      "Total number of durable timers that fired.",
		}),
		SignalsDelivered: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "signals_delivered_total",
			Help:      "Total number of signals delivered to workflow executions.",
		}),
		LeasesReclaimed: factory.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "leases_reclaimed_total",
			Help:      "Total number of task leases reclaimed from crashed or stalled workers.",
		}),
		TaskPolls: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "task_polls_total",
			Help:      "Total number of task polls, labelled by task type and whether a task was returned.",
		}, []string{"task_type", "result"}),
	}
}
