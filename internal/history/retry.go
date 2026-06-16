package history

import (
	"math"
	"math/rand/v2"
	"time"
)

var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:        3,
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaxInterval:        time.Minute,
}

type RetryPolicy struct {
	MaxAttempts        int32
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration
}

func netxtRetryDelay(policy RetryPolicy, attempt int32) time.Duration {
	backoff := float64(policy.InitialInterval) *
		math.Pow(policy.BackoffCoefficient, float64(attempt-1))

	if backoff > float64(policy.MaxInterval) {
		backoff = float64(policy.MaxInterval)
	}

	jittered := backoff * (0.5 + rand.Float64()*0.5)

	return time.Duration(jittered)
}
