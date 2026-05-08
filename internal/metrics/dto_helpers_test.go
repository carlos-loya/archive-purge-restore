// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package metrics

import (
	dto "github.com/prometheus/client_model/go"
)

// dtoMetricBridge keeps test code's interaction with the prometheus dto
// package localized. The dto types are used by the prometheus client
// internally for serialization; they're the only way to read observation
// counts off a histogram via the metric collection API.
type dtoMetricBridge struct {
	m dto.Metric
}

func (b *dtoMetricBridge) metric() *dto.Metric { return &b.m }

func (b *dtoMetricBridge) labelsAsMap() map[string]string {
	out := make(map[string]string, len(b.m.Label))
	for _, lp := range b.m.Label {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

func (b *dtoMetricBridge) histogramSampleCount() int {
	if h := b.m.GetHistogram(); h != nil {
		return int(h.GetSampleCount())
	}
	return 0
}
