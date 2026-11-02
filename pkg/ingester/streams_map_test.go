package ingester

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/util"
	"github.com/grafana/loki/v3/pkg/validation"
)

func TestStreamsMap(t *testing.T) {
	limits, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)
	limiter := NewLimiter(limits, NilMetrics, newIngesterRingLimiterStrategy(&ringCountMock{count: 1}, 1), &TenantBasedStrategy{limits: limits})
	retentionHours := util.RetentionHours(limiter.limits.RetentionPeriod("fake"))
	chunkfmt, headfmt := defaultChunkFormat(t)

	ss := []*stream{
		newStream(
			chunkfmt,
			headfmt,
			defaultConfig(),
			limiter.rateLimitStrategy,
			"fake",
			model.Fingerprint(1),
			labels.FromStrings("foo", "bar"),
			NewStreamRateCalculator(),
			NilMetrics,
			nil,
			nil,
			retentionHours,
			noPolicy,
		),
		newStream(
			chunkfmt,
			headfmt,
			defaultConfig(),
			limiter.rateLimitStrategy,
			"fake",
			model.Fingerprint(2),
			labels.FromStrings("bar", "foo"),
			NewStreamRateCalculator(),
			NilMetrics,
			nil,
			nil,
			retentionHours,
			noPolicy,
		),
	}
	var s *stream
	var loaded bool

	streams := newStreamsMap()

	require.Equal(t, 0, streams.Len())
	s, loaded = streams.Load(ss[0].labelsString)
	require.Nil(t, s)
	require.False(t, loaded)
	s, loaded = streams.LoadByFP(ss[1].fp)
	require.Nil(t, s)
	require.False(t, loaded)

	// Test LoadOrStoreNew
	s, loaded, err = streams.LoadOrStoreNew(ss[0].labelsString, func() (*stream, error) {
		return ss[0], nil
	}, nil)
	require.Equal(t, s, ss[0])
	require.False(t, loaded)
	require.Nil(t, err)

	// Test LoadOrStoreNewByFP
	s, loaded, err = streams.LoadOrStoreNewByFP(ss[1].fp, func() (*stream, error) {
		return ss[1], nil
	}, nil)
	require.Equal(t, s, ss[1])
	require.False(t, loaded)
	require.Nil(t, err)

	require.Equal(t, len(ss), streams.Len())

	for _, st := range ss {
		s, loaded = streams.Load(st.labelsString)
		require.Equal(t, st, s)
		require.True(t, loaded)

		s, loaded = streams.LoadByFP(st.fp)
		require.Equal(t, st, s)
		require.True(t, loaded)
	}

	// Test Delete
	for _, st := range ss {
		deleted := streams.Delete(st)
		require.True(t, deleted)

		s, loaded = streams.Load(st.labelsString)
		require.Nil(t, s)
		require.False(t, loaded)

		s, loaded = streams.LoadByFP(st.fp)
		require.Nil(t, s)
		require.False(t, loaded)
	}

	require.Equal(t, 0, streams.Len())

	// Test Store
	streams.Store(ss[0].labelsString, ss[0])

	s, loaded = streams.Load(ss[0].labelsString)
	require.Equal(t, ss[0], s)
	require.True(t, loaded)

	s, loaded = streams.LoadByFP(ss[0].fp)
	require.Equal(t, ss[0], s)
	require.True(t, loaded)

	// Test StoreByFP
	streams.StoreByFP(ss[1].fp, ss[1])

	s, loaded = streams.Load(ss[1].labelsString)
	require.Equal(t, ss[1], s)
	require.True(t, loaded)

	s, loaded = streams.LoadByFP(ss[1].fp)
	require.Equal(t, ss[1], s)
	require.True(t, loaded)

	require.Equal(t, len(ss), streams.Len())
}


func TestStreamsMap_LoadOrStoreNewNormalizesLabels(t *testing.T) {
	limits, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)
	limiter := NewLimiter(limits, NilMetrics, newIngesterRingLimiterStrategy(&ringCountMock{count: 1}, 1), &TenantBasedStrategy{limits: limits})
	retentionHours := util.RetentionHours(limiter.limits.RetentionPeriod("fake"))
	chunkfmt, headfmt := defaultChunkFormat(t)
	testStream := newStream(
		chunkfmt,
		headfmt,
		defaultConfig(),
		limiter.rateLimitStrategy,
		"fake",
		model.Fingerprint(1),
		labels.FromStrings("app", "loki", "job", "test"),
		NewStreamRateCalculator(),
		NilMetrics,
		nil,
		nil,
		retentionHours,
		noPolicy,
	)

	for _, input := range []string{
		`{app="loki",job="test"}`,
		`{job="test", app="loki"}`,
		`{app="loki", job="test", ignored=""}`,
	} {
		t.Run(input, func(t *testing.T) {
			streams := newStreamsMap()
			newStreamFn := func() (*stream, error) {
				return testStream, nil
			}

			first, loaded, err := streams.LoadOrStoreNew(input, newStreamFn, nil)
			require.NoError(t, err)
			require.False(t, loaded)
			require.Same(t, testStream, first)

			second, loaded, err := streams.LoadOrStoreNew(input, newStreamFn, nil)
			require.NoError(t, err)
			require.True(t, loaded)
			require.Same(t, first, second)

			require.Equal(t, 1, streams.Len())
			require.True(t, streams.Delete(testStream))
			require.Equal(t, 0, streams.Len())
		})
	}
}
