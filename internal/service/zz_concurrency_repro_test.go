package service

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

// slowClock widens the critical section enough that parallel requests collide
// deterministically when the implementation leaves any read-then-write gap.
// Time advances one minute per call so explicitly timestamped observations
// stay ordered and inside the domain's clock-skew tolerance.
type slowClock struct {
	mu     sync.Mutex
	origin time.Time
	tick   int64
}

func (c *slowClock) Now() time.Time {
	time.Sleep(250 * time.Microsecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tick++
	return c.origin.Add(time.Duration(c.tick) * time.Minute).UTC()
}

type fixture struct {
	app   *Service
	dir   string
	store *storage.Store
	clock *slowClock
	base  time.Time
}

func setupFixture(t *testing.T) fixture {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	timeSource := &slowClock{origin: time.Now().UTC()}
	return fixture{
		app:   New(store, timeSource),
		dir:   dir,
		store: store,
		clock: timeSource,
	}
}

func (f fixture) formula(t *testing.T, name string) domain.Formula {
	t.Helper()
	formula, err := f.app.CreateFormula(CreateFormulaInput{
		Name:                  name,
		Style:                 "Pilot",
		TargetOriginalGravity: 1.052,
		TargetFinalGravity:    1.012,
		MaxFermentationTempC:  22,
		MinimumDays:           7,
		MaximumDays:           21,
	})
	if err != nil {
		t.Fatalf("create formula: %v", err)
	}
	steps := []domain.FormulaStep{{Sequence: 1, Label: "Primary", TemperatureC: 20, HoldMinutes: 60}}
	if _, err := f.app.SetFormulaSteps(formula.ID, SetFormulaStepsInput{Steps: steps}); err != nil {
		t.Fatalf("set steps: %v", err)
	}
	approved, err := f.app.ApproveFormula(formula.ID, VersionInput{})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	return approved
}

func (f fixture) vessel(t *testing.T, code string) domain.Vessel {
	t.Helper()
	vessel, err := f.app.CreateVessel(CreateVesselInput{Code: code, CapacityL: 300})
	if err != nil {
		t.Fatalf("create vessel: %v", err)
	}
	return vessel
}

func (f fixture) run(t *testing.T, code, formulaID string) domain.FermentationRun {
	t.Helper()
	run, err := f.app.CreateRun(CreateRunInput{
		Code: code, FormulaID: formulaID, PlannedVolumeL: 100,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	return run
}

func (f *fixture) startedRun(t *testing.T, code string) (domain.FermentationRun, domain.Vessel) {
	t.Helper()
	formula := f.formula(t, code+" Formula")
	vessel := f.vessel(t, "VSL-"+code)
	run := f.run(t, code, formula.ID)
	if _, _, err := f.app.ReserveRun(run.ID, ReserveRunInput{VesselID: vessel.ID}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	started, _, err := f.app.StartRun(run.ID, VersionInput{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if started.StartedAt == nil {
		t.Fatalf("started run must carry a start timestamp")
	}
	// Anchor explicit observation timestamps at the committed start time so
	// they pass the "not before run start" and clock-skew validations.
	f.base = started.StartedAt.UTC()
	return started, vessel
}

func (f fixture) assertRestartConsistency(t *testing.T, check func(*Service)) {
	t.Helper()
	store, err := storage.Open(f.dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	check(New(store, clock.System{}))
}

// Many planned runs race for one available vessel: exactly one reservation
// may win and the vessel must point at exactly that winning run.
func TestConcurrentReservationSingleWinner(t *testing.T) {
	f := setupFixture(t)
	formula := f.formula(t, "Reservation Formula")
	vessel := f.vessel(t, "VSL-RACE")

	const contenders = 16
	var runs []domain.FermentationRun
	for i := range contenders {
		runs = append(runs, f.run(t, fmt.Sprintf("RUN-RACE-%02d", i), formula.ID))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	successes := make(chan string, contenders)
	for _, run := range runs {
		wg.Add(1)
		go func(runID string) {
			defer wg.Done()
			<-start
			_, _, err := f.app.ReserveRun(runID, ReserveRunInput{VesselID: vessel.ID})
			if err == nil {
				successes <- runID
			}
		}(run.ID)
	}
	close(start)
	wg.Wait()
	close(successes)

	var winners []string
	for id := range successes {
		winners = append(winners, id)
	}
	if len(winners) != 1 {
		t.Fatalf("exactly one reservation must succeed, got %d", len(winners))
	}

	got, err := f.app.GetVessel(vessel.ID)
	if err != nil {
		t.Fatalf("get vessel: %v", err)
	}
	if got.State != domain.VesselReserved || got.ActiveRunID == nil ||
		*got.ActiveRunID != winners[0] {
		t.Fatalf("vessel must be reserved by the sole winner, got state %s run %v",
			got.State, got.ActiveRunID)
	}
	winner, err := f.app.GetRun(winners[0])
	if err != nil {
		t.Fatalf("get winner: %v", err)
	}
	if winner.State != domain.RunReserved || winner.VesselID == nil ||
		*winner.VesselID != vessel.ID {
		t.Fatalf("winning run must reference the vessel")
	}

	f.assertRestartConsistency(t, func(reloaded *Service) {
		reloadedVessel, err := reloaded.GetVessel(vessel.ID)
		if err != nil {
			t.Fatalf("reload vessel: %v", err)
		}
		if reloadedVessel.ActiveRunID == nil || *reloadedVessel.ActiveRunID != winners[0] {
			t.Fatalf("after restart vessel must still point at winner")
		}
	})
}

// Reservation must never re-occupy a vessel that was retired concurrently.
func TestReservationRacingRetire(t *testing.T) {
	f := setupFixture(t)
	formula := f.formula(t, "Retire Reservation Formula")

	const rounds = 32
	for range rounds {
		// Recreate a fresh available vessel and planned run each round.
		roundVessel := f.vessel(t, fmt.Sprintf("VSL-RET-%d", time.Now().UnixNano()))
		roundRun := f.run(t, fmt.Sprintf("RUN-RET-%d", time.Now().UnixNano()), formula.ID)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var reserveErr, retireErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _, reserveErr = f.app.ReserveRun(roundRun.ID,
				ReserveRunInput{VesselID: roundVessel.ID})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, retireErr = f.app.RetireVessel(roundVessel.ID, VersionInput{})
		}()
		close(start)
		wg.Wait()

		got, _ := f.app.GetVessel(roundVessel.ID)
		switch {
		case reserveErr == nil && retireErr == nil:
			t.Fatalf("reservation and retirement cannot both succeed")
		case reserveErr == nil:
			if got.State != domain.VesselReserved {
				t.Fatalf("won reservation but vessel state is %s", got.State)
			}
		case retireErr == nil:
			if got.State != domain.VesselRetired || got.ActiveRunID != nil {
				t.Fatalf("retired vessel must stay free, got state %s run %v",
					got.State, got.ActiveRunID)
			}
		default:
			t.Fatalf("one of reservation or retirement must succeed")
		}
	}
}

// Observations racing an abort/complete: a terminal run never gains an
// observation or returns to fermenting, and sequences stay contiguous.
func TestObservationRacingAbort(t *testing.T) {
	f := setupFixture(t)
	run, _ := f.startedRun(t, "OBSABORT")
	_, err := f.app.AppendObservation(run.ID, ObservationInput{
		Sequence: 1, Gravity: 1.050, TemperatureC: 20,
		ObservedAt: f.base,
	})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}

	const contenders = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	var abortedState domain.Vessel
	wg.Add(contenders + 1)
	go func() {
		defer wg.Done()
		<-start
		_, abortedState, _ = f.app.AbortRun(run.ID, VersionInput{})
	}()
	observationOK := make(chan struct{}, contenders)
	for i := 2; i < 2+contenders; i++ {
		sequence := i
		go func() {
			defer wg.Done()
			<-start
			_, err := f.app.AppendObservation(run.ID, ObservationInput{
				Sequence:     sequence,
				Gravity:      1.045,
				TemperatureC: 20,
				ObservedAt:   f.base.Add(time.Duration(sequence) * time.Second),
			})
			if err == nil {
				observationOK <- struct{}{}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(observationOK)

	got, err := f.app.GetRun(run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.State != domain.RunAborted {
		t.Fatalf("run must end aborted, got %s", got.State)
	}
	if len(got.Observations) != 1+len(observationOK) {
		t.Fatalf("accepted observations must match committed count, got %d",
			len(got.Observations))
	}
	for index, observation := range got.Observations {
		if observation.Sequence != index+1 {
			t.Fatalf("sequences must remain contiguous, got %d at index %d",
				observation.Sequence, index)
		}
	}
	vessel, _ := f.app.GetVessel(abortedState.ID)
	if vessel.State != domain.VesselCleaning {
		t.Fatalf("vessel for an aborted started run must be cleaning, got %s", vessel.State)
	}

	f.assertRestartConsistency(t, func(reloaded *Service) {
		reloadedRun, err := reloaded.GetRun(run.ID)
		if err != nil {
			t.Fatalf("reload run: %v", err)
		}
		if reloadedRun.State != domain.RunAborted {
			t.Fatalf("after restart run must remain aborted, got %s", reloadedRun.State)
		}
		if len(reloadedRun.Observations) != len(got.Observations) {
			t.Fatalf("after restart observation count changed: %d vs %d",
				len(reloadedRun.Observations), len(got.Observations))
		}
	})
}

// Two identical completions racing an abort: the terminal decision is made
// once and is never overwritten, including after a restart.
func TestCompletionRacingAbort(t *testing.T) {
	f := setupFixture(t)
	run, vessel := f.startedRun(t, "CMPABORT")
	for index, gravity := range []float64{1.050, 1.040, 1.020} {
		_, err := f.app.AppendObservation(run.ID, ObservationInput{
			Sequence: index + 1, Gravity: gravity, TemperatureC: 20,
			ObservedAt: f.base.Add(time.Duration(index+1) * time.Minute),
		})
		if err != nil {
			t.Fatalf("seed observation %d: %v", index+1, err)
		}
	}
	if _, err := f.app.BeginConditioning(run.ID, VersionInput{}); err != nil {
		t.Fatalf("conditioning: %v", err)
	}

	const finishers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(finishers + 1)
	completeOK := make(chan struct{}, finishers)
	abortOK := make(chan struct{}, 1)
	for range finishers {
		go func() {
			defer wg.Done()
			<-start
			_, _, err := f.app.CompleteRun(run.ID, VersionInput{})
			if err == nil {
				completeOK <- struct{}{}
			}
		}()
	}
	go func() {
		defer wg.Done()
		<-start
		if _, _, err := f.app.AbortRun(run.ID, VersionInput{}); err == nil {
			abortOK <- struct{}{}
		}
	}()
	close(start)
	wg.Wait()
	close(completeOK)
	close(abortOK)

	completedWins := len(completeOK) > 0
	abortedWins := len(abortOK) > 0
	if completedWins == abortedWins {
		t.Fatalf("exactly one terminal transition must win (complete=%d, abort=%t)",
			len(completeOK), abortedWins)
	}
	if len(completeOK) > 1 {
		t.Fatalf("completion must succeed at most once, got %d", len(completeOK))
	}

	got, err := f.app.GetRun(run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	finalVessel, _ := f.app.GetVessel(vessel.ID)
	switch {
	case completedWins:
		if got.State != domain.RunCompleted || got.Metrics == nil {
			t.Fatalf("winning completion must publish completed state and metrics")
		}
		if finalVessel.State != domain.VesselCleaning {
			t.Fatalf("completed run leaves the vessel cleaning, got %s", finalVessel.State)
		}
	case abortedWins:
		if got.State != domain.RunAborted || got.Metrics != nil {
			t.Fatalf("winning abort must leave an aborted run without metrics")
		}
	}

	f.assertRestartConsistency(t, func(reloaded *Service) {
		reloadedRun, err := reloaded.GetRun(run.ID)
		if err != nil {
			t.Fatalf("reload run: %v", err)
		}
		if reloadedRun.State != got.State {
			t.Fatalf("after restart state changed: %s vs %s",
				reloadedRun.State, got.State)
		}
		if (reloadedRun.Metrics == nil) != (got.Metrics == nil) {
			t.Fatalf("after restart metrics presence changed")
		}
		reloadedVessel, err := reloaded.GetVessel(vessel.ID)
		if err != nil {
			t.Fatalf("reload vessel: %v", err)
		}
		if reloadedVessel.State != finalVessel.State {
			t.Fatalf("after restart vessel state changed: %s vs %s",
				reloadedVessel.State, finalVessel.State)
		}
	})
}

// Versioned append: once another request commits, a stale X-Expected-Version
// must be rejected with revision_conflict and must not overwrite.
func TestObservationStaleExpectedVersion(t *testing.T) {
	f := setupFixture(t)
	run, _ := f.startedRun(t, "OBSVER")

	stale := run.Version // version before the first observation committed
	first, err := f.app.AppendObservation(run.ID, ObservationInput{
		Sequence: 1, Gravity: 1.050, TemperatureC: 20, ObservedAt: f.base,
	})
	if err != nil {
		t.Fatalf("first observation: %v", err)
	}
	if first.Version != stale+1 {
		t.Fatalf("successful append must publish the new version, got %d (was %d)",
			first.Version, stale)
	}
	_, err = f.app.AppendObservation(run.ID, ObservationInput{
		ExpectedVersion: &stale,
		Sequence:        2,
		Gravity:         1.045,
		TemperatureC:    20,
		ObservedAt:      f.base.Add(time.Minute),
	})
	if !domain.IsCode(err, domain.CodeRevisionConflict) {
		t.Fatalf("stale expected version must be revision_conflict, got %v", err)
	}

	current := first.Version
	updated, err := f.app.AppendObservation(run.ID, ObservationInput{
		ExpectedVersion: &current,
		Sequence:        2,
		Gravity:         1.045,
		TemperatureC:    20,
		ObservedAt:      f.base.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("current expected version must succeed, got %v", err)
	}
	if updated.Version != current+1 || len(updated.Observations) != 2 {
		t.Fatalf("accepted observation must advance version and sequence")
	}

	// The rejected append left nothing behind.
	got, err := f.app.GetRun(run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(got.Observations) != 2 {
		t.Fatalf("rejected observation must not be stored, got %d",
			len(got.Observations))
	}
}
