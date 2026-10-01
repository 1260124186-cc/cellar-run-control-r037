package service

import "github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"

func (s *Service) AppendObservation(
	id string,
	input ObservationInput,
) (domain.FermentationRun, error) {
	var committed domain.FermentationRun
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		receivedAt := s.clock.Now()
		observation := domain.GravityObservation{
			RunID:        run.ID,
			Sequence:     input.Sequence,
			Gravity:      input.Gravity,
			TemperatureC: input.TemperatureC,
			ObservedAt:   input.ObservedAt.UTC(),
			Note:         input.Note,
			ReceivedAt:   receivedAt,
		}
		// Validate against the committed run (state, sequence, and time
		// ordering) inside the write transaction so a transition committed by
		// a concurrent request makes this write fail instead of overwriting it.
		if err := run.AddObservation(observation); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		committed = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return committed, nil
}

func (s *Service) BeginConditioning(
	id string,
	input VersionInput,
) (domain.FermentationRun, error) {
	var updated domain.FermentationRun
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		if err := run.BeginConditioning(s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		updated = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return updated, nil
}

func (s *Service) CompleteRun(
	id string,
	input VersionInput,
) (domain.FermentationRun, domain.Vessel, error) {
	var committedRun domain.FermentationRun
	var committedVessel domain.Vessel
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		formula, ok := snapshot.Formulas[run.FormulaID]
		if !ok {
			return domain.NewError(domain.CodeFormulaNotFound, "attached formula was not found")
		}
		if run.VesselID == nil {
			return domain.NewError(domain.CodeRunState, "run has no vessel")
		}
		vessel, ok := snapshot.Vessels[*run.VesselID]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		stamp := s.clock.Now()
		// Both aggregate transitions are applied to committed state in this
		// transaction; an abort or observation committed in the meantime turns
		// one of these checks into a stable failure with nothing written.
		if err := run.Complete(formula.TargetOriginalGravity, stamp); err != nil {
			return err
		}
		if err := vessel.StartCleaning(run.ID, stamp); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		snapshot.Vessels[vessel.ID] = vessel
		committedRun = run.Clone()
		committedVessel = vessel
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, domain.Vessel{}, err
	}
	return committedRun, committedVessel, nil
}
