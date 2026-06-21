/* ==========================================================================
   JourneyStepper.tsx — Visual progress rail across the 4-screen flow
   ========================================================================== */

/** HARDCODED UI — replace step labels/copy when flow changes; index mapping lives in App journeyIndex(). */
const STEPS = ['Describe', 'Scan', 'Pick', 'Hunt live'] as const;

interface JourneyStepperProps {
  activeIndex: number;
}

export function JourneyStepper({ activeIndex }: JourneyStepperProps) {
  return (
    <nav className="journey-stepper" aria-label="Hunt progress">
      <ol className="journey-stepper-list">
        {STEPS.map((label, index) => {
          const isComplete = index < activeIndex;
          const isActive = index === activeIndex;

          return (
            <li
              key={label}
              className={`journey-step ${isComplete ? 'journey-step--complete' : ''} ${isActive ? 'journey-step--active' : ''}`}
            >
              <span className="journey-step-dot" aria-hidden />
              <span className="journey-step-label">{label}</span>
              {index < STEPS.length - 1 && (
                <span className="journey-step-connector" aria-hidden />
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
