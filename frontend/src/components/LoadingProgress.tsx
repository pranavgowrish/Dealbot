/* ==========================================================================
   LoadingProgress.tsx — Scanning narrative + progress bar (visual only)
   ========================================================================== */

/** HARDCODED UI — replace with real pipeline stages from backend/SSE stream. */
const LOADER_STEPS = [
  'Parsing your request',
  'Scanning Facebook Marketplace',
  'Scanning Craigslist & OfferUp',
  'Scanning Mercari & Nextdoor',
  'Ranking your best matches',
  'Preparing your shortlist',
] as const;

interface LoadingProgressProps {
  progress: number;
  activeStep: number;
  listingCount: number;
}

export function LoadingProgress({
  progress,
  activeStep,
  listingCount,
}: LoadingProgressProps) {
  return (
    <div className="loader-panel">
      <div className="loader-panel-header">
        <span className="loader-panel-pulse" aria-hidden />
        <h2 className="loader-panel-title">Building your shortlist</h2>
        {/* HARDCODED UI — optionally interpolate {location} from form state via props. */}
        <p className="loader-panel-sub">
          Scanning sources near your location
        </p>
      </div>

      <div className="loader-progress-track" role="progressbar" aria-valuenow={Math.round(progress)} aria-valuemin={0} aria-valuemax={100}>
        <div
          className="loader-progress-fill"
          style={{ width: `${progress}%` }}
        />
      </div>

      <p className="loader-stat">
        {/* HARDCODED UI — listingCount is faked in App.tsx loader useEffect; wire to API. */}
        <span className="loader-stat-value">{listingCount}</span> candidates
        found so far
      </p>

      <ol className="loader-steps">
        {LOADER_STEPS.map((step, index) => {
          const isComplete = index < activeStep;
          const isActive = index === activeStep;

          return (
            <li
              key={step}
              className={`loader-step ${isComplete ? 'loader-step--complete' : ''} ${isActive ? 'loader-step--active' : ''}`}
            >
              <span className="loader-step-icon" aria-hidden>
                {isComplete ? '✓' : isActive ? '●' : '○'}
              </span>
              <span>{step}</span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}
