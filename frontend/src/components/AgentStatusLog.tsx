/* ==========================================================================
   AgentStatusLog.tsx — Backend agent pipeline stepper
   Renders a vertical step log with pending / active / complete indicators.
   ========================================================================== */

import { motion } from 'framer-motion';

export interface AgentStep {
  id: string;
  label: string;
}

export interface AgentStatusLogProps {
  steps: AgentStep[];
  currentNode: string | null;
  allStepsComplete?: boolean;
  className?: string;
}

type StepState = 'pending' | 'active' | 'complete';

/** Derives visual state for a single pipeline step. */
function getStepState(
  stepId: string,
  currentNode: string | null,
  stepIndex: number,
  currentIndex: number,
  allStepsComplete: boolean,
): StepState {
  if (allStepsComplete) return 'complete';
  if (currentNode === stepId) return 'active';
  if (currentIndex === -1) return 'pending';
  if (stepIndex < currentIndex) return 'complete';
  return 'pending';
}

/** Vertical stepper listing agent execution steps and their live status. */
export function AgentStatusLog({
  steps,
  currentNode,
  allStepsComplete = false,
  className = '',
}: AgentStatusLogProps) {
  const currentIndex = currentNode
    ? steps.findIndex((s) => s.id === currentNode)
    : -1;

  return (
    <ol
      className={`space-y-4 ${className}`}
      aria-label="Agent execution progress"
    >
      {steps.map((step, index) => {
        const state = getStepState(
          step.id,
          currentNode,
          index,
          currentIndex,
          allStepsComplete,
        );

        return (
          <li key={step.id} className="flex items-start gap-3">
            <div className="relative mt-0.5 flex flex-col items-center">
              <StepIndicator state={state} />
              {index < steps.length - 1 && (
                <span
                  className="mt-2 h-8 w-px"
                  style={{
                    background:
                      state === 'complete'
                        ? 'rgba(107, 143, 113, 0.3)'
                        : 'rgba(255,255,255,0.08)',
                  }}
                />
              )}
            </div>

            <div className="pt-0.5">
              <p
                className="text-sm font-medium transition-colors"
                style={{
                  color:
                    state === 'active'
                      ? 'var(--green-accent-light)'
                      : state === 'complete'
                        ? 'rgba(255,255,255,0.45)'
                        : 'rgba(255,255,255,0.22)',
                }}
              >
                {step.label}
              </p>
              {state === 'active' && (
                <motion.p
                  initial={{ opacity: 0, y: 4 }}
                  animate={{ opacity: 1, y: 0 }}
                  className="mt-1 text-xs"
                  style={{ color: 'rgba(107, 143, 113, 0.75)' }}
                >
                  In progress…
                </motion.p>
              )}
            </div>
          </li>
        );
      })}
    </ol>
  );
}

/** Pulse / spin / checkmark icon for each step state. */
function StepIndicator({ state }: { state: StepState }) {
  if (state === 'complete') {
    return (
      <span
        className="flex h-5 w-5 items-center justify-center rounded-full"
        style={{
          background: 'rgba(107, 143, 113, 0.2)',
          border: '1px solid rgba(143, 174, 148, 0.4)',
        }}
      >
        <svg
          className="h-3 w-3"
          fill="none"
          viewBox="0 0 24 24"
          stroke="var(--green-accent-light)"
          strokeWidth={3}
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M5 13l4 4L19 7"
          />
        </svg>
      </span>
    );
  }

  if (state === 'active') {
    return (
      <span className="relative flex h-5 w-5 items-center justify-center">
        <motion.span
          className="absolute inset-0 rounded-full"
          style={{ background: 'rgba(107, 143, 113, 0.3)' }}
          animate={{ scale: [1, 1.6], opacity: [0.6, 0] }}
          transition={{ duration: 1.4, repeat: Infinity, ease: 'easeOut' }}
        />
        <motion.span
          className="h-5 w-5 rounded-full border-2"
          style={{
            borderColor: 'rgba(143, 174, 148, 0.3)',
            borderTopColor: 'var(--green-accent-dark)',
          }}
          animate={{ rotate: 360 }}
          transition={{ duration: 0.9, repeat: Infinity, ease: 'linear' }}
        />
      </span>
    );
  }

  return (
    <span
      className="h-5 w-5 rounded-full"
      style={{
        border: '1px solid rgba(255,255,255,0.12)',
        background: 'transparent',
      }}
    />
  );
}
