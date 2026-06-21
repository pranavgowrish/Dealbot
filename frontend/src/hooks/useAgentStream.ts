import { useCallback, useEffect, useRef, useState } from 'react';

export type AgentStreamStatus =
  | 'idle'
  | 'searching'
  | 'streaming'
  | 'complete';

export interface UseAgentStreamOptions {
  /** When true, simulates backend flow with timers (demo mode). */
  simulate?: boolean;
}

export interface UseAgentStreamReturn {
  status: AgentStreamStatus;
  currentNode: string | null;
  streamedResponse: string;
  error: string | null;
  submitQuery: (query: string) => void;
  reset: () => void;
}

export const AGENT_STEPS = [
  { id: 'redis-cache', label: 'Checking Redis Cache' },
  { id: 'band-oracle', label: 'Fetching Band Protocol Oracle Data' },
  {
    id: 'langgraph-routing',
    label: 'Executing LangGraph Multi-Agent Routing',
  },
  { id: 'arize-traces', label: 'Evaluating Arize Phoenix Traces' },
] as const;

const SIMULATED_RESPONSE =
  'Based on current oracle pricing and cached market signals, DealBot recommends negotiating ' +
  'a 12–15% discount on this SaaS renewal. Comparable contracts in your segment closed at ' +
  '$42k–$48k ARR with net-30 terms and a 90-day opt-out clause.';

export function useAgentStream(
  options: UseAgentStreamOptions = {},
): UseAgentStreamReturn {
  const { simulate = true } = options;

  const [status, setStatus] = useState<AgentStreamStatus>('idle');
  const [currentNode, setCurrentNode] = useState<string | null>(null);
  const [streamedResponse, setStreamedResponse] = useState('');
  const [error, setError] = useState<string | null>(null);

  const timersRef = useRef<ReturnType<typeof setTimeout>[]>([]);
  const intervalsRef = useRef<ReturnType<typeof setInterval>[]>([]);

  const clearTimers = useCallback(() => {
    timersRef.current.forEach(clearTimeout);
    intervalsRef.current.forEach(clearInterval);
    timersRef.current = [];
    intervalsRef.current = [];
  }, []);

  const reset = useCallback(() => {
    clearTimers();
    setStatus('idle');
    setCurrentNode(null);
    setStreamedResponse('');
    setError(null);
  }, [clearTimers]);

  const submitQuery = useCallback(
    (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;

      clearTimers();
      setError(null);
      setStreamedResponse('');
      setStatus('searching');
      setCurrentNode(AGENT_STEPS[0]?.id ?? null);

      if (!simulate) {
        return;
      }

      let stepIndex = 0;
      const stepInterval = setInterval(() => {
        stepIndex += 1;
        if (stepIndex < AGENT_STEPS.length) {
          setCurrentNode(AGENT_STEPS[stepIndex].id);
        } else {
          clearInterval(stepInterval);
          setCurrentNode(null);
          setStatus('streaming');

          let charIndex = 0;
          const streamInterval = setInterval(() => {
            charIndex += 1;
            setStreamedResponse(SIMULATED_RESPONSE.slice(0, charIndex));
            if (charIndex >= SIMULATED_RESPONSE.length) {
              clearInterval(streamInterval);
              setStatus('complete');
            }
          }, 18);

          intervalsRef.current.push(streamInterval);
        }
      }, 1400);

      intervalsRef.current.push(stepInterval);
    },
    [clearTimers, simulate],
  );

  useEffect(() => () => clearTimers(), [clearTimers]);

  return {
    status,
    currentNode,
    streamedResponse,
    error,
    submitQuery,
    reset,
  };
}
