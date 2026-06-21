import { useEffect, useMemo, useState } from 'react';
import { motion } from 'framer-motion';
import {
  useAgentGraphStream,
  type WorkerGraphState,
} from '../hooks/useAgentGraphStream';

interface AgentNegotiationGraphProps {
  jobId: string | null;
}

type WorkerPosition = {
  id: string;
  x: number;
  y: number;
};

const MANAGER_X = 50;
const MANAGER_Y = 18;
const PULSE_TTL_MS = 6500;

const WORKER_LAYOUT: WorkerPosition[] = [
  { id: 'worker1', x: 16, y: 58 },
  { id: 'worker2', x: 33, y: 76 },
  { id: 'worker3', x: 50, y: 84 },
  { id: 'worker4', x: 67, y: 76 },
  { id: 'worker5', x: 84, y: 58 },
];

function displayWorkerName(workerId: string): string {
  const suffix = workerId.replace('worker', '');
  return `Worker ${suffix || workerId}`;
}

function fallbackWorker(workerId: string): WorkerGraphState {
  return {
    id: workerId,
    status: 'idle',
    mode: 'idle',
    directive: 'Awaiting assignment',
    thinking: 'No telemetry yet',
    lastMessage: '',
    updatedAtMs: Date.now(),
  };
}

export function AgentNegotiationGraph({ jobId }: AgentNegotiationGraphProps) {
  const {
    connectionStatus,
    connectionError,
    manager,
    workers,
    pulses,
    eventLog,
    topic,
  } = useAgentGraphStream(jobId);
  const [nowMs, setNowMs] = useState(Date.now());

  useEffect(() => {
    const interval = window.setInterval(() => setNowMs(Date.now()), 500);
    return () => window.clearInterval(interval);
  }, []);

  const activePulses = useMemo(
    () => pulses.filter((pulse) => nowMs - pulse.createdAtMs <= PULSE_TTL_MS),
    [nowMs, pulses],
  );
  const latestPulseByWorker = useMemo(() => {
    const map = new Map<string, (typeof activePulses)[number]>();
    for (const pulse of activePulses) {
      if (!map.has(pulse.workerId)) map.set(pulse.workerId, pulse);
    }
    return map;
  }, [activePulses]);

  return (
    <section className="agentic-view" aria-live="polite">
      <header className="agentic-header">
        <div>
          <p className="agentic-eyebrow">Agent Negotiation Live Graph</p>
          <h2 className="agentic-title">Manager supervising five workers</h2>
          <p className="agentic-subtitle">{manager.brief || manager.thinking}</p>
        </div>
        <div className="agentic-connection-badges">
          <span className={`agentic-badge agentic-badge--${connectionStatus}`}>
            MQTT {connectionStatus}
          </span>
          {topic && <span className="agentic-badge">{topic}</span>}
          {connectionError && (
            <span className="agentic-badge agentic-badge--error">{connectionError}</span>
          )}
        </div>
      </header>

      <div className="agentic-grid">
        <div className="agentic-graph-shell">
          <svg
            viewBox="0 0 100 100"
            className="agentic-edge-layer"
            aria-label="Manager to worker links"
          >
            {WORKER_LAYOUT.map((node) => {
              const worker = workers[node.id] || fallbackWorker(node.id);
              const pulse = latestPulseByWorker.get(node.id);
              if (worker.status === 'killed') return null;
              return (
                <line
                  key={node.id}
                  x1={MANAGER_X}
                  y1={MANAGER_Y}
                  x2={node.x}
                  y2={node.y}
                  className={`agentic-edge ${pulse ? 'agentic-edge--active' : ''}`}
                />
              );
            })}
          </svg>

          <div
            className="agent-node agent-node--manager"
            style={{ left: `${MANAGER_X}%`, top: `${MANAGER_Y}%` }}
          >
            <span className="agent-node-role">Manager</span>
            <p className="agent-node-title">Supervisor</p>
            <p className="agent-node-copy">{manager.thinking}</p>
          </div>

          {WORKER_LAYOUT.map((node) => {
            const worker = workers[node.id] || fallbackWorker(node.id);
            const pulse = latestPulseByWorker.get(node.id);
            const isKilled = worker.status === 'killed';
            return (
              <div
                key={node.id}
                className={`agent-node agent-node--worker ${isKilled ? 'agent-node--killed' : ''} ${
                  worker.status === 'success' ? 'agent-node--success' : ''
                }`}
                style={{ left: `${node.x}%`, top: `${node.y}%` }}
              >
                <span className="agent-node-role">{displayWorkerName(worker.id)}</span>
                <p className="agent-node-title">{worker.mode}</p>
                <p className="agent-node-copy">{worker.directive}</p>
                <p className="agent-node-copy agent-node-copy--muted">{worker.thinking}</p>
                {worker.lastMessage && (
                  <p className="agent-node-last-message">{worker.lastMessage}</p>
                )}
                {pulse && !isKilled && (
                  <motion.span
                    key={pulse.id}
                    initial={{ opacity: 0, y: 6 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0 }}
                    className="agent-edge-pulse"
                  >
                    {pulse.direction === 'manager_to_worker' ? 'M→W' : 'W→M'} {pulse.text}
                  </motion.span>
                )}
              </div>
            );
          })}
        </div>

        <aside className="agentic-log-panel">
          <p className="agentic-log-heading">Live Event Feed</p>
          {eventLog.length === 0 ? (
            <p className="agentic-log-empty">Waiting for live agent telemetry...</p>
          ) : (
            <ul className="agentic-log-list">
              {eventLog.slice(0, 12).map((line, index) => (
                <li key={`${line}-${index}`} className="agentic-log-item">
                  {line}
                </li>
              ))}
            </ul>
          )}
        </aside>
      </div>
    </section>
  );
}
