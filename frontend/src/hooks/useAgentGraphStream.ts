import { useEffect, useMemo, useState } from 'react';
import mqtt, { type MqttClient } from 'mqtt';

export type WorkerStatus = 'idle' | 'active' | 'success' | 'killed';

export interface WorkerGraphState {
  id: string;
  status: WorkerStatus;
  mode: string;
  directive: string;
  thinking: string;
  lastMessage: string;
  updatedAtMs: number;
}

export interface ManagerGraphState {
  thinking: string;
  brief: string;
  focusWorker: string | null;
  updatedAtMs: number;
}

export interface EdgePulse {
  id: string;
  workerId: string;
  direction: 'manager_to_worker' | 'worker_to_manager';
  text: string;
  createdAtMs: number;
}

export interface AgentGraphEvent {
  event_id?: string;
  event_type?: string;
  actor_type?: string;
  actor_id?: string;
  summary?: string;
  details?: Record<string, unknown>;
  published_at_ms?: number;
}

type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'error';

export interface UseAgentGraphStreamResult {
  connectionStatus: ConnectionStatus;
  connectionError: string | null;
  manager: ManagerGraphState;
  workers: Record<string, WorkerGraphState>;
  pulses: EdgePulse[];
  eventLog: string[];
  topic: string | null;
}

const WORKER_IDS = ['worker1', 'worker2', 'worker3', 'worker4', 'worker5'] as const;
const EVENT_LOG_LIMIT = 40;
const PULSE_LIMIT = 24;

function blankWorker(workerId: string): WorkerGraphState {
  return {
    id: workerId,
    status: 'idle',
    mode: 'idle',
    directive: 'Awaiting manager directive',
    thinking: 'Waiting for assignment',
    lastMessage: '',
    updatedAtMs: Date.now(),
  };
}

function initialWorkers(): Record<string, WorkerGraphState> {
  return Object.fromEntries(WORKER_IDS.map((id) => [id, blankWorker(id)]));
}

function toText(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return '';
}

function clampText(value: string, limit = 130): string {
  if (value.length <= limit) return value;
  return `${value.slice(0, limit - 1)}...`;
}

export function useAgentGraphStream(jobId: string | null): UseAgentGraphStreamResult {
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('idle');
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [manager, setManager] = useState<ManagerGraphState>({
    thinking: 'Awaiting orchestration startup',
    brief: '',
    focusWorker: null,
    updatedAtMs: Date.now(),
  });
  const [workers, setWorkers] = useState<Record<string, WorkerGraphState>>(initialWorkers);
  const [pulses, setPulses] = useState<EdgePulse[]>([]);
  const [eventLog, setEventLog] = useState<string[]>([]);

  const brokerUrl = import.meta.env.VITE_DEALBOT_MQTT_WS_URL || 'ws://localhost:9001';
  const topicRoot = (
    import.meta.env.VITE_DEALBOT_MQTT_TOPIC_ROOT || 'dealbot/agent-events'
  ).replace(/\/+$/, '');
  const topic = useMemo(
    () => (jobId ? `${topicRoot}/${jobId}` : null),
    [jobId, topicRoot],
  );

  useEffect(() => {
    setWorkers(initialWorkers());
    setPulses([]);
    setEventLog([]);
    setConnectionError(null);
    if (!jobId || !topic) {
      setConnectionStatus('idle');
      setManager({
        thinking: 'Awaiting orchestration startup',
        brief: '',
        focusWorker: null,
        updatedAtMs: Date.now(),
      });
      return;
    }

    let disposed = false;
    setConnectionStatus('connecting');
    setManager({
      thinking: 'Connecting to live MQTT event stream',
      brief: '',
      focusWorker: null,
      updatedAtMs: Date.now(),
    });

    const client: MqttClient = mqtt.connect(brokerUrl, {
      reconnectPeriod: 2000,
      connectTimeout: 6000,
      keepalive: 30,
      clean: true,
      clientId: `dealbot-ui-${Math.random().toString(16).slice(2, 10)}`,
      username: import.meta.env.VITE_DEALBOT_MQTT_USERNAME || undefined,
      password: import.meta.env.VITE_DEALBOT_MQTT_PASSWORD || undefined,
    });

    const upsertWorker = (
      workerId: string,
      updater: (worker: WorkerGraphState) => WorkerGraphState,
    ) => {
      setWorkers((prev) => {
        const existing = prev[workerId] || blankWorker(workerId);
        return { ...prev, [workerId]: updater(existing) };
      });
    };

    const pushEvent = (line: string) => {
      setEventLog((prev) => [line, ...prev].slice(0, EVENT_LOG_LIMIT));
    };

    const pushPulse = (pulse: EdgePulse) => {
      setPulses((prev) => [pulse, ...prev].slice(0, PULSE_LIMIT));
    };

    client.on('connect', () => {
      if (disposed) return;
      setConnectionStatus('connected');
      setConnectionError(null);
      setManager((prev) => ({
        ...prev,
        thinking: 'Live stream connected',
        updatedAtMs: Date.now(),
      }));
      client.subscribe(topic, { qos: 0 }, (error) => {
        if (!error || disposed) return;
        setConnectionStatus('error');
        setConnectionError(error.message);
      });
    });

    client.on('error', (error) => {
      if (disposed) return;
      setConnectionStatus('error');
      setConnectionError(error.message);
    });

    client.on('message', (_incomingTopic, payload) => {
      if (disposed) return;
      let event: AgentGraphEvent;
      try {
        event = JSON.parse(payload.toString()) as AgentGraphEvent;
      } catch {
        return;
      }
      const nowMs = Date.now();
      const eventType = toText(event.event_type);
      const actorType = toText(event.actor_type);
      const actorId = toText(event.actor_id);
      const details = event.details ?? {};
      const summary = clampText(toText(event.summary) || eventType || 'event');
      const detailWorker = toText(details.worker_id);
      const workerId = detailWorker || (actorId.startsWith('worker') ? actorId : '');
      const directive = clampText(toText(details.instruction));
      const mode = toText(details.mode) || toText(details.manager_mode);
      const thought = clampText(
        toText(details.thought) ||
          toText(details.analysis_answer) ||
          toText(details.manager_brief) ||
          summary,
      );
      const messageText = clampText(
        toText(details.message) ||
          toText(details.sent_message) ||
          toText(details.reply_message),
      );

      pushEvent(summary);

      if (eventType === 'manager_thinking') {
        setManager((prev) => ({
          ...prev,
          thinking: thought,
          brief: clampText(toText(details.manager_brief) || summary),
          focusWorker: toText(details.focus_worker) || null,
          updatedAtMs: nowMs,
        }));
      }

      if (eventType === 'manager_directive' && workerId) {
        setManager((prev) => ({
          ...prev,
          thinking: `Directing ${workerId}`,
          brief: directive || summary,
          focusWorker: toText(details.focus_worker) || prev.focusWorker,
          updatedAtMs: nowMs,
        }));
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: worker.status === 'killed' ? 'killed' : 'active',
          mode: mode || worker.mode,
          directive: directive || worker.directive,
          updatedAtMs: nowMs,
        }));
        pushPulse({
          id: toText(event.event_id) || `${workerId}-directive-${nowMs}`,
          workerId,
          direction: 'manager_to_worker',
          text: directive || 'Directive assigned',
          createdAtMs: nowMs,
        });
      }

      if (eventType === 'worker_initialized' && workerId) {
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: 'active',
          thinking: 'Initialized listing context',
          updatedAtMs: nowMs,
        }));
      }

      if (eventType === 'worker_thinking' && workerId) {
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: worker.status === 'killed' ? 'killed' : 'active',
          thinking: thought || worker.thinking,
          mode: mode || worker.mode,
          updatedAtMs: nowMs,
        }));
        pushPulse({
          id: toText(event.event_id) || `${workerId}-thinking-${nowMs}`,
          workerId,
          direction: 'worker_to_manager',
          text: thought || 'Thinking...',
          createdAtMs: nowMs,
        });
      }

      if ((eventType === 'message_sent' || eventType === 'message_received') && workerId) {
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: worker.status === 'killed' ? 'killed' : 'active',
          lastMessage: messageText || worker.lastMessage,
          updatedAtMs: nowMs,
        }));
        pushPulse({
          id: toText(event.event_id) || `${workerId}-${eventType}-${nowMs}`,
          workerId,
          direction: eventType === 'message_sent' ? 'worker_to_manager' : 'manager_to_worker',
          text: messageText || summary,
          createdAtMs: nowMs,
        });
      }

      if (eventType === 'worker_price_update' && workerId) {
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: worker.status === 'killed' ? 'killed' : 'active',
          thinking: clampText(
            `Lowered floor to $${toText(details.new_floor_price) || '?'} while targeting $${toText(details.target_budget) || '?'}`,
          ),
          updatedAtMs: nowMs,
        }));
      }

      if (eventType === 'worker_success' && workerId) {
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: 'success',
          thinking: 'Reached budget-compatible deal',
          updatedAtMs: nowMs,
        }));
      }

      if (eventType === 'worker_killed' && workerId) {
        upsertWorker(workerId, (worker) => ({
          ...worker,
          status: 'killed',
          mode: 'killed',
          directive: 'Worker retired by manager',
          thinking: 'Execution stopped',
          updatedAtMs: nowMs,
        }));
      }

      if (eventType === 'job_completed' || eventType === 'negotiation_completed') {
        setManager((prev) => ({
          ...prev,
          thinking: 'Negotiation loop completed',
          brief: summary,
          updatedAtMs: nowMs,
        }));
      }

      if (eventType === 'job_failed') {
        setManager((prev) => ({
          ...prev,
          thinking: 'Negotiation failed',
          brief: summary,
          updatedAtMs: nowMs,
        }));
      }

      if (actorType === 'manager' && !eventType.startsWith('manager_')) {
        setManager((prev) => ({
          ...prev,
          brief: summary,
          updatedAtMs: nowMs,
        }));
      }
    });

    return () => {
      disposed = true;
      client.end(true);
    };
  }, [brokerUrl, jobId, topic]);

  return {
    connectionStatus,
    connectionError,
    manager,
    workers,
    pulses,
    eventLog,
    topic,
  };
}
