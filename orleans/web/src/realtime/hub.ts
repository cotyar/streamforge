// Singleton SignalR connection manager for the /hubs/stream hub.
// Connects lazily — the first subscribe*() call after login triggers connection
// setup; accessTokenFactory reads the current bearer token from localStorage on
// every (re)connect attempt, so no explicit wiring to the auth module is needed.
import * as signalR from '@microsoft/signalr'
import type { PipelineMetrics, PipelineStatus, ResultEnvelope, ResultRow } from '../api/types'
import { getStoredToken } from '../api/client'

type RowsHandler = (rows: ResultEnvelope[]) => void
type StatusHandler = (status: PipelineStatus) => void
type MetricsHandler = (metrics: PipelineMetrics) => void
type SourceHandler = (row: ResultRow) => void
type Unsubscribe = () => void

let connection: signalR.HubConnection | null = null
let connectPromise: Promise<signalR.HubConnection> | null = null

const pipelineRowHandlers = new Map<string, Set<RowsHandler>>()
const pipelineStatusHandlers = new Map<string, Set<StatusHandler>>()
const pipelineRefCounts = new Map<string, number>()

const sourceHandlers = new Map<string, Set<SourceHandler>>()
const sourceRefCounts = new Map<string, number>()

const metricsHandlers = new Set<MetricsHandler>()
let metricsRefCount = 0

function registerListeners(conn: signalR.HubConnection) {
  conn.on('pipelineResult', (pipelineId: string, rows: ResultEnvelope[]) => {
    pipelineRowHandlers.get(pipelineId)?.forEach((h) => h(rows))
  })
  conn.on('pipelineStatus', (pipelineId: string, status: PipelineStatus) => {
    pipelineStatusHandlers.get(pipelineId)?.forEach((h) => h(status))
  })
  conn.on('pipelineMetrics', (metrics: PipelineMetrics) => {
    metricsHandlers.forEach((h) => h(metrics))
  })
  conn.on('sourceEvent', (sourceName: string, row: ResultRow) => {
    sourceHandlers.get(sourceName)?.forEach((h) => h(row))
  })

  conn.onreconnected(() => {
    for (const id of pipelineRefCounts.keys()) {
      void conn.invoke('subscribePipeline', id)
    }
    for (const name of sourceRefCounts.keys()) {
      void conn.invoke('subscribeSource', name)
    }
    if (metricsRefCount > 0) {
      void conn.invoke('subscribeMetrics')
    }
  })
}

function getConnection(): Promise<signalR.HubConnection> {
  if (connection) return Promise.resolve(connection)
  if (connectPromise) return connectPromise

  const conn = new signalR.HubConnectionBuilder()
    .withUrl('/hubs/stream', {
      accessTokenFactory: () => getStoredToken() ?? '',
    })
    .withAutomaticReconnect()
    .build()

  registerListeners(conn)

  connectPromise = conn
    .start()
    .then(() => {
      connection = conn
      connectPromise = null
      return conn
    })
    .catch((err) => {
      connectPromise = null
      throw err
    })

  return connectPromise
}

/** Stops the connection (if any) and clears all subscription state. Call on logout. */
export async function disconnectHub(): Promise<void> {
  const conn = connection
  connection = null
  connectPromise = null
  pipelineRowHandlers.clear()
  pipelineStatusHandlers.clear()
  pipelineRefCounts.clear()
  sourceHandlers.clear()
  sourceRefCounts.clear()
  metricsHandlers.clear()
  metricsRefCount = 0
  if (conn) {
    try {
      await conn.stop()
    } catch {
      // ignore — connection is being torn down anyway
    }
  }
}

export function subscribePipeline(id: string, onRows: RowsHandler, onStatus?: StatusHandler): Unsubscribe {
  if (!pipelineRowHandlers.has(id)) pipelineRowHandlers.set(id, new Set())
  pipelineRowHandlers.get(id)!.add(onRows)
  if (onStatus) {
    if (!pipelineStatusHandlers.has(id)) pipelineStatusHandlers.set(id, new Set())
    pipelineStatusHandlers.get(id)!.add(onStatus)
  }

  const wasZero = (pipelineRefCounts.get(id) ?? 0) === 0
  pipelineRefCounts.set(id, (pipelineRefCounts.get(id) ?? 0) + 1)

  if (wasZero) {
    void getConnection().then((conn) => {
      if ((pipelineRefCounts.get(id) ?? 0) > 0) {
        void conn.invoke('subscribePipeline', id)
      }
    })
  }

  let done = false
  return () => {
    if (done) return
    done = true
    pipelineRowHandlers.get(id)?.delete(onRows)
    if (onStatus) pipelineStatusHandlers.get(id)?.delete(onStatus)
    const next = Math.max(0, (pipelineRefCounts.get(id) ?? 1) - 1)
    if (next === 0) {
      pipelineRefCounts.delete(id)
      pipelineRowHandlers.delete(id)
      pipelineStatusHandlers.delete(id)
      if (connection) void connection.invoke('unsubscribePipeline', id)
    } else {
      pipelineRefCounts.set(id, next)
    }
  }
}

export function subscribeSource(name: string, onEvent: SourceHandler): Unsubscribe {
  if (!sourceHandlers.has(name)) sourceHandlers.set(name, new Set())
  sourceHandlers.get(name)!.add(onEvent)

  const wasZero = (sourceRefCounts.get(name) ?? 0) === 0
  sourceRefCounts.set(name, (sourceRefCounts.get(name) ?? 0) + 1)

  if (wasZero) {
    void getConnection().then((conn) => {
      if ((sourceRefCounts.get(name) ?? 0) > 0) {
        void conn.invoke('subscribeSource', name)
      }
    })
  }

  let done = false
  return () => {
    if (done) return
    done = true
    sourceHandlers.get(name)?.delete(onEvent)
    const next = Math.max(0, (sourceRefCounts.get(name) ?? 1) - 1)
    if (next === 0) {
      sourceRefCounts.delete(name)
      sourceHandlers.delete(name)
      if (connection) void connection.invoke('unsubscribeSource', name)
    } else {
      sourceRefCounts.set(name, next)
    }
  }
}

export function subscribeMetrics(onMetrics: MetricsHandler): Unsubscribe {
  metricsHandlers.add(onMetrics)
  const wasZero = metricsRefCount === 0
  metricsRefCount += 1

  if (wasZero) {
    void getConnection().then((conn) => {
      if (metricsRefCount > 0) void conn.invoke('subscribeMetrics')
    })
  }

  let done = false
  return () => {
    if (done) return
    done = true
    metricsHandlers.delete(onMetrics)
    metricsRefCount = Math.max(0, metricsRefCount - 1)
    // Hub contract exposes no unsubscribeMetrics — the server keeps pushing at
    // low cost; simply stop forwarding to local listeners once none remain.
  }
}
