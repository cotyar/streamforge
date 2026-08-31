// Types + client for the API Explorer page's own backend surface (GET /api/meta/*). Kept out of
// ./types.ts (the frozen REST/hub contract shared with the rest of the console) since this is a
// new, Explorer-only surface — see src/StreamsForge.Host/Api/MetaEndpoints.cs for the DTOs this mirrors.
import { api, ApiError, environmentHeader, getStoredToken } from './client'

/** Raw text of one of the two hand-authored static .proto files (streamsforge.proto /
 * streamsforge_dynamic.proto), served verbatim by GET /api/meta/protos/static. */
export interface StaticProtoDto {
  name: string
  text: string
}

/** One live-reflectable dynamic entity (source/table/pipeline) as GET /api/meta/grpc reports it. */
export interface DynamicEntityMetaDto {
  kind: 'source' | 'table' | 'pipeline'
  /** id-or-name segment the REST proto-download routes key off (source: name, table/pipeline: id) —
   * same value protoPath is built from and what tools/generate-client.sh expects as its second arg. */
  id: string
  name: string
  /** "Enabled" | "Disabled" for sources, "Running" | "Stopped" | "Failed" for tables/pipelines. */
  status: string
  /** "source:{name}" | "table:{id}" | "pipeline:{id}" — the DynamicStreamService subscribe key. */
  entityKey: string
  messageName: string
  eventMessageName: string
  deltaMessageName: string
  /** REST path that downloads this entity's self-contained .proto (GET, Viewer). */
  protoPath: string
}

export interface GrpcMetaResponse {
  grpcPort: number
  services: string[]
  dynamicEntities: DynamicEntityMetaDto[]
}

export const metaApi = {
  staticProtos: () => api.get<StaticProtoDto[]>('/api/meta/protos/static'),
  grpc: () => api.get<GrpcMetaResponse>('/api/meta/grpc'),
}

/** Fetches a downloadable .proto endpoint's raw text (e.g. entity.protoPath) — unlike everything else
 * in ./client.ts, these routes return `text/plain`, not JSON, so the shared `api` helper can't be
 * reused as-is; this mirrors its auth/error handling for the one text-response case the Explorer needs. */
export async function fetchProtoText(path: string): Promise<string> {
  const headers: Record<string, string> = { ...environmentHeader() }
  const token = getStoredToken()
  if (token) headers.Authorization = `Bearer ${token}`

  const res = await fetch(path, { headers })
  if (!res.ok) {
    let message = res.statusText
    try {
      const data: unknown = await res.clone().json()
      if (data && typeof data === 'object' && typeof (data as Record<string, unknown>).error === 'string') {
        message = (data as Record<string, unknown>).error as string
      }
    } catch {
      // not JSON — fall through to statusText
    }
    throw new ApiError(res.status, message || `Request failed with status ${res.status}`)
  }
  return res.text()
}
