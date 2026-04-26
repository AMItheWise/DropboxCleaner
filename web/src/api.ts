export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function apiGet<T>(path: string): Promise<T> {
  return request<T>(path, { method: 'GET' });
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

export async function apiDelete<T>(path: string): Promise<T> {
  return request<T>(path, { method: 'DELETE' });
}

async function request<T>(path: string, init: RequestInit): Promise<T> {
  const response = await fetch(path, init);
  const contentType = response.headers.get('content-type') || '';
  const payload = contentType.includes('application/json') ? await response.json() : await response.text();
  if (!response.ok) {
    const detail = typeof payload === 'object' && payload !== null && 'detail' in payload ? String(payload.detail) : String(payload);
    throw new ApiError(response.status, detail || response.statusText);
  }
  return payload as T;
}

export function fileUrl(runId: string, name: string, outputDir: string): string {
  const params = new URLSearchParams({ output_dir: outputDir });
  return `/api/runs/${encodeURIComponent(runId)}/files/${encodeURIComponent(name)}?${params.toString()}`;
}
