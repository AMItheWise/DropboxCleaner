import type { RunSettings } from './types';

export function normalizeDropboxPath(value: string): string {
  const trimmed = value.trim().replace(/\\/g, '/');
  if (!trimmed || trimmed === '/') return '/';
  const withSlash = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
  return withSlash.replace(/\/+/g, '/').replace(/\/$/, '') || '/';
}

export function addUniquePath(paths: string[], value: string, allowRoot: boolean): string[] {
  const normalized = normalizeDropboxPath(value);
  if (!allowRoot && normalized === '/') return paths;
  if (normalized === '/' && allowRoot) return [];
  return paths.includes(normalized) ? paths : [...paths, normalized];
}

export function validateRunSettings(settings: RunSettings): string | null {
  if (!settings.archive_root.trim() || normalizeDropboxPath(settings.archive_root) === '/') {
    return 'Choose a dedicated archive folder inside Dropbox.';
  }
  if (!settings.output_dir.trim()) {
    return 'Choose a local reports folder.';
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(settings.cutoff_date)) {
    return 'Use a cutoff date in YYYY-MM-DD format.';
  }
  if (settings.batch_size < 1 || settings.batch_size > 10000) {
    return 'Batch size must be between 1 and 10000.';
  }
  if (settings.worker_count < 1 || settings.worker_count > 8) {
    return 'Worker count must be between 1 and 8.';
  }
  return null;
}
