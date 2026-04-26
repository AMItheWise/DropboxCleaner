import { describe, expect, it } from 'vitest';
import { addUniquePath, normalizeDropboxPath, validateRunSettings } from './validation';
import type { RunSettings } from './types';

const baseSettings: RunSettings = {
  account_mode: 'personal',
  mode: 'dry_run',
  cutoff_date: '2020-05-01',
  date_filter_field: 'server_modified',
  archive_root: '/Archive_PreMay2020',
  output_dir: 'outputs',
  batch_size: 500,
  conflict_policy: 'safe_skip',
  include_folders_in_inventory: true,
  exclude_archive_destination: true,
  worker_count: 1,
  verify_after_run: true,
  team_coverage_preset: 'team_owned_only',
  team_archive_layout: 'segmented',
  source_roots: [],
  excluded_roots: [],
  retry: {
    max_retries: 5,
    initial_backoff_seconds: 1,
    backoff_multiplier: 2,
    max_backoff_seconds: 30,
  },
};

describe('wizard validation', () => {
  it('normalizes Dropbox paths', () => {
    expect(normalizeDropboxPath('Screenshots\\Old/')).toBe('/Screenshots/Old');
    expect(normalizeDropboxPath('')).toBe('/');
  });

  it('keeps root as whole Dropbox for include roots', () => {
    expect(addUniquePath(['/Photos'], '/', true)).toEqual([]);
    expect(addUniquePath([], '/', false)).toEqual([]);
  });

  it('rejects unsafe run settings', () => {
    expect(validateRunSettings({ ...baseSettings, archive_root: '/' })).toContain('archive folder');
    expect(validateRunSettings({ ...baseSettings, cutoff_date: '05/01/2020' })).toContain('YYYY-MM-DD');
    expect(validateRunSettings(baseSettings)).toBeNull();
  });
});
