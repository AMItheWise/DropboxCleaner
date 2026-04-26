export type AccountMode = 'personal' | 'team_admin';
export type RunMode = 'inventory_only' | 'dry_run' | 'copy_run';
export type DateFilterField = 'server_modified' | 'client_modified' | 'oldest_modified';
export type TeamCoveragePreset = 'all_team_content' | 'team_owned_only';
export type TeamArchiveLayout = 'segmented' | 'merged';

export interface Choice {
  label: string;
  value: string;
  description: string;
}

export interface OptionsResponse {
  accounts: Choice[];
  run_modes: Choice[];
  date_filters: Choice[];
  team_coverage: Choice[];
  team_archive_layouts: Choice[];
  defaults: SettingsDefaults;
  packaged_app_key_available: boolean;
}

export interface SettingsDefaults {
  account_mode: AccountMode;
  mode: RunMode;
  cutoff_date: string;
  date_filter_field: DateFilterField;
  archive_root: string;
  output_dir: string;
  batch_size: number;
  conflict_policy: 'safe_skip' | 'abort_run';
  include_folders_in_inventory: boolean;
  exclude_archive_destination: boolean;
  worker_count: number;
  verify_after_run: boolean;
  team_coverage_preset: TeamCoveragePreset;
  team_archive_layout: TeamArchiveLayout;
}

export interface AccountInfo {
  account_id: string;
  display_name: string;
  email?: string | null;
  account_mode: AccountMode;
  team_name?: string | null;
  team_model?: string | null;
  active_member_count: number;
  namespace_count: number;
}

export interface AuthStatus {
  saved_credentials_available: boolean;
  account_mode?: AccountMode | null;
  app_key?: string | null;
  admin_member_id?: string | null;
  packaged_app_key_available: boolean;
}

export interface BrowserLocation {
  display_path: string;
  namespace_id?: string | null;
  namespace_path: string;
  title: string;
  view_mode: string;
}

export interface BrowserFolder {
  name: string;
  display_path: string;
  namespace_id?: string | null;
  namespace_path: string;
  namespace_type: string;
  subtitle: string;
}

export interface FolderListResponse {
  location: BrowserLocation;
  parent: BrowserLocation;
  folders: BrowserFolder[];
  advanced_team_locations_available: boolean;
}

export interface ProgressSnapshot {
  phase: string;
  message: string;
  counters: Record<string, number>;
  outputs: Record<string, string>;
  level: string;
  extra: Record<string, unknown>;
}

export interface Metric {
  label: string;
  value: number;
  tone: string;
}

export interface FolderResult {
  folder: string;
  matched: number;
  copied: number;
  failed: number;
  skipped: number;
  total_size: number;
}

export interface RunResultPayload {
  run_id: string;
  mode: string;
  created_at: string;
  success_message: string;
  review_title: string;
  has_issues: boolean;
  has_skipped_details: boolean;
  metrics: Metric[];
  top_folders: FolderResult[];
  already_archived: string[];
  conflicts: string[];
  failures: string[];
  blocked: string[];
  verification: Record<string, unknown>;
  output_files: string[];
}

export interface RunStatus {
  run_id: string;
  status: 'running' | 'completed' | 'failed' | 'cancelled' | string;
  kind: string;
  actual_run_id?: string | null;
  run_dir?: string | null;
  error?: string | null;
  result?: RunResultPayload | null;
}

export interface RunHistoryItem {
  run_id: string;
  mode: string;
  created_at: string;
  run_dir: string;
  latest: boolean;
  status_message: string;
  metrics: Metric[];
  has_issues: boolean;
}

export interface RunHistoryResponse {
  output_dir: string;
  latest_run_id?: string | null;
  runs: RunHistoryItem[];
}

export interface RunSettings extends SettingsDefaults {
  source_roots: string[];
  excluded_roots: string[];
  retry: {
    max_retries: number;
    initial_backoff_seconds: number;
    backoff_multiplier: number;
    max_backoff_seconds: number;
  };
}
