import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  AlertTriangle,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  FolderOpen,
  Loader2,
  LogOut,
  Play,
  RotateCcw,
  ShieldCheck,
  Square,
  X,
} from 'lucide-react';
import { ApiError, apiDelete, apiGet, apiPost, fileUrl } from './api';
import { addUniquePath, normalizeDropboxPath, validateRunSettings } from './validation';
import type {
  AccountInfo,
  AccountMode,
  AuthStatus,
  BrowserFolder,
  BrowserLocation,
  DateFilterField,
  FolderListResponse,
  OptionsResponse,
  ProgressSnapshot,
  RunHistoryResponse,
  RunMode,
  RunSettings,
  RunStatus,
  TeamArchiveLayout,
  TeamCoveragePreset,
} from './types';

type Step = 'account' | 'connect' | 'settings' | 'run' | 'results';
type PickerTarget = 'source' | 'exclude' | 'archive' | null;

const defaultSettings: RunSettings = {
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

const phases = ['connecting', 'team_discovery', 'inventory', 'filter', 'copy', 'verify', 'outputs', 'completed'];

export default function App() {
  const queryClient = useQueryClient();
  const [step, setStep] = useState<Step>('account');
  const [accountMode, setAccountMode] = useState<AccountMode>('personal');
  const [settings, setSettings] = useState<RunSettings>(defaultSettings);
  const [account, setAccount] = useState<AccountInfo | null>(null);
  const [appKey, setAppKey] = useState('');
  const [authCode, setAuthCode] = useState('');
  const [adminMemberId, setAdminMemberId] = useState('');
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [selectedResultRunId, setSelectedResultRunId] = useState<string | null>(null);
  const [progress, setProgress] = useState<ProgressSnapshot | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [pickerTarget, setPickerTarget] = useState<PickerTarget>(null);
  const [pickerLocation, setPickerLocation] = useState<BrowserLocation | null>(null);
  const [pickerData, setPickerData] = useState<FolderListResponse | null>(null);
  const [pickerLoading, setPickerLoading] = useState(false);

  const optionsQuery = useQuery({
    queryKey: ['options'],
    queryFn: () => apiGet<OptionsResponse>('/api/options'),
  });

  const authStatusQuery = useQuery({
    queryKey: ['auth-status'],
    queryFn: () => apiGet<AuthStatus>('/api/auth/status'),
  });

  const historyQuery = useQuery({
    queryKey: ['runs', settings.output_dir],
    queryFn: () => apiGet<RunHistoryResponse>(`/api/runs?output_dir=${encodeURIComponent(settings.output_dir)}`),
  });

  const runStatusQuery = useQuery({
    queryKey: ['run-status', selectedResultRunId || activeRunId, settings.output_dir],
    queryFn: () =>
      apiGet<RunStatus>(
        `/api/runs/${encodeURIComponent(selectedResultRunId || activeRunId || '')}?output_dir=${encodeURIComponent(settings.output_dir)}`,
      ),
    enabled: Boolean(selectedResultRunId || activeRunId),
    refetchInterval: activeRunId ? 1500 : false,
  });

  useEffect(() => {
    if (!optionsQuery.data) return;
    setSettings((current) => ({
      ...current,
      ...optionsQuery.data.defaults,
      output_dir: current.output_dir === defaultSettings.output_dir ? optionsQuery.data.defaults.output_dir : current.output_dir,
      source_roots: current.source_roots,
      excluded_roots: current.excluded_roots,
      retry: current.retry,
    }));
  }, [optionsQuery.data]);

  useEffect(() => {
    const status = authStatusQuery.data;
    if (!status) return;
    if (status.account_mode) {
      setAccountMode(status.account_mode);
      setSettings((current) => ({ ...current, account_mode: status.account_mode || current.account_mode }));
    }
    if (status.app_key) setAppKey(status.app_key);
    if (status.admin_member_id) setAdminMemberId(status.admin_member_id);
  }, [authStatusQuery.data]);

  useEffect(() => {
    if (!activeRunId) return;
    const events = new EventSource(`/api/runs/${encodeURIComponent(activeRunId)}/events`);
    events.addEventListener('progress', (event) => {
      setProgress(JSON.parse((event as MessageEvent).data) as ProgressSnapshot);
    });
    events.addEventListener('log', (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { message: string };
      setLogs((current) => [...current.slice(-199), payload.message]);
    });
    events.addEventListener('result', (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { run_id: string };
      setSelectedResultRunId(payload.run_id);
      setActiveRunId(null);
      setStep('results');
      queryClient.invalidateQueries({ queryKey: ['runs'] });
      events.close();
    });
    events.addEventListener('error', (event) => {
      const messageEvent = event as MessageEvent;
      if (messageEvent.data) {
        const payload = JSON.parse(messageEvent.data) as { message: string };
        setError(payload.message);
        setActiveRunId(null);
        queryClient.invalidateQueries({ queryKey: ['runs'] });
        events.close();
      }
    });
    return () => events.close();
  }, [activeRunId, queryClient]);

  const startAuth = useMutation({
    mutationFn: () => apiPost<{ authorize_url: string }>('/api/auth/start', { account_mode: accountMode, app_key: appKey }),
    onSuccess: (payload) => {
      window.open(payload.authorize_url, '_blank', 'noopener,noreferrer');
      setError(null);
    },
    onError: (err) => setError(errorMessage(err)),
  });

  const finishAuth = useMutation({
    mutationFn: () => apiPost<{ account: AccountInfo }>('/api/auth/finish', { auth_code: authCode, admin_member_id: adminMemberId || null }),
    onSuccess: (payload) => {
      setAccount(payload.account);
      setAuthCode('');
      setError(null);
      queryClient.invalidateQueries({ queryKey: ['auth-status'] });
      setStep('settings');
    },
    onError: (err) => setError(errorMessage(err)),
  });

  const testAuth = useMutation({
    mutationFn: () => apiPost<{ account: AccountInfo }>('/api/auth/test', { account_mode: accountMode, admin_member_id: adminMemberId || null }),
    onSuccess: (payload) => {
      setAccount(payload.account);
      setError(null);
      setStep('settings');
    },
    onError: (err) => setError(errorMessage(err)),
  });

  const clearAuth = useMutation({
    mutationFn: () => apiDelete<{ status: string }>('/api/auth'),
    onSuccess: () => {
      setAccount(null);
      queryClient.invalidateQueries({ queryKey: ['auth-status'] });
    },
  });

  const startRun = useMutation({
    mutationFn: () =>
      apiPost<{ run_id: string }>('/api/runs', {
        ...settings,
        account_mode: accountMode,
        archive_root: normalizeDropboxPath(settings.archive_root),
        confirmed_copy_run: settings.mode === 'copy_run',
        admin_member_id: adminMemberId || null,
      }),
    onSuccess: (payload) => {
      setActiveRunId(payload.run_id);
      setSelectedResultRunId(null);
      setProgress(null);
      setLogs([]);
      setError(null);
      setStep('run');
    },
    onError: (err) => setError(errorMessage(err)),
  });

  const resumeRun = useMutation({
    mutationFn: () =>
      apiPost<{ run_id: string }>('/api/runs/resume', {
        output_dir: settings.output_dir,
        account_mode: accountMode,
        admin_member_id: adminMemberId || null,
      }),
    onSuccess: (payload) => {
      setActiveRunId(payload.run_id);
      setSelectedResultRunId(null);
      setProgress(null);
      setLogs([]);
      setError(null);
      setStep('run');
    },
    onError: (err) => setError(errorMessage(err)),
  });

  const cancelRun = useMutation({
    mutationFn: () => apiPost<{ status: string }>(`/api/runs/${encodeURIComponent(activeRunId || '')}/cancel`),
    onSuccess: () => setLogs((current) => [...current, 'Cancellation requested.']),
    onError: (err) => setError(errorMessage(err)),
  });

  const activeResult = runStatusQuery.data?.result || null;
  const validationMessage = useMemo(() => validateRunSettings(settings), [settings]);
  const busy = startAuth.isPending || finishAuth.isPending || testAuth.isPending || startRun.isPending || resumeRun.isPending;

  function chooseAccount(next: AccountMode) {
    setAccountMode(next);
    setSettings((current) => ({ ...current, account_mode: next }));
    setStep('connect');
  }

  async function openPicker(target: Exclude<PickerTarget, null>) {
    setPickerTarget(target);
    setPickerLocation(null);
    await loadFolders(null);
  }

  async function loadFolders(location: BrowserLocation | null) {
    setPickerLoading(true);
    setError(null);
    try {
      const data = await apiPost<FolderListResponse>('/api/folders/list', {
        ...settings,
        account_mode: accountMode,
        location,
      });
      setPickerData(data);
      setPickerLocation(data.location);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setPickerLoading(false);
    }
  }

  function applyFolder(folder: BrowserFolder | BrowserLocation) {
    const path = normalizeDropboxPath(folder.display_path);
    if (pickerTarget === 'archive') {
      if (path === '/') {
        setError('Choose a folder inside Dropbox for the archive.');
        return;
      }
      setSettings((current) => ({ ...current, archive_root: path }));
    }
    if (pickerTarget === 'source') {
      setSettings((current) => ({ ...current, source_roots: addUniquePath(current.source_roots, path, true) }));
    }
    if (pickerTarget === 'exclude') {
      setSettings((current) => ({ ...current, excluded_roots: addUniquePath(current.excluded_roots, path, false) }));
    }
    setPickerTarget(null);
  }

  function submitRun() {
    const validation = validateRunSettings(settings);
    if (validation) {
      setError(validation);
      return;
    }
    if (settings.mode === 'copy_run') {
      const confirmed = window.confirm('This creates archive copies in Dropbox. Originals are not deleted or moved. Continue?');
      if (!confirmed) return;
    }
    startRun.mutate();
  }

  return (
    <main className="app-shell">
      <aside className="rail">
        <div className="brand">
          <span className="brand-mark">DC</span>
          <div>
            <strong>Dropbox Cleaner</strong>
            <span>Local browser UI</span>
          </div>
        </div>
        <nav className="steps" aria-label="Workflow">
          {(['account', 'connect', 'settings', 'run', 'results'] as Step[]).map((item, index) => (
            <button key={item} className={item === step ? 'active' : ''} onClick={() => setStep(item)}>
              <span>{index + 1}</span>
              {stepLabel(item)}
            </button>
          ))}
        </nav>
        <section className="rail-status">
          <ShieldCheck size={18} />
          <p>Credentials stay in the local keyring. Reports are written to the selected local folder.</p>
        </section>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">{stepLabel(step)}</p>
            <h1>{headingForStep(step)}</h1>
          </div>
          <div className="topbar-actions">
            {account && <StatusPill tone="success" text={account.display_name} />}
            {authStatusQuery.data?.saved_credentials_available && !account && <StatusPill tone="neutral" text="Saved connection" />}
          </div>
        </header>

        {error && (
          <div className="notice danger" role="alert">
            <AlertTriangle size={18} />
            <span>{error}</span>
            <button className="icon-button" onClick={() => setError(null)} aria-label="Dismiss">
              <X size={16} />
            </button>
          </div>
        )}

        {step === 'account' && (
          <div className="two-column">
            <section className="panel primary-panel">
              <h2>Choose Dropbox access</h2>
              <div className="choice-list">
                {optionsQuery.data?.accounts.map((choice) => (
                  <button key={choice.value} className="choice-row" onClick={() => chooseAccount(choice.value as AccountMode)}>
                    <span>
                      <strong>{choice.label}</strong>
                      <small>{choice.description}</small>
                    </span>
                    <ChevronRight size={18} />
                  </button>
                ))}
              </div>
            </section>
            <RunHistory
              history={historyQuery.data}
              onOpen={(runId) => {
                setSelectedResultRunId(runId);
                setStep('results');
              }}
              onResume={() => resumeRun.mutate()}
              busy={resumeRun.isPending}
            />
          </div>
        )}

        {step === 'connect' && (
          <section className="panel flow-panel">
            <div className="section-heading">
              <div>
                <h2>Connect Dropbox</h2>
                <p>Approve access in Dropbox, then return here to finish the connection.</p>
              </div>
              <button className="ghost-button" onClick={() => setStep('account')}>
                <ChevronLeft size={16} /> Back
              </button>
            </div>
            {authStatusQuery.data?.saved_credentials_available && (
              <div className="inline-row saved-row">
                <div>
                  <strong>Saved Dropbox connection found</strong>
                  <span>Test it before continuing, or forget it and connect again.</span>
                </div>
                <button className="primary-button" onClick={() => testAuth.mutate()} disabled={busy}>
                  {testAuth.isPending ? <Loader2 className="spin" size={16} /> : <CheckCircle2 size={16} />} Use saved connection
                </button>
                <button className="danger-button" onClick={() => clearAuth.mutate()}>
                  <LogOut size={16} /> Forget
                </button>
              </div>
            )}
            <label className="field">
              <span>Dropbox app key</span>
              <input value={appKey} onChange={(event) => setAppKey(event.target.value)} disabled={authStatusQuery.data?.packaged_app_key_available} />
            </label>
            {accountMode === 'team_admin' && (
              <label className="field">
                <span>Admin member ID override</span>
                <input value={adminMemberId} onChange={(event) => setAdminMemberId(event.target.value)} placeholder="Optional" />
              </label>
            )}
            <div className="inline-actions">
              <button className="primary-button" onClick={() => startAuth.mutate()} disabled={busy}>
                {startAuth.isPending ? <Loader2 className="spin" size={16} /> : <ShieldCheck size={16} />} Open Dropbox authorization
              </button>
            </div>
            <label className="field">
              <span>Authorization code</span>
              <input value={authCode} onChange={(event) => setAuthCode(event.target.value)} placeholder="Paste the code from Dropbox" />
            </label>
            <div className="inline-actions">
              <button className="success-button" onClick={() => finishAuth.mutate()} disabled={busy || !authCode.trim()}>
                {finishAuth.isPending ? <Loader2 className="spin" size={16} /> : <CheckCircle2 size={16} />} Finish connection
              </button>
              <button className="ghost-button" onClick={() => setStep('settings')} disabled={!account}>
                Continue <ChevronRight size={16} />
              </button>
            </div>
          </section>
        )}

        {step === 'settings' && (
          <section className="settings-grid">
            <div className="settings-main">
              <SettingsPanel
                settings={settings}
                accountMode={accountMode}
                options={optionsQuery.data}
                onChange={setSettings}
                onPick={openPicker}
              />
            </div>
            <aside className="run-panel">
              <h2>Run type</h2>
              <div className="choice-stack">
                {optionsQuery.data?.run_modes.map((choice) => (
                  <button
                    key={choice.value}
                    className={settings.mode === choice.value ? 'choice-row selected' : 'choice-row'}
                    onClick={() => setSettings((current) => ({ ...current, mode: choice.value as RunMode }))}
                  >
                    <span>
                      <strong>{choice.label}</strong>
                      <small>{choice.description}</small>
                    </span>
                  </button>
                ))}
              </div>
              {validationMessage && <p className="form-error">{validationMessage}</p>}
              <button className="primary-button wide" onClick={submitRun} disabled={busy || Boolean(validationMessage)}>
                {startRun.isPending ? <Loader2 className="spin" size={16} /> : <Play size={16} />} Start run
              </button>
              <button className="ghost-button wide" onClick={() => resumeRun.mutate()} disabled={resumeRun.isPending}>
                {resumeRun.isPending ? <Loader2 className="spin" size={16} /> : <RotateCcw size={16} />} Resume latest run
              </button>
            </aside>
          </section>
        )}

        {step === 'run' && (
          <section className="run-layout">
            <div className="panel progress-panel">
              <div className="section-heading">
                <div>
                  <h2>{friendlyPhase(progress?.phase || 'starting')}</h2>
                  <p>{progress?.message || 'Preparing the run.'}</p>
                </div>
                <button className="danger-button" onClick={() => cancelRun.mutate()} disabled={!activeRunId || cancelRun.isPending}>
                  <Square size={14} /> Stop safely
                </button>
              </div>
              <PhaseTimeline phase={progress?.phase} />
              <MetricStrip counters={progress?.counters || {}} />
            </div>
            <div className="panel log-panel">
              <h2>Details for support</h2>
              <pre>{logs.length ? logs.join('\n') : 'Waiting for run output...'}</pre>
            </div>
          </section>
        )}

        {step === 'results' && (
          <ResultsPanel
            status={runStatusQuery.data || null}
            result={activeResult}
            outputDir={settings.output_dir}
            onStartAnother={() => setStep('settings')}
            onResume={() => resumeRun.mutate()}
          />
        )}
      </section>

      {pickerTarget && (
        <FolderPicker
          target={pickerTarget}
          data={pickerData}
          location={pickerLocation}
          loading={pickerLoading}
          onClose={() => setPickerTarget(null)}
          onOpenAdvanced={() => loadFolders({ display_path: '/', namespace_id: null, namespace_path: '/', title: 'Advanced team locations', view_mode: 'team_namespaces' })}
          onBack={() => pickerData?.parent && loadFolders(pickerData.parent)}
          onOpen={(folder) => loadFolders(folderToLocation(folder))}
          onChoose={applyFolder}
        />
      )}
    </main>
  );
}

function SettingsPanel({
  settings,
  accountMode,
  options,
  onChange,
  onPick,
}: {
  settings: RunSettings;
  accountMode: AccountMode;
  options?: OptionsResponse;
  onChange: (settings: RunSettings) => void;
  onPick: (target: Exclude<PickerTarget, null>) => void;
}) {
  const set = <K extends keyof RunSettings>(key: K, value: RunSettings[K]) => onChange({ ...settings, [key]: value });
  return (
    <section className="panel flow-panel">
      <h2>Run settings</h2>
      <div className="form-grid">
        <label className="field">
          <span>Cutoff date</span>
          <input type="date" value={settings.cutoff_date} onChange={(event) => set('cutoff_date', event.target.value)} />
        </label>
        <label className="field">
          <span>Date filter</span>
          <select value={settings.date_filter_field} onChange={(event) => set('date_filter_field', event.target.value as DateFilterField)}>
            {options?.date_filters.map((choice) => (
              <option key={choice.value} value={choice.value}>
                {choice.label}
              </option>
            ))}
          </select>
        </label>
      </div>
      <PathEditor
        title="Archive folder"
        value={settings.archive_root}
        onChange={(value) => set('archive_root', value)}
        onPick={() => onPick('archive')}
      />
      <PathList
        title="Folders to include"
        emptyText="Whole Dropbox"
        paths={settings.source_roots}
        allowRoot
        onAdd={(value) => set('source_roots', addUniquePath(settings.source_roots, value, true))}
        onRemove={(path) => set('source_roots', settings.source_roots.filter((item) => item !== path))}
        onPick={() => onPick('source')}
      />
      <PathList
        title="Folders to skip"
        emptyText="No skipped folders"
        paths={settings.excluded_roots}
        onAdd={(value) => set('excluded_roots', addUniquePath(settings.excluded_roots, value, false))}
        onRemove={(path) => set('excluded_roots', settings.excluded_roots.filter((item) => item !== path))}
        onPick={() => onPick('exclude')}
      />
      {accountMode === 'team_admin' && (
        <div className="form-grid">
          <label className="field">
            <span>Team coverage</span>
            <select value={settings.team_coverage_preset} onChange={(event) => set('team_coverage_preset', event.target.value as TeamCoveragePreset)}>
              {options?.team_coverage.map((choice) => (
                <option key={choice.value} value={choice.value}>
                  {choice.label}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Archive layout</span>
            <select value={settings.team_archive_layout} onChange={(event) => set('team_archive_layout', event.target.value as TeamArchiveLayout)}>
              {options?.team_archive_layouts.map((choice) => (
                <option key={choice.value} value={choice.value}>
                  {choice.label}
                </option>
              ))}
            </select>
          </label>
        </div>
      )}
      <label className="field">
        <span>Local reports folder</span>
        <input value={settings.output_dir} onChange={(event) => set('output_dir', event.target.value)} />
      </label>
      <details className="advanced">
        <summary>Advanced settings</summary>
        <div className="form-grid">
          <NumberField label="Batch size" value={settings.batch_size} min={1} max={10000} onChange={(value) => set('batch_size', value)} />
          <NumberField label="Worker count" value={settings.worker_count} min={1} max={8} onChange={(value) => set('worker_count', value)} />
          <NumberField
            label="Retry count"
            value={settings.retry.max_retries}
            min={0}
            max={20}
            onChange={(value) => set('retry', { ...settings.retry, max_retries: value })}
          />
          <label className="field">
            <span>Conflict policy</span>
            <select value={settings.conflict_policy} onChange={(event) => set('conflict_policy', event.target.value as 'safe_skip' | 'abort_run')}>
              <option value="safe_skip">safe_skip</option>
              <option value="abort_run">abort_run</option>
            </select>
          </label>
        </div>
        <label className="check-row">
          <input
            type="checkbox"
            checked={settings.include_folders_in_inventory}
            onChange={(event) => set('include_folders_in_inventory', event.target.checked)}
          />
          Include folders in inventory export
        </label>
        <label className="check-row">
          <input
            type="checkbox"
            checked={settings.exclude_archive_destination}
            onChange={(event) => set('exclude_archive_destination', event.target.checked)}
          />
          Exclude archive folder from traversal
        </label>
      </details>
    </section>
  );
}

function PathEditor({ title, value, onChange, onPick }: { title: string; value: string; onChange: (value: string) => void; onPick: () => void }) {
  return (
    <div className="path-block">
      <label className="field">
        <span>{title}</span>
        <input value={value} onChange={(event) => onChange(event.target.value)} onBlur={(event) => onChange(normalizeDropboxPath(event.target.value))} />
      </label>
      <button className="ghost-button" onClick={onPick}>
        <FolderOpen size={16} /> Browse Dropbox
      </button>
    </div>
  );
}

function PathList({
  title,
  emptyText,
  paths,
  allowRoot = false,
  onAdd,
  onRemove,
  onPick,
}: {
  title: string;
  emptyText: string;
  paths: string[];
  allowRoot?: boolean;
  onAdd: (value: string) => void;
  onRemove: (path: string) => void;
  onPick: () => void;
}) {
  const [draft, setDraft] = useState('');
  return (
    <div className="path-list">
      <div className="section-heading compact">
        <h3>{title}</h3>
        <button className="ghost-button" onClick={onPick}>
          <FolderOpen size={16} /> Browse
        </button>
      </div>
      <div className="chips">
        {paths.length === 0 ? (
          <span className="empty-chip">{emptyText}</span>
        ) : (
          paths.map((path) => (
            <span className="chip" key={path}>
              {path}
              <button onClick={() => onRemove(path)} aria-label={`Remove ${path}`}>
                <X size={14} />
              </button>
            </span>
          ))
        )}
      </div>
      <div className="inline-actions">
        <input value={draft} onChange={(event) => setDraft(event.target.value)} placeholder={allowRoot ? '/Screenshots or / for whole Dropbox' : '/Screenshots'} />
        <button
          className="secondary-button"
          onClick={() => {
            onAdd(draft);
            setDraft('');
          }}
        >
          Add path
        </button>
      </div>
    </div>
  );
}

function NumberField({
  label,
  value,
  min,
  max,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <input type="number" min={min} max={max} value={value} onChange={(event) => onChange(Number(event.target.value))} />
    </label>
  );
}

function FolderPicker({
  target,
  data,
  loading,
  onClose,
  onOpenAdvanced,
  onBack,
  onOpen,
  onChoose,
}: {
  target: Exclude<PickerTarget, null>;
  data: FolderListResponse | null;
  location: BrowserLocation | null;
  loading: boolean;
  onClose: () => void;
  onOpenAdvanced: () => void;
  onBack: () => void;
  onOpen: (folder: BrowserFolder) => void;
  onChoose: (folder: BrowserFolder | BrowserLocation) => void;
}) {
  return (
    <div className="modal-backdrop">
      <section className="modal">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Dropbox folder</p>
            <h2>{pickerTitle(target)}</h2>
            <p>{data?.location.display_path || '/'}</p>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="Close">
            <X size={18} />
          </button>
        </div>
        <div className="inline-actions">
          <button className="ghost-button" onClick={onBack} disabled={loading}>
            <ChevronLeft size={16} /> Parent
          </button>
          {data?.advanced_team_locations_available && (
            <button className="ghost-button" onClick={onOpenAdvanced} disabled={loading}>
              Advanced team locations
            </button>
          )}
          {data?.location && (
            <button className="primary-button" onClick={() => onChoose(data.location)} disabled={loading}>
              Choose current folder
            </button>
          )}
        </div>
        <div className="folder-list">
          {loading && <div className="loading-row"><Loader2 className="spin" size={18} /> Loading folders...</div>}
          {!loading &&
            data?.folders.map((folder) => (
              <div className="folder-row" key={`${folder.namespace_id || 'root'}:${folder.display_path}`}>
                <button onClick={() => onOpen(folder)}>
                  <FolderOpen size={17} />
                  <span>
                    <strong>{folder.name || 'Dropbox'}</strong>
                    <small>{folder.subtitle || folder.display_path}</small>
                  </span>
                </button>
                <button className="secondary-button" onClick={() => onChoose(folder)}>
                  Choose
                </button>
              </div>
            ))}
          {!loading && data?.folders.length === 0 && <p className="empty-state">No folders at this level.</p>}
        </div>
      </section>
    </div>
  );
}

function RunHistory({
  history,
  onOpen,
  onResume,
  busy,
}: {
  history?: RunHistoryResponse;
  onOpen: (runId: string) => void;
  onResume: () => void;
  busy: boolean;
}) {
  return (
    <aside className="panel history-panel">
      <div className="section-heading compact">
        <h2>Local runs</h2>
        <button className="ghost-button" onClick={onResume} disabled={busy || !history?.latest_run_id}>
          {busy ? <Loader2 className="spin" size={16} /> : <RotateCcw size={16} />} Resume
        </button>
      </div>
      <div className="history-list">
        {history?.runs.slice(0, 6).map((run) => (
          <button key={`${run.run_id}:${run.run_dir}`} onClick={() => onOpen(run.run_id)}>
            <span>
              <strong>{run.run_id || run.run_dir}</strong>
              <small>{run.status_message}</small>
            </span>
            {run.latest && <StatusPill tone="neutral" text="Latest" />}
          </button>
        ))}
        {(!history || history.runs.length === 0) && <p className="empty-state">No local run history found in the current reports folder.</p>}
      </div>
    </aside>
  );
}

function ResultsPanel({
  status,
  result,
  outputDir,
  onStartAnother,
  onResume,
}: {
  status: RunStatus | null;
  result: RunStatus['result'];
  outputDir: string;
  onStartAnother: () => void;
  onResume: () => void;
}) {
  if (!status) {
    return (
      <section className="panel">
        <h2>No run selected</h2>
        <p className="empty-state">Choose a run from local history or start a new run.</p>
      </section>
    );
  }
  if (!result) {
    return (
      <section className="panel">
        <h2>{status.status === 'running' ? 'Run still in progress' : 'Results unavailable'}</h2>
        {status.error && <p className="form-error">{status.error}</p>}
      </section>
    );
  }
  const fileRunId = status.actual_run_id || result.run_id || status.run_id;
  const issueRows = [...result.conflicts, ...result.failures, ...result.blocked];
  return (
    <section className="results-grid">
      <div className="panel results-main">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Run complete</p>
            <h2>{result.success_message}</h2>
          </div>
          <StatusPill tone={result.has_issues ? 'danger' : 'success'} text={result.has_issues ? 'Needs review' : 'Ready'} />
        </div>
        <div className="metric-grid">
          {result.metrics.map((metric) => (
            <div className={`metric ${metric.tone}`} key={metric.label}>
              <span>{metric.label}</span>
              <strong>{metric.value.toLocaleString()}</strong>
            </div>
          ))}
        </div>
        <h3>Top folders</h3>
        <div className="table">
          <div className="table-head">
            <span>Folder</span>
            <span>Matched</span>
            <span>Copied</span>
            <span>Skipped</span>
            <span>Failed</span>
          </div>
          {result.top_folders.map((folder) => (
            <div className="table-row" key={folder.folder}>
              <span>{folder.folder}</span>
              <span>{folder.matched}</span>
              <span>{folder.copied}</span>
              <span>{folder.skipped}</span>
              <span>{folder.failed}</span>
            </div>
          ))}
        </div>
        <h3>{result.review_title}</h3>
        {issueRows.length ? (
          <ul className="issue-list">
            {issueRows.slice(0, 20).map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ) : (
          <p className="empty-state">{result.already_archived[0] || 'No conflicts or failures were reported.'}</p>
        )}
      </div>
      <aside className="panel output-panel">
        <h2>Output files</h2>
        <p>{status.run_dir}</p>
        <div className="file-list">
          {result.output_files.map((name) => (
            <a key={name} href={fileUrl(fileRunId, name, outputDir)} target="_blank" rel="noreferrer">
              {name}
            </a>
          ))}
        </div>
        <button className="primary-button wide" onClick={onStartAnother}>
          Start another run
        </button>
        <button className="ghost-button wide" onClick={onResume}>
          <RotateCcw size={16} /> Resume latest run
        </button>
      </aside>
    </section>
  );
}

function PhaseTimeline({ phase }: { phase?: string }) {
  const current = Math.max(0, phases.indexOf(phase || 'connecting'));
  return (
    <div className="phase-line">
      {phases.map((item, index) => (
        <div key={item} className={index <= current ? 'done' : ''}>
          <span />
          <small>{friendlyPhase(item)}</small>
        </div>
      ))}
    </div>
  );
}

function MetricStrip({ counters }: { counters: Record<string, number> }) {
  const items = [
    ['Scanned', 'items_scanned'],
    ['Matched', 'files_matched'],
    ['Copied', 'files_copied'],
    ['Skipped', 'files_skipped'],
    ['Failed', 'files_failed'],
  ];
  return (
    <div className="metric-grid compact-metrics">
      {items.map(([label, key]) => (
        <div className="metric" key={key}>
          <span>{label}</span>
          <strong>{(counters[key] || 0).toLocaleString()}</strong>
        </div>
      ))}
    </div>
  );
}

function StatusPill({ tone, text }: { tone: 'success' | 'danger' | 'neutral'; text: string }) {
  return <span className={`status-pill ${tone}`}>{text}</span>;
}

function folderToLocation(folder: BrowserFolder): BrowserLocation {
  return {
    display_path: folder.display_path,
    namespace_id: folder.namespace_id,
    namespace_path: folder.namespace_path,
    title: folder.name,
    view_mode: 'default',
  };
}

function errorMessage(err: unknown): string {
  if (err instanceof ApiError) return err.message;
  if (err instanceof Error) return err.message;
  return 'Something went wrong.';
}

function stepLabel(step: Step): string {
  return {
    account: 'Account',
    connect: 'Connection',
    settings: 'Settings',
    run: 'Progress',
    results: 'Results',
  }[step];
}

function headingForStep(step: Step): string {
  return {
    account: 'Start a local cleanup run',
    connect: 'Verify Dropbox access',
    settings: 'Configure the archive',
    run: 'Run in progress',
    results: 'Review outputs',
  }[step];
}

function friendlyPhase(phase: string): string {
  return (
    {
      starting: 'Starting',
      connecting: 'Connecting',
      team_discovery: 'Discovering team content',
      inventory: 'Scanning Dropbox',
      filter: 'Finding older files',
      copy: 'Copying archive files',
      verify: 'Verifying archive',
      outputs: 'Writing reports',
      completed: 'Completed',
    }[phase] || phase.replace(/_/g, ' ')
  );
}

function pickerTitle(target: Exclude<PickerTarget, null>): string {
  return {
    source: 'Choose a folder to include',
    exclude: 'Choose a folder to skip',
    archive: 'Choose the archive folder',
  }[target];
}
