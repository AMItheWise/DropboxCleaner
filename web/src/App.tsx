import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  AlertTriangle,
  Archive,
  CalendarClock,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  FileCheck2,
  FolderOpen,
  HardDrive,
  Loader2,
  LockKeyhole,
  LogOut,
  Play,
  RotateCcw,
  Server,
  ShieldCheck,
  Square,
  UserRound,
  UsersRound,
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
  const [copyConfirmOpen, setCopyConfirmOpen] = useState(false);
  const [authFormOpen, setAuthFormOpen] = useState(false);

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
      setAuthFormOpen(true);
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
      setCopyConfirmOpen(false);
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
  const hasSavedConnection = Boolean(authStatusQuery.data?.saved_credentials_available);
  const showAuthFields = !hasSavedConnection || authFormOpen;

  function chooseAccount(next: AccountMode) {
    setAccountMode(next);
    setSettings((current) => ({ ...current, account_mode: next }));
    setAuthFormOpen(false);
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
      setCopyConfirmOpen(true);
      return;
    }
    startRun.mutate();
  }

  function startConfirmedCopyRun() {
    setCopyConfirmOpen(false);
    startRun.mutate();
  }

  return (
    <main className="app-shell">
      <aside className="rail">
        <div className="brand">
          <span className="brand-mark">DC</span>
          <div>
            <strong>Dropbox Cleaner</strong>
            <span>Local archive workspace</span>
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
          <div className="rail-status-line">
            <LockKeyhole size={16} />
            <span>Local credentials</span>
          </div>
          <div className="rail-status-line">
            <Archive size={16} />
            <span>Copy-first archive</span>
          </div>
          <div className="rail-status-line">
            <FileCheck2 size={16} />
            <span>CSV reports and resume state</span>
          </div>
        </section>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">{stepLabel(step)}</p>
            <h1>{headingForStep(step)}</h1>
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
          <section className="panel primary-panel start-panel welcome-panel">
            <h2>Choose account type</h2>
            <div className="choice-list">
              {optionsQuery.data?.accounts.map((choice) => (
                <button key={choice.value} className="choice-row account-choice" onClick={() => chooseAccount(choice.value as AccountMode)}>
                  <span className="choice-icon">{choice.value === 'team_admin' ? <UsersRound size={20} /> : <UserRound size={20} />}</span>
                  <span>
                    <strong>{choice.label}</strong>
                  </span>
                  <ChevronRight size={18} />
                </button>
              ))}
            </div>
          </section>
        )}

        {step === 'connect' && (
          <section className="panel flow-panel">
            <div className="section-heading">
              <div>
                <h2>Connect Dropbox</h2>
                <p>{hasSavedConnection && !showAuthFields ? 'Use the saved connection or connect a different Dropbox account.' : 'Approve access in Dropbox, then return here to finish the connection.'}</p>
              </div>
              <button className="ghost-button" onClick={() => setStep('account')}>
                <ChevronLeft size={16} /> Back
              </button>
            </div>
            {hasSavedConnection && !showAuthFields && (
              <div className="saved-connection-card">
                <div>
                  <strong>Saved Dropbox connection found</strong>
                  <span>Continue with the connection stored on this computer.</span>
                </div>
                <div className="saved-actions">
                  <button className="primary-button" onClick={() => testAuth.mutate()} disabled={busy}>
                    {testAuth.isPending ? <Loader2 className="spin" size={16} /> : <CheckCircle2 size={16} />} Use saved connection
                  </button>
                  <button className="ghost-button" onClick={() => setAuthFormOpen(true)} disabled={busy}>
                    Connect different account
                  </button>
                  <button className="danger-button" onClick={() => clearAuth.mutate()} disabled={clearAuth.isPending}>
                    <LogOut size={16} /> Forget
                  </button>
                </div>
              </div>
            )}
            {showAuthFields && (
              <div className="auth-form">
                {hasSavedConnection && (
                  <div className="notice subtle">
                    <span>Connecting a different account will replace the saved Dropbox connection after authorization.</span>
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
                  {hasSavedConnection && (
                    <button className="ghost-button" onClick={() => setAuthFormOpen(false)} disabled={busy}>
                      Cancel
                    </button>
                  )}
                  <button className="ghost-button" onClick={() => setStep('settings')} disabled={!account}>
                    Continue <ChevronRight size={16} />
                  </button>
                </div>
              </div>
            )}
            {!showAuthFields && account && (
              <div className="inline-actions">
                <button className="ghost-button" onClick={() => setStep('settings')}>
                  Continue <ChevronRight size={16} />
                </button>
              </div>
            )}
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
            <div className="settings-side">
              <aside className="run-panel">
                <h2>Run type</h2>
                <p className="panel-copy">Choose preview before copy when working with a client Dropbox for the first time.</p>
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
                <div className="run-summary">
                  <div>
                    <CalendarClock size={16} />
                    <span>Cutoff</span>
                    <strong>{settings.cutoff_date}</strong>
                  </div>
                  <div>
                    <Archive size={16} />
                    <span>Archive</span>
                    <strong>{normalizeDropboxPath(settings.archive_root)}</strong>
                  </div>
                  <div>
                    <HardDrive size={16} />
                    <span>Reports</span>
                    <strong>{settings.output_dir}</strong>
                  </div>
                </div>
                {validationMessage && <p className="form-error">{validationMessage}</p>}
                <button className="primary-button wide" onClick={submitRun} disabled={busy || Boolean(validationMessage)}>
                  {startRun.isPending ? <Loader2 className="spin" size={16} /> : <Play size={16} />} Start run
                </button>
                <button className="ghost-button wide" onClick={() => resumeRun.mutate()} disabled={resumeRun.isPending}>
                  {resumeRun.isPending ? <Loader2 className="spin" size={16} /> : <RotateCcw size={16} />} Resume latest run
                </button>
              </aside>
              {account && (
                <RunHistory
                  history={historyQuery.data}
                  onOpen={(runId) => {
                    setSelectedResultRunId(runId);
                    setStep('results');
                  }}
                  onResume={() => resumeRun.mutate()}
                  busy={resumeRun.isPending}
                />
              )}
            </div>
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
            <details className="panel log-panel">
              <summary>
                <span>
                  <strong>Details for support</strong>
                  <small>{logs.length ? `${logs.length} log lines captured` : 'Run messages will appear here'}</small>
                </span>
                <StatusPill tone="neutral" text="Technical log" />
              </summary>
              <pre>{logs.length ? logs.join('\n') : 'Waiting for run output...'}</pre>
            </details>
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

      {copyConfirmOpen && (
        <CopyRunDialog
          settings={settings}
          busy={startRun.isPending}
          onCancel={() => setCopyConfirmOpen(false)}
          onConfirm={startConfirmedCopyRun}
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
      <div className="section-heading">
        <div>
          <h2>Run settings</h2>
          <p>Set the cutoff, archive destination, and local report folder before starting a preview or copy run.</p>
        </div>
      </div>
      <div className="settings-section">
        <div className="settings-section-title">
          <CalendarClock size={17} />
          <h3>Date rules</h3>
        </div>
        <div className="form-grid">
          <label className="field">
            <span>Cutoff date</span>
            <input type="date" value={settings.cutoff_date} onChange={(event) => set('cutoff_date', event.target.value)} />
            <small className="field-help">Files older than this date are included in the match set.</small>
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
            <small className="field-help">Server modified is safest when Dropbox metadata is the source of truth.</small>
          </label>
        </div>
      </div>
      <div className="settings-section">
        <div className="settings-section-title">
          <Archive size={17} />
          <h3>Dropbox scope</h3>
        </div>
        <PathEditor
          title="Archive folder"
          helpText="Copy runs create archive copies here. Originals stay where they are."
          value={settings.archive_root}
          onChange={(value) => set('archive_root', value)}
          onPick={() => onPick('archive')}
        />
        <PathList
          title="Folders to include"
          emptyText="Whole Dropbox"
          helpText="Leave empty to scan the whole connected Dropbox scope."
          paths={settings.source_roots}
          allowRoot
          onAdd={(value) => set('source_roots', addUniquePath(settings.source_roots, value, true))}
          onRemove={(path) => set('source_roots', settings.source_roots.filter((item) => item !== path))}
          onPick={() => onPick('source')}
        />
        <PathList
          title="Folders to skip"
          emptyText="No skipped folders"
          helpText="Use this for active project areas or any destination that should never be scanned."
          paths={settings.excluded_roots}
          onAdd={(value) => set('excluded_roots', addUniquePath(settings.excluded_roots, value, false))}
          onRemove={(path) => set('excluded_roots', settings.excluded_roots.filter((item) => item !== path))}
          onPick={() => onPick('exclude')}
        />
      </div>
      {accountMode === 'team_admin' && (
        <div className="settings-section">
          <div className="settings-section-title">
            <Server size={17} />
            <h3>Team mode</h3>
          </div>
          <div className="form-grid team-mode-grid">
            <label className="field">
              <span>Team coverage</span>
              <select value={settings.team_coverage_preset} onChange={(event) => set('team_coverage_preset', event.target.value as TeamCoveragePreset)}>
                {options?.team_coverage.map((choice) => (
                  <option key={choice.value} value={choice.value}>
                    {choice.label}
                  </option>
                ))}
              </select>
              <small className="field-help">Start with team-owned content unless the client asks for every member namespace.</small>
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
              <small className="field-help">Segmented keeps archives separated by source namespace.</small>
            </label>
          </div>
        </div>
      )}
      <div className="settings-section">
        <div className="settings-section-title">
          <HardDrive size={17} />
          <h3>Local outputs</h3>
        </div>
        <label className="field">
          <span>Local reports folder</span>
          <input value={settings.output_dir} onChange={(event) => set('output_dir', event.target.value)} />
          <small className="field-help">Run state, manifests, and CSV reports are written here on this computer.</small>
        </label>
      </div>
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

function PathEditor({
  title,
  helpText,
  value,
  onChange,
  onPick,
}: {
  title: string;
  helpText: string;
  value: string;
  onChange: (value: string) => void;
  onPick: () => void;
}) {
  return (
    <div className="path-block">
      <label className="field">
        <span>{title}</span>
        <input value={value} onChange={(event) => onChange(event.target.value)} onBlur={(event) => onChange(normalizeDropboxPath(event.target.value))} />
        <small className="field-help">{helpText}</small>
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
  helpText,
  paths,
  allowRoot = false,
  onAdd,
  onRemove,
  onPick,
}: {
  title: string;
  emptyText: string;
  helpText: string;
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
        <div>
          <h3>{title}</h3>
          <p>{helpText}</p>
        </div>
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

function CopyRunDialog({
  settings,
  busy,
  onCancel,
  onConfirm,
}: {
  settings: RunSettings;
  busy: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div className="modal-backdrop">
      <section className="modal confirm-modal" role="dialog" aria-modal="true" aria-labelledby="copy-run-title">
        <div className="confirm-badge">
          <Archive size={22} />
        </div>
        <div className="section-heading">
          <div>
            <p className="eyebrow">Copy run confirmation</p>
            <h2 id="copy-run-title">Create archive copies in Dropbox?</h2>
            <p>Original files are not deleted or moved. This run writes archive copies and local reports only.</p>
          </div>
          <button className="icon-button" onClick={onCancel} aria-label="Close">
            <X size={18} />
          </button>
        </div>
        <div className="confirm-summary">
          <div>
            <span>Archive destination</span>
            <strong>{normalizeDropboxPath(settings.archive_root)}</strong>
          </div>
          <div>
            <span>Cutoff date</span>
            <strong>{settings.cutoff_date}</strong>
          </div>
          <div>
            <span>Scan scope</span>
            <strong>{settings.source_roots.length ? `${settings.source_roots.length} included folder(s)` : 'Whole connected Dropbox scope'}</strong>
          </div>
          <div>
            <span>Reports folder</span>
            <strong>{settings.output_dir}</strong>
          </div>
        </div>
        <div className="notice warning">
          <AlertTriangle size={18} />
          <span>Use a dry run first when the client has not reviewed the matched files report.</span>
        </div>
        <div className="modal-actions">
          <button className="ghost-button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
          <button className="primary-button" onClick={onConfirm} disabled={busy}>
            {busy ? <Loader2 className="spin" size={16} /> : <Archive size={16} />} Confirm copy run
          </button>
        </div>
      </section>
    </div>
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
    <details className="panel history-panel">
      <summary className="history-summary">
        <span>
          <strong>Local runs</strong>
          <small>{history?.runs.length ? `${history.runs.length} saved run(s)` : 'No saved runs'}</small>
        </span>
        <ChevronRight size={18} />
      </summary>
      <div className="history-toolbar">
        <button className="ghost-button" onClick={onResume} disabled={busy || !history?.latest_run_id}>
          {busy ? <Loader2 className="spin" size={16} /> : <RotateCcw size={16} />} Resume latest run
        </button>
      </div>
      <div className="history-list">
        {history?.runs.slice(0, 6).map((run) => (
          <button key={`${run.run_id}:${run.run_dir}`} onClick={() => onOpen(run.run_id)}>
            <span className="history-copy">
              <strong title={run.run_id || run.run_dir}>{shortRunId(run.run_id || run.run_dir)}</strong>
              <small>{run.status_message}</small>
              <span className="history-meta">
                <CalendarClock size={13} />
                {formatDateTime(run.created_at)}
              </span>
            </span>
            <span className="history-pills">
              <StatusPill tone="neutral" text={runModeLabel(run.mode)} />
              {run.has_issues && <StatusPill tone="danger" text="Review" />}
              {run.latest && <StatusPill tone="neutral" text="Latest" />}
            </span>
          </button>
        ))}
        {(!history || history.runs.length === 0) && <p className="empty-state">No local run history found in the current reports folder.</p>}
      </div>
    </details>
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
  const reportFiles = primaryReportFiles(result.output_files);
  const technicalFiles = result.output_files.filter((name) => !reportFiles.includes(name));
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
        <section className="result-section">
          <div className="result-section-heading">
            <h3>Files to review</h3>
            <p>Folders with the largest matched file counts are listed first.</p>
          </div>
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
                <span>{folder.matched.toLocaleString()}</span>
                <span>{folder.copied.toLocaleString()}</span>
                <span>{folder.skipped.toLocaleString()}</span>
                <span>{folder.failed.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </section>
        <section className="result-section">
          <div className="result-section-heading">
            <h3>{result.review_title}</h3>
            <p>Use these rows for manual follow-up before sharing the report.</p>
          </div>
          {issueRows.length ? (
            <ul className="issue-list">
              {issueRows.slice(0, 20).map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          ) : (
            <p className="empty-state">{result.already_archived[0] || 'No conflicts or failures were reported.'}</p>
          )}
        </section>
      </div>
      <aside className="panel output-panel">
        <div className="output-heading">
          <HardDrive size={18} />
          <div>
            <h2>Reports</h2>
          </div>
        </div>
        <div className="report-list">
          {reportFiles.map((name) => {
            const meta = outputFileMeta(name);
            return (
              <a key={name} className="report-link" href={fileUrl(fileRunId, name, outputDir)} target="_blank" rel="noreferrer">
                <FileCheck2 size={17} />
                <span>
                  <strong>{meta.label}</strong>
                  <small>{name}</small>
                </span>
              </a>
            );
          })}
        </div>
        {technicalFiles.length > 0 && (
          <details className="technical-files">
            <summary>{technicalFiles.length} technical file(s)</summary>
            <div className="technical-file-list">
              {technicalFiles.map((name) => (
                <a key={name} href={fileUrl(fileRunId, name, outputDir)} target="_blank" rel="noreferrer">
                  {name}
                </a>
              ))}
            </div>
          </details>
        )}
        {status.run_dir && (
          <details className="output-location">
            <summary>Output folder</summary>
            <p className="output-path">{status.run_dir}</p>
          </details>
        )}
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

function formatDateTime(value: string): string {
  if (!value) return 'Date unknown';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function runModeLabel(mode: string): string {
  const labels: Record<string, string> = {
    inventory_only: 'Inventory',
    dry_run: 'Preview',
    copy_run: 'Copy',
  };
  return labels[mode] || mode.replace(/_/g, ' ');
}

function shortRunId(value: string): string {
  const match = value.match(/[0-9a-f]{8}-[0-9a-f-]{27,}/i);
  if (!match) return value;
  return `${match[0].slice(0, 8)}...${match[0].slice(-6)}`;
}

function primaryReportFiles(names: string[]): string[] {
  const selected = [
    findOutputFile(names, ['summary.md', 'summary.json']),
    findOutputFile(names, ['matched']),
    findOutputFile(names, ['inventory']),
    findOutputFile(names, ['verification_report.csv', 'verification']),
  ].filter((name): name is string => Boolean(name));
  return selected.length ? selected.slice(0, 4) : names.slice(0, 3);
}

function findOutputFile(names: string[], preferred: string[]): string | undefined {
  for (const pattern of preferred) {
    const found = names.find((name) => name.toLowerCase().includes(pattern));
    if (found) return found;
  }
  return undefined;
}

function outputFileMeta(name: string): { label: string; description: string } {
  const lower = name.toLowerCase();
  if (lower === 'app.log') return { label: 'Run log', description: 'Support log with detailed run messages.' };
  if (lower === 'app.jsonl') return { label: 'Event stream', description: 'Structured event log for debugging or audit review.' };
  if (lower.includes('config_snapshot')) return { label: 'Configuration snapshot', description: 'Settings captured at the start of the run.' };
  if (lower.includes('manifest')) return { label: 'Copy manifest', description: 'Source and destination mapping for archive copy work.' };
  if (lower.includes('verification')) return { label: 'Verification report', description: 'Post-copy checks against the planned archive output.' };
  if (lower.includes('matched')) return { label: 'Matched files', description: 'Files that met the cutoff and scope rules.' };
  if (lower.includes('copied')) return { label: 'Copied files', description: 'Archive copy results for completed copy work.' };
  if (lower.includes('skipped')) return { label: 'Skipped files', description: 'Items intentionally skipped by rules or safety checks.' };
  if (lower.includes('conflict')) return { label: 'Conflicts', description: 'Files needing manual review before retrying.' };
  if (lower.includes('failure') || lower.includes('failed')) return { label: 'Failures', description: 'Errors from Dropbox or local report writing.' };
  if (lower.includes('blocked')) return { label: 'Blocked items', description: 'Items not processed because a safety check stopped them.' };
  if (lower.includes('inventory')) return { label: 'Inventory', description: 'Full discovered Dropbox inventory for this run.' };
  if (lower.includes('summary') || lower.includes('report')) return { label: 'Run summary', description: 'Human-readable overview of the run outcome.' };
  if (lower.endsWith('.db')) return { label: 'Resume database', description: 'Local state used to resume interrupted work.' };
  return { label: 'Output file', description: 'Generated artifact from this run.' };
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
    account: 'Welcome',
    connect: 'Connection',
    settings: 'Settings',
    run: 'Progress',
    results: 'Results',
  }[step];
}

function headingForStep(step: Step): string {
  return {
    account: 'Welcome to Dropbox Cleaner',
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
