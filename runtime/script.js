const STORAGE_KEYS = {
  config: "embyPulseConfig",
  invites: "embyPulseInvites",
  renewals: "embyPulseRenewals",
  botConfig: "embyPulseBotConfig",
  activeView: "embyPulseActiveView",
  inviteSyncEndpoint: "embyPulseInviteSyncEndpoint",
  qualityLastScanAt: "embyPulseQualityLastScanAt"
};

const DEFAULT_BOT_CONFIG = {
  enableCore: true,
  enablePlayback: true,
  enableLibrary: true,
  telegramToken: "",
  telegramChatId: "",
  enableCommands: true,
  notifyEvents: {
    start: true,
    pause: true,
    resume: true,
    stop: true
  },
  showIp: true,
  showIpGeo: true,
  showOverview: true,
  eventDedupSeconds: 10,
  wechatCorpId: "",
  wechatAgentId: "",
  wechatSecret: "",
  wechatToUser: "@all",
  wechatCallbackToken: "",
  wechatCallbackAes: ""
};

const DEFAULT_EMBY_CLIENT_NAME = "镜界Vistamirror User Console";
const QUALITY_RESOLUTION_ITEMS_QUERY =
  "/Items?Recursive=true&IncludeItemTypes=Movie,Episode&Fields=Path,ImageTags,SeriesPrimaryImageTag,ParentPrimaryImageTag,MediaSources,MediaStreams,Width,Height,VideoType,SeriesName,IndexNumber,ParentIndexNumber&Limit=2000";

const appState = {
  config: loadJson(STORAGE_KEYS.config, {
    serverUrl: "",
    apiKey: "",
    clientName: DEFAULT_EMBY_CLIENT_NAME,
    tmdbEnabled: false,
    tmdbToken: "",
    tmdbLanguage: "zh-CN",
    tmdbRegion: "CN"
  }),
  invites: loadJson(STORAGE_KEYS.invites, []),
  renewals: loadJson(STORAGE_KEYS.renewals, {}),
  botConfig: loadJson(STORAGE_KEYS.botConfig, DEFAULT_BOT_CONFIG),
  users: [],
  sessions: [],
  devices: [],
  logs: [],
  mediaCounts: null,
  qualityResolutionStats: null,
  qualityResolutionItemsByBucket: {},
  qualityResolutionActiveBucket: "uhd",
  qualityResolutionFilters: {
    type: "all",
    keyword: "",
    sort: "resolution_desc"
  },
  qualityResolutionFilteredEntries: [],
  qualityResolutionFocusBucketKey: "uhd",
  qualityResolutionFocusBucketTitle: "Ultra HD / 4K",
  contentRanking: [],
  contentRankingSource: "none",
  systemInfo: null,
  selectedUserId: null,
  userFilter: "all",
  userSearch: "",
  userCenterSearch: "",
  userCenterSort: "recommend",
  invitePresetDays: "30",
  generatedInviteLinks: [],
  logSearch: "",
  clientSoftwareFilter: "",
  clientDeviceFilter: "",
  pendingClientListScroll: false,
  syncEvents: [],
  toastTimer: null,
  settingsSaveTimer: null,
  playbackCoverCache: {},
  playbackCoverCandidateCache: {},
  playbackItemPosterCache: {},
  playbackCoverLookupRunning: false,
  tmdbPosterCache: {},
  tmdbPosterCacheTimestamps: {},
  tmdbInFlightMap: {},
  userConfigLibraries: [],
  userConfigEditingId: null,
  createUserDraft: buildDefaultCreateUserDraft(),
  activeView: localStorage.getItem(STORAGE_KEYS.activeView) || "",
  inviteSyncEndpoint: localStorage.getItem(STORAGE_KEYS.inviteSyncEndpoint) || "",
  botWebhookUrl: "",
  botWebhookState: null,
  botWebhookWarning: "",
  envControlledFields: {
    embyConfig: [],
    botConfig: []
  },
  botWebhookRefreshPromise: null,
  botWebhookStatusTimer: null,
  qualityRescanPromise: null,
  qualityLastScanAt: Number(localStorage.getItem(STORAGE_KEYS.qualityLastScanAt) || 0) || 0
};

function normalizeAppConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  return {
    serverUrl: String(config.serverUrl || "").trim(),
    apiKey: String(config.apiKey || "").trim(),
    clientName: DEFAULT_EMBY_CLIENT_NAME,
    tmdbEnabled: Boolean(config.tmdbEnabled),
    tmdbToken: String(config.tmdbToken || "").trim(),
    tmdbLanguage: String(config.tmdbLanguage || "zh-CN").trim() || "zh-CN",
    tmdbRegion: String(config.tmdbRegion || "CN").trim().toUpperCase() || "CN"
  };
}

function normalizeBotConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const defaults = DEFAULT_BOT_CONFIG;
  const notifyEventsSource =
    config.notifyEvents && typeof config.notifyEvents === "object" ? config.notifyEvents : defaults.notifyEvents;
  let dedupeSeconds = Number.parseInt(String(config.eventDedupSeconds ?? defaults.eventDedupSeconds), 10);
  if (!Number.isFinite(dedupeSeconds)) {
    dedupeSeconds = defaults.eventDedupSeconds;
  }
  dedupeSeconds = Math.max(1, Math.min(120, dedupeSeconds));
  return {
    enableCore: Boolean(config.enableCore ?? defaults.enableCore),
    enablePlayback: Boolean(config.enablePlayback ?? defaults.enablePlayback),
    enableLibrary: Boolean(config.enableLibrary ?? defaults.enableLibrary),
    telegramToken: String(config.telegramToken || defaults.telegramToken).trim(),
    telegramChatId: String(config.telegramChatId || defaults.telegramChatId).trim(),
    enableCommands: Boolean(config.enableCommands ?? defaults.enableCommands),
    notifyEvents: {
      start: Boolean(notifyEventsSource.start ?? defaults.notifyEvents.start),
      pause: Boolean(notifyEventsSource.pause ?? defaults.notifyEvents.pause),
      resume: Boolean(notifyEventsSource.resume ?? defaults.notifyEvents.resume),
      stop: Boolean(notifyEventsSource.stop ?? defaults.notifyEvents.stop)
    },
    showIp: Boolean(config.showIp ?? defaults.showIp),
    showIpGeo: Boolean(config.showIpGeo ?? defaults.showIpGeo),
    showOverview: Boolean(config.showOverview ?? defaults.showOverview),
    eventDedupSeconds: dedupeSeconds,
    wechatCorpId: String(config.wechatCorpId || defaults.wechatCorpId).trim(),
    wechatAgentId: String(config.wechatAgentId || defaults.wechatAgentId).trim(),
    wechatSecret: String(config.wechatSecret || defaults.wechatSecret).trim(),
    wechatToUser: String(config.wechatToUser || defaults.wechatToUser).trim() || defaults.wechatToUser,
    wechatCallbackToken: String(config.wechatCallbackToken || defaults.wechatCallbackToken).trim(),
    wechatCallbackAes: String(config.wechatCallbackAes || defaults.wechatCallbackAes).trim()
  };
}

function normalizeEnvControlledFields(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const normalizeList = (value) =>
    Array.isArray(value)
      ? value.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
  return {
    embyConfig: normalizeList(source.embyConfig),
    botConfig: normalizeList(source.botConfig)
  };
}

function mergeEnvControlledFields(raw, groupHint = "") {
  const current = normalizeEnvControlledFields(appState?.envControlledFields);
  const normalizeList = (value) =>
    Array.isArray(value)
      ? value.map((item) => String(item || "").trim()).filter(Boolean)
      : [];

  if (Array.isArray(raw)) {
    if (groupHint === "embyConfig" || groupHint === "botConfig") {
      current[groupHint] = normalizeList(raw);
    }
    return current;
  }

  if (raw && typeof raw === "object") {
    if (Object.prototype.hasOwnProperty.call(raw, "embyConfig")) {
      current.embyConfig = normalizeList(raw.embyConfig);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "botConfig")) {
      current.botConfig = normalizeList(raw.botConfig);
    }
  }

  return current;
}

function setFieldEnvControlled(input, controlled) {
  if (!input) {
    return;
  }
  input.disabled = Boolean(controlled);
  input.dataset.envControlled = controlled ? "true" : "false";
  if (controlled) {
    input.title = "该配置由环境变量控制，请在 .env 或 docker-compose.yml 中修改。";
  } else {
    input.removeAttribute("title");
  }
}

function renderEnvControlledState() {
  const embyManaged = appState?.envControlledFields?.embyConfig || [];
  const botManaged = appState?.envControlledFields?.botConfig || [];

  setFieldEnvControlled(elements.serverUrl, embyManaged.includes("serverUrl"));
  setFieldEnvControlled(elements.apiKey, embyManaged.includes("apiKey"));
  if (elements.apiKeyToggle) {
    elements.apiKeyToggle.disabled = embyManaged.includes("apiKey");
  }
  if (elements.settingsEnvManagedHint) {
    const hasManaged = embyManaged.length > 0;
    elements.settingsEnvManagedHint.hidden = !hasManaged;
    if (hasManaged) {
      elements.settingsEnvManagedHint.textContent = "该配置由环境变量控制，请在 .env 或 docker-compose.yml 中修改。";
    }
  }

  setFieldEnvControlled(elements.botTelegramToken, botManaged.includes("telegramToken"));
  setFieldEnvControlled(elements.botTelegramChatId, botManaged.includes("telegramChatId"));
  if (elements.botTelegramTokenToggle) {
    elements.botTelegramTokenToggle.disabled = botManaged.includes("telegramToken");
  }
  if (elements.botEnvManagedHint) {
    const hasManaged = botManaged.length > 0;
    elements.botEnvManagedHint.hidden = !hasManaged;
    if (hasManaged) {
      elements.botEnvManagedHint.textContent = "该配置由环境变量控制，请在 .env 或 docker-compose.yml 中修改。";
    }
  }
}

appState.config = normalizeAppConfig(appState.config);
appState.botConfig = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...appState.botConfig });
appState.qualityResolutionFilters = normalizeQualityResolutionFilters(appState.qualityResolutionFilters);

const elements = {
  navItems: document.querySelectorAll(".nav-item"),
  sidebarGlobalSearchTrigger: document.getElementById("global-search-trigger"),
  sidebarGlobalSearchInput: document.getElementById("sidebar-global-search"),
  viewSections: document.querySelectorAll(".view-section"),
  overviewStatsGrid: document.getElementById("overview-stats-grid"),
  mainContent: document.querySelector(".main-content"),
  topbarActions: document.getElementById("topbar-actions"),
  topbarLogsToolbarHost: document.getElementById("topbar-logs-toolbar-host"),
  topbarUserCenterActions: document.getElementById("topbar-user-center-actions"),
  topbarBotActions: document.getElementById("topbar-bot-actions"),
  ucInviteManageBtn: document.getElementById("uc-invite-manage-btn"),
  ucGenerateInviteBtn: document.getElementById("uc-generate-invite-btn"),
  ucCreateUserBtn: document.getElementById("uc-create-user-btn"),
  settingsSaveBtn: document.getElementById("settings-save-btn"),
  logsToolbarHost: document.getElementById("logs-toolbar-host"),
  logsToolbar: document.getElementById("logs-toolbar"),
  topbarIcon: document.querySelector(".topbar-icon"),
  topbarTitle: document.getElementById("topbar-title"),
  topbarSubtitle: document.getElementById("topbar-subtitle"),
  serverUrl: document.getElementById("server-url"),
  apiKey: document.getElementById("api-key"),
  apiKeyToggle: document.getElementById("api-key-toggle"),
  tmdbEnabled: document.getElementById("tmdb-enabled"),
  tmdbToken: document.getElementById("tmdb-token"),
  tmdbLanguage: document.getElementById("tmdb-language"),
  tmdbRegion: document.getElementById("tmdb-region"),
  tmdbStatusTip: document.getElementById("tmdb-status-tip"),
  tmdbTokenHint: document.getElementById("tmdb-token-hint"),
  connectBtn: document.getElementById("connect-btn"),
  diagnoseBtn: document.getElementById("diagnose-btn"),
  disconnectBtn: document.getElementById("disconnect-btn"),
  connectionBadge: document.getElementById("connection-badge"),
  connectionMessage: document.getElementById("connection-message"),
  settingsEnvManagedHint: document.getElementById("settings-env-managed-hint"),
  statMovies: document.getElementById("stat-movies"),
  statMoviesSub: document.getElementById("stat-movies-sub"),
  statSeries: document.getElementById("stat-series"),
  statSeriesSub: document.getElementById("stat-series-sub"),
  statEpisodes: document.getElementById("stat-episodes"),
  statEpisodesSub: document.getElementById("stat-episodes-sub"),
  statUsers: document.getElementById("stat-users"),
  statUsersSub: document.getElementById("stat-users-sub"),
  overviewRemote: document.getElementById("overview-remote"),
  overviewLivetv: document.getElementById("overview-livetv"),
  overviewInvites: document.getElementById("overview-invites"),
  profileMenuBtn: document.getElementById("profile-menu-btn"),
  profileMenuPanel: document.getElementById("profile-menu-panel"),
  profileOpenSettings: document.getElementById("profile-open-settings"),
  profileOpenSupport: document.getElementById("profile-open-support"),
  syncFeed: document.getElementById("sync-feed"),
  contentRankingBody: document.getElementById("content-ranking-body"),
  contentRankingSource: document.getElementById("content-ranking-source"),
  clientDeviceSource: document.getElementById("client-device-source"),
  clientStatTotal: document.getElementById("client-stat-total"),
  clientStatOnline: document.getElementById("client-stat-online"),
  clientStatBlocked: document.getElementById("client-stat-blocked"),
  clientEcosystemChart: document.getElementById("client-ecosystem-chart"),
  clientEcosystemTooltip: document.getElementById("client-ecosystem-tooltip"),
  clientEcosystemLegend: document.getElementById("client-ecosystem-legend"),
  clientEcosystemEmpty: document.getElementById("client-ecosystem-empty"),
  clientTopDevicesChart: document.getElementById("client-top-devices-chart"),
  clientTopDevicesEmpty: document.getElementById("client-top-devices-empty"),
  clientSoftwareFilterFeedback: document.getElementById("client-software-filter-feedback"),
  clientDevicesBody: document.getElementById("client-devices-body"),
  userSearch: document.getElementById("user-search"),
  userFilterButtons: document.querySelectorAll("[data-user-filter]"),
  usersBody: document.getElementById("users-body"),
  detailName: document.getElementById("detail-name"),
  detailStatus: document.getElementById("detail-status"),
  qualityLastScanTime: document.getElementById("quality-last-scan-time"),
  detailAvatar: document.getElementById("detail-avatar"),
  detailEmail: document.getElementById("detail-email"),
  detailNote: document.getElementById("detail-note"),
  detailLastLogin: document.getElementById("detail-last-login"),
  detailLastActivity: document.getElementById("detail-last-activity"),
  detailDeviceCount: document.getElementById("detail-device-count"),
  qualityResolutionBars: document.getElementById("quality-resolution-bars"),
  qualityResolutionTotal: document.getElementById("quality-resolution-total"),
  qualityResolutionFocusTitle: document.getElementById("quality-resolution-focus-title"),
  qualityResolutionFocusSubtitle: document.getElementById("quality-resolution-focus-subtitle"),
  qualityResolutionFocusBody: document.getElementById("quality-resolution-focus-body"),
  qualityResolutionFilterType: document.getElementById("quality-resolution-filter-type"),
  qualityResolutionFilterKeyword: document.getElementById("quality-resolution-filter-keyword"),
  qualityResolutionFilterSort: document.getElementById("quality-resolution-filter-sort"),
  qualityResolutionFilterSummary: document.getElementById("quality-resolution-filter-summary"),
  qualityResolutionExportBtn: document.getElementById("quality-resolution-export-btn"),
  qualityResolutionDetailTitle: document.getElementById("quality-resolution-detail-title"),
  qualityResolutionDetailSubtitle: document.getElementById("quality-resolution-detail-subtitle"),
  qualityResolutionDetailList: document.getElementById("quality-resolution-detail-list"),
  detailMeta: document.getElementById("detail-meta"),
  detailActivity: document.getElementById("detail-activity"),
  toggleRemote: document.getElementById("toggle-remote"),
  toggleDownload: document.getElementById("toggle-download"),
  toggleLivetv: document.getElementById("toggle-livetv"),
  toggleDisabled: document.getElementById("toggle-disabled"),
  qualityRescanBtn: document.getElementById("quality-rescan-btn"),
  userActionFeedback: document.getElementById("user-action-feedback"),
  inviteForm: document.getElementById("invite-form"),
  inviteLabel: document.getElementById("invite-label"),
  inviteUsername: document.getElementById("invite-username"),
  inviteDays: document.getElementById("invite-days"),
  invitePlan: document.getElementById("invite-plan"),
  inviteList: document.getElementById("invite-list"),
  userCenterSearch: document.getElementById("user-center-search"),
  userCenterSort: document.getElementById("user-center-sort"),
  userCenterBody: document.getElementById("user-center-body"),
  ucStatTotal: document.getElementById("uc-stat-total"),
  ucStatVip: document.getElementById("uc-stat-vip"),
  ucStatActive: document.getElementById("uc-stat-active"),
  ucStatExpiring: document.getElementById("uc-stat-expiring"),
  ucStatBlocked: document.getElementById("uc-stat-blocked"),
  ucCreateUserModal: document.getElementById("uc-create-user-modal"),
  ucCreateUserCloseIconBtn: document.getElementById("uc-create-user-close-icon-btn"),
  ucCreateTabs: document.querySelectorAll("#uc-create-user-modal .create-user-tab"),
  ucCreatePanes: document.querySelectorAll("#uc-create-user-modal .create-user-pane"),
  ucCreateUsername: document.getElementById("uc-create-username"),
  ucCreateNote: document.getElementById("uc-create-note"),
  ucCreatePassword: document.getElementById("uc-create-password"),
  ucCreateConfirmPassword: document.getElementById("uc-create-confirm-password"),
  ucCreateStatus: document.getElementById("uc-create-status"),
  ucCreateExpiry: document.getElementById("uc-create-expiry"),
  ucCreateTemplate: document.getElementById("uc-create-template"),
  ucCreateSourceUser: document.getElementById("uc-create-source-user"),
  ucCreateRefreshSourceUsersBtn: document.getElementById("uc-create-refresh-source-users-btn"),
  ucCreateApplyPresetBtn: document.getElementById("uc-create-apply-preset-btn"),
  ucCreateRemote: document.getElementById("uc-create-remote"),
  ucCreateLivetv: document.getElementById("uc-create-livetv"),
  ucCreateDeleteMedia: document.getElementById("uc-create-delete-media"),
  ucCreateManageCollections: document.getElementById("uc-create-manage-collections"),
  ucCreateRatingList: document.getElementById("uc-create-rating-list"),
  ucCreateEnableAllFolders: document.getElementById("uc-create-enable-all-folders"),
  ucCreateResetFolders: document.getElementById("uc-create-reset-folders"),
  ucCreateFoldersList: document.getElementById("uc-create-folders-list"),
  ucCreateMaxRating: document.getElementById("uc-create-max-rating"),
  ucCreateStreamLimit: document.getElementById("uc-create-stream-limit"),
  ucCreateDownload: document.getElementById("uc-create-download"),
  ucCreateVideoTranscode: document.getElementById("uc-create-video-transcode"),
  ucCreateAudioTranscode: document.getElementById("uc-create-audio-transcode"),
  ucCreateFeedback: document.getElementById("uc-create-feedback"),
  ucCreateCancelBtn: document.getElementById("uc-create-cancel-btn"),
  ucCreateSaveBtn: document.getElementById("uc-create-save-btn"),
  ucUserConfigModal: document.getElementById("uc-user-config-modal"),
  ucUserConfigCloseIconBtn: document.getElementById("uc-user-config-close-icon-btn"),
  ucUserConfigCancelBtn: document.getElementById("uc-user-config-cancel-btn"),
  ucUserConfigSaveBtn: document.getElementById("uc-user-config-save-btn"),
  ucConfigTabs: document.querySelectorAll(".user-config-tab"),
  ucConfigPanes: document.querySelectorAll(".user-config-pane"),
  ucConfigUsername: document.getElementById("uc-config-username"),
  ucConfigNote: document.getElementById("uc-config-note"),
  ucConfigPassword: document.getElementById("uc-config-password"),
  ucConfigStatus: document.getElementById("uc-config-status"),
  ucConfigExpiry: document.getElementById("uc-config-expiry"),
  ucConfigEnableAllFolders: document.getElementById("uc-config-enable-all-folders"),
  ucConfigResetFolders: document.getElementById("uc-config-reset-folders"),
  ucConfigFoldersList: document.getElementById("uc-config-folders-list"),
  ucConfigStreamLimit: document.getElementById("uc-config-stream-limit"),
  ucConfigAdmin: document.getElementById("uc-config-admin"),
  ucConfigRemote: document.getElementById("uc-config-remote"),
  ucConfigDownload: document.getElementById("uc-config-download"),
  ucConfigLivetv: document.getElementById("uc-config-livetv"),
  ucInviteManageModal: document.getElementById("uc-invite-manage-modal"),
  ucInviteManageList: document.getElementById("uc-invite-manage-list"),
  ucInviteSelectAll: document.getElementById("uc-invite-select-all"),
  ucInviteBulkDeleteBtn: document.getElementById("uc-invite-bulk-delete-btn"),
  ucInviteManageCloseBtn: document.getElementById("uc-invite-manage-close-btn"),
  ucInviteManageCloseIconBtn: document.getElementById("uc-invite-manage-close-icon-btn"),
  ucInviteModal: document.getElementById("uc-invite-modal"),
  ucInviteResultModal: document.getElementById("uc-invite-result-modal"),
  ucInviteForm: document.getElementById("uc-invite-form"),
  ucInvitePresetButtons: document.querySelectorAll(".invite-preset-btn"),
  ucInviteCustomDays: document.getElementById("uc-invite-custom-days"),
  ucInviteCustomInc: document.getElementById("uc-invite-custom-inc"),
  ucInviteCustomDec: document.getElementById("uc-invite-custom-dec"),
  ucInviteQuantity: document.getElementById("uc-invite-quantity"),
  ucInviteTemplate: document.getElementById("uc-invite-template"),
  ucInviteCancelBtn: document.getElementById("uc-invite-cancel-btn"),
  ucInviteResultList: document.getElementById("uc-invite-result-list"),
  ucInviteCopyAllBtn: document.getElementById("uc-invite-copy-all-btn"),
  ucInviteDoneBtn: document.getElementById("uc-invite-done-btn"),
  renewalList: document.getElementById("renewal-list"),
  renewalForm: document.getElementById("renewal-form"),
  renewalUser: document.getElementById("renewal-user"),
  renewalPlan: document.getElementById("renewal-plan"),
  renewalExpiry: document.getElementById("renewal-expiry"),
  renewalNote: document.getElementById("renewal-note"),
  renewalFeedback: document.getElementById("renewal-feedback"),
  logSearch: document.getElementById("log-search"),
  logList: document.getElementById("log-list"),
  logUserFilter: document.getElementById("log-user-filter"),
  logQueryBtn: document.getElementById("log-query-btn"),
  playbackTodayCount: document.getElementById("playback-today-count"),
  playbackTodayDuration: document.getElementById("playback-today-duration"),
  playbackActiveUsers: document.getElementById("playback-active-users"),
  playbackTotalCount: document.getElementById("playback-total-count"),
  botSaveBtn: document.getElementById("bot-save-btn"),
  botEnableCore: document.getElementById("bot-enable-core"),
  botEnablePlayback: document.getElementById("bot-enable-playback"),
  botEnableLibrary: document.getElementById("bot-enable-library"),
  botEnableCommands: document.getElementById("bot-enable-commands"),
  botEnvManagedHint: document.getElementById("bot-env-managed-hint"),
  botEventStart: document.getElementById("bot-event-start"),
  botEventPause: document.getElementById("bot-event-pause"),
  botEventResume: document.getElementById("bot-event-resume"),
  botEventStop: document.getElementById("bot-event-stop"),
  botShowIp: document.getElementById("bot-show-ip"),
  botShowIpGeo: document.getElementById("bot-show-ip-geo"),
  botShowOverview: document.getElementById("bot-show-overview"),
  botDedupeSeconds: document.getElementById("bot-dedupe-seconds"),
  botWebhookUrl: document.getElementById("bot-webhook-url"),
  botWebhookStatus: document.getElementById("bot-webhook-status"),
  botTelegramToken: document.getElementById("bot-telegram-token"),
  botTelegramTokenToggle: document.getElementById("bot-telegram-token-toggle"),
  botTelegramChatId: document.getElementById("bot-telegram-chat-id"),
  botTelegramTest: document.getElementById("bot-telegram-test"),
  botWechatCorpId: document.getElementById("bot-wechat-corp-id"),
  botWechatAgentId: document.getElementById("bot-wechat-agent-id"),
  botWechatSecret: document.getElementById("bot-wechat-secret"),
  botWechatToUser: document.getElementById("bot-wechat-to-user"),
  botWechatTest: document.getElementById("bot-wechat-test"),
  botWechatCallbackToken: document.getElementById("bot-wechat-callback-token"),
  botWechatCallbackAes: document.getElementById("bot-wechat-callback-aes"),
  botWechatCallbackUrl: document.getElementById("bot-wechat-callback-url"),
  botCopyCallbackUrl: document.getElementById("bot-copy-callback-url"),
  botFeedback: document.getElementById("bot-feedback"),
  globalToast: document.getElementById("global-toast"),
  passwordToggles: document.querySelectorAll(".password-toggle")
};

const VIEW_META = {
  overview: {
    icon: "📊",
    title: "仪表盘",
    subtitle: "查看全局用户状态、连接情况与关键指标"
  },
  "content-ranking": {
    icon: "🏆",
    title: "内容排行",
    subtitle: "按播放热度、时长与互动数据查看内容表现"
  },
  "data-insights": {
    icon: "🔎",
    title: "数据洞察",
    subtitle: "追踪活跃趋势、设备结构和行为波动"
  },
  users: {
    icon: "✅",
    title: "质量盘点",
    subtitle: "聚焦用户健康度、策略状态与可用性检查"
  },
  missing: {
    icon: "🧩",
    title: "缺集管理",
    subtitle: "巡检缺失条目并推进补集处理流程"
  },
  dedup: {
    icon: "🗂",
    title: "去重管理",
    subtitle: "识别重复资源并执行统一治理策略"
  },
  logs: {
    icon: "🕘",
    title: "播放历史",
    subtitle: "回看 Emby 活动日志与播放行为轨迹"
  },
  invites: {
    icon: "🔗",
    title: "邀请码管理",
    subtitle: "维护邀请创建、使用状态与新用户引导"
  },
  renewals: {
    icon: "♻️",
    title: "续费管理",
    subtitle: "集中管理到期用户、套餐与续期进度"
  },
  workorders: {
    icon: "🎬",
    title: "工单大厅",
    subtitle: "处理用户反馈、异常事件与运营任务"
  },
  "user-center": {
    icon: "👥",
    title: "用户管理",
    subtitle: "聚焦账号生命周期、权限与状态控制"
  },
  points: {
    icon: "🧮",
    title: "积分引擎",
    subtitle: "维护积分规则、发放策略与兑换体系"
  },
  "risk-control": {
    icon: "🛡️",
    title: "风险管控",
    subtitle: "识别高风险行为并执行拦截与告警策略"
  },
  "client-control": {
    icon: "🖥️",
    title: "客户端管控",
    subtitle: "管理设备接入策略、版本与并发限制"
  },
  calendar: {
    icon: "📅",
    title: "追剧日历",
    subtitle: "查看更新排期并追踪订阅提醒"
  },
  "bot-assistant": {
    icon: "🤖",
    title: "机器人助手",
    subtitle: "配置通知机器人和自动化助手流程"
  },
  "task-center": {
    icon: "⚡",
    title: "任务中心",
    subtitle: "管理自动化任务队列、执行状态与重试"
  },
  workshop: {
    icon: "🛠️",
    title: "映迹工坊",
    subtitle: "管理媒体处理工作流与产出状态"
  },
  settings: {
    icon: "⚙️",
    title: "系统设置",
    subtitle: "全局参数、API 密钥与自动化规则配置"
  },
  "about-support": {
    icon: "💬",
    title: "关于与支持",
    subtitle: "查看版本信息、帮助文档与支持入口"
  },
  plugins: {
    icon: "🧱",
    title: "插件中心",
    subtitle: "扩展系统能力并管理插件生命周期"
  }
};

const EYE_OPEN_SVG = `
<svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true" focusable="false">
  <path d="M2.4 12c0 0 3.6-6.3 9.6-6.3S21.6 12 21.6 12s-3.6 6.3-9.6 6.3S2.4 12 2.4 12z" fill="#722ED1" fill-opacity="0.16" stroke="#722ED1" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></path>
  <circle cx="12" cy="12" r="3.1" fill="#722ED1"></circle>
  <circle cx="11.1" cy="10.9" r="1.1" fill="#FFFFFF"></circle>
</svg>
`;

const EYE_CLOSED_SVG = `
<svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true" focusable="false">
  <path d="M2.4 12c0 0 3.6-6.3 9.6-6.3S21.6 12 21.6 12s-3.6 6.3-9.6 6.3S2.4 12 2.4 12z" fill="#722ED1" fill-opacity="0.08" stroke="#722ED1" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></path>
  <circle cx="12" cy="12" r="3.1" fill="#722ED1" fill-opacity="0.82"></circle>
  <path d="M4.3 19.4L19.7 4.6" stroke="#722ED1" stroke-width="2.2" stroke-linecap="round"></path>
</svg>
`;

const CREATE_USER_RATING_OPTIONS = [
  { value: "0", label: "全部" },
  { value: "6", label: "6+" },
  { value: "9", label: "9+" },
  { value: "13", label: "13+" },
  { value: "16", label: "16+" },
  { value: "18", label: "18+" }
];

function buildDefaultCreateUserDraft() {
  return {
    username: "",
    note: "",
    password: "",
    confirmPassword: "",
    status: "active",
    expiry: "",
    template: "default",
    sourceUserId: "",
    enableAllFolders: true,
    selectedFolders: [],
    allowRemoteAccess: true,
    allowLiveTvAccess: false,
    allowDeleteMedia: false,
    allowManageCollections: false,
    allowDownload: false,
    allowVideoTranscoding: true,
    allowAudioTranscoding: true,
    selectedRatings: ["18"],
    maxParentalRating: "18",
    streamLimit: 0
  };
}

function loadJson(key, fallback) {
  try {
    const value = localStorage.getItem(key);
    return value ? JSON.parse(value) : fallback;
  } catch {
    return fallback;
  }
}

function saveJson(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function normalizeServerUrl(raw) {
  if (!raw) {
    return "";
  }

  let url = raw.trim().replace(/\/+$/, "");
  if (!/^https?:\/\//i.test(url)) {
    url = `http://${url}`;
  }
  if (!/\/emby$/i.test(url)) {
    url = `${url}/emby`;
  }
  return url;
}

function getHeaders() {
  return {
    "Content-Type": "application/json",
    "X-Emby-Client": appState.config.clientName || DEFAULT_EMBY_CLIENT_NAME,
    "X-Emby-Device-Name": "Web Browser",
    "X-Emby-Device-Id": "emby-pulse-web-console",
    "X-Emby-Client-Version": "1.0.0"
  };
}

function shouldUseLocalProxy() {
  return window.location.protocol === "http:" || window.location.protocol === "https:";
}

function appendApiKeyToPath(path, apiKey) {
  const [pathname, query = ""] = String(path || "").split("?");
  const params = new URLSearchParams(query);
  params.set("api_key", apiKey);
  const nextQuery = params.toString();
  return nextQuery ? `${pathname}?${nextQuery}` : pathname;
}

async function embyFetch(path, options = {}) {
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    throw new Error("请先填写 Emby 地址和 API Key。");
  }

  const useProxy = shouldUseLocalProxy();
  const targetUrl = useProxy ? `/api/emby${path}` : `${appState.config.serverUrl}${path}`;
  const mergedHeaders = {
    ...getHeaders(),
    ...(options.headers || {})
  };

  if (useProxy) {
    mergedHeaders["X-Emby-Base-Url"] = appState.config.serverUrl;
    mergedHeaders["X-Emby-Api-Key"] = appState.config.apiKey;
  } else {
    mergedHeaders["X-Emby-Token"] = appState.config.apiKey;
  }

  const parseResponse = async (response) => {
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`请求失败 ${response.status}：${text || response.statusText}`);
    }

    if (response.status === 204) {
      return null;
    }

    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      return response.json();
    }

    return response.text();
  };

  const tryDirectFetch = async () => {
    const directPath = appendApiKeyToPath(path, appState.config.apiKey);
    const directUrl = `${appState.config.serverUrl}${directPath}`;
    const method = String(options.method || "GET").toUpperCase();
    const directHeaders = { ...(options.headers || {}) };

    // 直连时尽量避免自定义头，减少 CORS 预检失败概率
    if (method !== "GET" && method !== "HEAD" && !directHeaders["Content-Type"]) {
      directHeaders["Content-Type"] = "application/json";
    }

    const directResponse = await fetch(directUrl, {
      ...options,
      headers: directHeaders
    });
    return parseResponse(directResponse);
  };

  let response;
  try {
    response = await fetch(targetUrl, {
      ...options,
      headers: mergedHeaders
    });
  } catch (error) {
    if (useProxy) {
      try {
        return await tryDirectFetch();
      } catch {
        throw new Error("无法连接本地代理或直连失败。请先运行 `python3 dev_server.py`，或在 Emby 开启 CORS 后再重试。");
      }
    }
    throw new Error(`网络请求失败：${error.message || "请检查地址与证书设置"}`);
  }

  if (useProxy && !response.ok && [404, 502, 503, 504].includes(response.status)) {
    try {
      return await tryDirectFetch();
    } catch {
      // ignore and keep proxy error below
    }
  }

  return parseResponse(response);
}

function isTmdbFallbackEnabled() {
  return Boolean(appState?.config?.tmdbEnabled && String(appState?.config?.tmdbToken || "").trim());
}

function dedupeStringList(rows) {
  return Array.from(new Set((rows || []).filter(Boolean)));
}

function normalizePosterMediaType(type) {
  const text = String(type || "").toLowerCase();
  if (text.includes("movie")) {
    return "movie";
  }
  if (text.includes("season")) {
    return "season";
  }
  if (text.includes("series") || text.includes("tv") || text.includes("episode")) {
    return "series";
  }
  return "other";
}

function extractSeasonNoFromTitle(title, fallback = 0) {
  const text = String(title || "");
  const cnMatch = text.match(/第\s*(\d+)\s*季/i);
  if (cnMatch?.[1]) {
    return Number(cnMatch[1]) || 0;
  }
  const sMatch = text.match(/\bS\s*(\d+)\b/i);
  if (sMatch?.[1]) {
    return Number(sMatch[1]) || 0;
  }
  const seasonMatch = text.match(/\bseason\s*(\d+)\b/i);
  if (seasonMatch?.[1]) {
    return Number(seasonMatch[1]) || 0;
  }
  return Math.max(0, Number(fallback) || 0);
}

function extractYearFromTitle(title, fallback = 0) {
  const candidate = Math.max(0, Number(fallback) || 0);
  if (candidate >= 1900 && candidate <= 2100) {
    return candidate;
  }
  const text = String(title || "");
  const match = text.match(/\b(19\d{2}|20\d{2}|21\d{2})\b/);
  return match?.[1] ? Number(match[1]) : 0;
}

function stripPosterSearchTitle(title) {
  return String(title || "")
    .replace(/\s*[-–—]\s*第\s*\d+\s*季\s*$/i, "")
    .replace(/\s*[-–—]\s*season\s*\d+\s*$/i, "")
    .replace(/\s*[-–—]\s*s\s*\d+\s*$/i, "")
    .replace(/\s*第\s*\d+\s*季\s*$/i, "")
    .trim();
}

function buildPosterSearchTitles(title) {
  const raw = String(title || "").trim();
  const normalized = raw
    .replace(/\s*[-–—]\s*S\d+\s*,?\s*E(?:P)?\d+\b.*$/i, "")
    .replace(/\s*[-–—]\s*S\d+\s*E\d+\b.*$/i, "")
    .replace(/\s*[-–—]\s*第\s*\d+\s*季\s*第\s*\d+\s*集.*$/i, "")
    .replace(/\s*[-–—]\s*第\s*\d+\s*集.*$/i, "")
    .trim() || raw;
  const stripped = stripPosterSearchTitle(raw);
  const normalizedStripped = stripPosterSearchTitle(normalized);
  const beforeDash = raw.split("-")[0]?.trim() || "";
  const beforeLongDash = raw.split("—")[0]?.trim() || "";
  return dedupeStringList([raw, normalized, stripped, normalizedStripped, beforeDash, beforeLongDash].filter(Boolean));
}

function buildTmdbImageUrl(path, size = "w500") {
  const cleanPath = String(path || "").trim();
  if (!cleanPath) {
    return "";
  }
  return `https://image.tmdb.org/t/p/${size}${cleanPath}`;
}

function buildTmdbPosterCacheKey({ title = "", type = "", year = 0, seasonNo = 0, size = "w500" }) {
  const normalizedTitle = normalizeMediaLookupKey(title);
  const normalizedType = normalizePosterMediaType(type);
  return `${normalizedType}|${normalizedTitle}|${Math.max(0, Number(year) || 0)}|${Math.max(0, Number(seasonNo) || 0)}|${size}`;
}

function pruneTmdbPosterCache() {
  const ttlMs = 10 * 60 * 1000;
  const now = Date.now();
  Object.keys(appState.tmdbPosterCacheTimestamps || {}).forEach((key) => {
    const ts = Number(appState.tmdbPosterCacheTimestamps[key] || 0);
    if (!ts || now - ts > ttlMs) {
      delete appState.tmdbPosterCacheTimestamps[key];
      delete appState.tmdbPosterCache[key];
    }
  });
}

async function tmdbFetchJson(path, query = {}) {
  const token = String(appState?.config?.tmdbToken || "").trim();
  if (!token) {
    throw new Error("TMDB Token 未配置");
  }
  const params = new URLSearchParams();
  Object.entries(query || {}).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }
    params.set(key, String(value));
  });
  const url = `https://api.themoviedb.org${path}${params.toString() ? `?${params.toString()}` : ""}`;
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${token}`
    }
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`TMDB 请求失败 ${response.status}${text ? `: ${text}` : ""}`);
  }
  return response.json();
}

function scoreTmdbResult(item, searchTitle, targetYear = 0) {
  const name = String(item?.name || item?.title || "").trim();
  const nameKey = normalizeMediaLookupKey(name);
  const queryKey = normalizeMediaLookupKey(searchTitle);
  let score = 0;
  if (!nameKey || !queryKey) {
    return score;
  }
  if (nameKey === queryKey) {
    score += 1000;
  } else if (nameKey.includes(queryKey)) {
    score += 700;
  } else if (queryKey.includes(nameKey)) {
    score += 500;
  } else {
    score += 50;
  }

  const resultYear = Number(String(item?.first_air_date || item?.release_date || "").slice(0, 4)) || 0;
  if (targetYear > 0 && resultYear > 0) {
    const diff = Math.abs(targetYear - resultYear);
    if (diff === 0) {
      score += 120;
    } else if (diff <= 1) {
      score += 80;
    } else if (diff <= 2) {
      score += 30;
    } else {
      score -= 30;
    }
  }
  if (item?.poster_path) {
    score += 90;
  }
  return score;
}

async function resolveTmdbPosterCandidates({ title = "", type = "", year = 0, seasonNo = 0, topSize = "w780", listSize = "w500" } = {}) {
  const normalizedType = normalizePosterMediaType(type);
  const searchTitles = buildPosterSearchTitles(title);
  if (!isTmdbFallbackEnabled() || searchTitles.length === 0) {
    return { top: [], list: [], source: "fallback" };
  }

  pruneTmdbPosterCache();
  const primaryTitle = searchTitles[0] || "";
  const normalizedYear = extractYearFromTitle(primaryTitle, year);
  const normalizedSeasonNo = extractSeasonNoFromTitle(primaryTitle, seasonNo);
  const cacheKey = buildTmdbPosterCacheKey({
    title: primaryTitle,
    type: normalizedType,
    year: normalizedYear,
    seasonNo: normalizedSeasonNo,
    size: `${topSize}|${listSize}`
  });
  if (Object.prototype.hasOwnProperty.call(appState.tmdbPosterCache, cacheKey)) {
    return appState.tmdbPosterCache[cacheKey];
  }
  if (appState.tmdbInFlightMap[cacheKey]) {
    return appState.tmdbInFlightMap[cacheKey];
  }

  const worker = (async () => {
    let best = null;
    let bestScore = Number.NEGATIVE_INFINITY;
    const endpoint = normalizedType === "movie" ? "/3/search/movie" : "/3/search/tv";

    for (const searchTitle of searchTitles.slice(0, 4)) {
      try {
        const result = await tmdbFetchJson(endpoint, {
          query: searchTitle,
          language: appState.config.tmdbLanguage || "zh-CN",
          region: appState.config.tmdbRegion || "CN",
          include_adult: false
        });
        const rows = Array.isArray(result?.results) ? result.results : [];
        rows.slice(0, 8).forEach((row) => {
          const score = scoreTmdbResult(row, searchTitle, normalizedYear);
          if (score > bestScore) {
            bestScore = score;
            best = row;
          }
        });
      } catch {
        // ignore per-title error
      }
    }

    if (!best) {
      const empty = { top: [], list: [], source: "fallback" };
      appState.tmdbPosterCache[cacheKey] = empty;
      appState.tmdbPosterCacheTimestamps[cacheKey] = Date.now();
      return empty;
    }

    const candidatesTop = [];
    const candidatesList = [];
    if (normalizedType !== "movie") {
      const tvId = Number(best?.id || 0);
      if (tvId > 0 && normalizedSeasonNo > 0) {
        try {
          const seasonDetail = await tmdbFetchJson(`/3/tv/${tvId}/season/${normalizedSeasonNo}`, {
            language: appState.config.tmdbLanguage || "zh-CN"
          });
          const seasonPoster = String(seasonDetail?.poster_path || "").trim();
          if (seasonPoster) {
            candidatesTop.push(buildTmdbImageUrl(seasonPoster, topSize));
            candidatesList.push(buildTmdbImageUrl(seasonPoster, listSize));
          }
        } catch {
          // ignore season-level fallback failure
        }
      }
    }

    const commonPoster = String(best?.poster_path || "").trim();
    if (commonPoster) {
      candidatesTop.push(buildTmdbImageUrl(commonPoster, topSize));
      candidatesList.push(buildTmdbImageUrl(commonPoster, listSize));
    }
    const payload = {
      top: dedupeStringList(candidatesTop.filter(Boolean)),
      list: dedupeStringList(candidatesList.filter(Boolean)),
      source: candidatesTop.length || candidatesList.length ? "tmdb" : "fallback"
    };
    appState.tmdbPosterCache[cacheKey] = payload;
    appState.tmdbPosterCacheTimestamps[cacheKey] = Date.now();
    return payload;
  })();

  appState.tmdbInFlightMap[cacheKey] = worker;
  try {
    return await worker;
  } finally {
    delete appState.tmdbInFlightMap[cacheKey];
  }
}

async function resolvePosterCandidates({
  title = "",
  type = "",
  year = 0,
  seasonNo = 0,
  embyTopCandidates = [],
  embyListCandidates = [],
  topSize = "w780",
  listSize = "w500",
  includeTmdbBackup = false
} = {}) {
  const embyTop = dedupeStringList((embyTopCandidates || []).filter(Boolean));
  const embyList = dedupeStringList((embyListCandidates || []).filter(Boolean));
  if ((embyTop.length || embyList.length) && !includeTmdbBackup) {
    return {
      top: embyTop,
      list: embyList,
      source: embyTop.length || embyList.length ? "emby" : "fallback"
    };
  }
  if (!isTmdbFallbackEnabled()) {
    return {
      top: embyTop,
      list: embyList,
      source: embyTop.length || embyList.length ? "emby" : "fallback"
    };
  }

  const tmdb = await resolveTmdbPosterCandidates({
    title,
    type,
    year,
    seasonNo,
    topSize,
    listSize
  });
  return {
    top: dedupeStringList([...(embyTop || []), ...(tmdb.top || [])]),
    list: dedupeStringList([...(embyList || []), ...(tmdb.list || [])]),
    source: tmdb.source || "fallback"
  };
}

async function inviteApiFetch(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });

  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    if (body && typeof body === "object" && body.error) {
      throw new Error(body.error);
    }
    throw new Error(typeof body === "string" ? body : `请求失败 ${response.status}`);
  }
  return body;
}

function normalizeInviteStatus(rawStatus) {
  const text = String(rawStatus || "").trim().toLowerCase();
  if (text === "used" || text === "已用") {
    return "used";
  }
  if (text === "active" || text === "空闲" || text === "idle" || text === "free") {
    return "active";
  }
  if (text === "expired") {
    return "expired";
  }
  return "invalid";
}

function normalizeInviteRecord(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  const code = String(record.code || "").trim();
  if (!code) {
    return null;
  }

  const rawDays = record.initialDays ?? record.duration ?? null;
  const parsedDays = Number(rawDays);
  const initialDays = Number.isFinite(parsedDays) && parsedDays > 0 ? Math.floor(parsedDays) : null;
  const status = normalizeInviteStatus(record.statusCode || record.status);

  return {
    id: String(record.id || ""),
    code,
    label: String(record.label || "").trim(),
    username: String(record.username || "").trim(),
    plan: String(record.plan || "").trim(),
    initialDays,
    duration: initialDays,
    expiresAt: String(record.expiresAt || ""),
    status,
    createdAt: String(record.createdAt || ""),
    usedAt: String(record.usedAt || ""),
    createdUserId: String(record.createdUserId || ""),
    usedUsername: String(record.usedUsername || "").trim()
  };
}

function applyInvitesFromServer(invites, options = {}) {
  const { persist = true, render = true } = options;
  if (!Array.isArray(invites)) {
    return false;
  }

  const normalized = invites.map((item) => normalizeInviteRecord(item)).filter(Boolean);
  appState.invites = normalized;
  if (persist) {
    persistLocalState();
  }
  if (render) {
    renderInvites();
    renderOverview();
    renderUserCenter();
  }
  return true;
}

function normalizeBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function getInviteSyncCandidates() {
  const candidates = [];
  const remembered = normalizeBaseUrl(appState.inviteSyncEndpoint);
  if (remembered) {
    candidates.push(remembered);
  }

  if (typeof window !== "undefined" && window.location?.origin) {
    const origin = normalizeBaseUrl(window.location.origin);
    candidates.push(`${origin}/api/invite/sync`);

    const pathname = String(window.location.pathname || "/");
    const basePath = pathname.endsWith("/")
      ? pathname
      : pathname.slice(0, pathname.lastIndexOf("/") + 1);
    candidates.push(`${origin}${basePath}api/invite/sync`);
  }

  const inviteBase = normalizeBaseUrl(getInvitePublicBaseUrl());
  if (inviteBase) {
    candidates.push(`${inviteBase}/api/invite/sync`);
  }

  candidates.push("/api/invite/sync");
  return Array.from(new Set(candidates));
}

function buildInviteSyncPayload() {
  const embyManaged = appState?.envControlledFields?.embyConfig || [];
  const embyConfig = {
    serverUrl: appState.config.serverUrl || "",
    apiKey: appState.config.apiKey || "",
    clientName: appState.config.clientName || DEFAULT_EMBY_CLIENT_NAME
  };
  if (embyManaged.includes("serverUrl")) {
    delete embyConfig.serverUrl;
  }
  if (embyManaged.includes("apiKey")) {
    delete embyConfig.apiKey;
  }
  if (embyManaged.includes("clientName")) {
    delete embyConfig.clientName;
  }

  return {
    embyConfig,
    invites: appState.invites.map((invite) => ({
      id: invite.id || "",
      code: invite.code || "",
      label: invite.label || "",
      username: invite.username || "",
      plan: invite.plan || "",
      initialDays: invite.initialDays ?? null,
      duration: invite.initialDays ?? invite.duration ?? null,
      expiresAt: invite.expiresAt || "",
      status: invite.status || "active",
      createdAt: invite.createdAt || "",
      usedAt: invite.usedAt || "",
      createdUserId: invite.createdUserId || "",
      usedUsername: invite.usedUsername || ""
    }))
  };
}

async function syncInviteStore(options = {}) {
  const {
    silentSuccess = true,
    failureToast = "邀请码同步失败，链接暂不可用，请稍后重试。",
    failureEventTitle = "邀请码同步失败",
    successToast = ""
  } = options;

  const payload = JSON.stringify(buildInviteSyncPayload());
  const endpoints = getInviteSyncCandidates();
  let lastError = null;

  for (let round = 0; round < 3; round += 1) {
    for (const endpoint of endpoints) {
      try {
        const result = await inviteApiFetch(endpoint, {
          method: "POST",
          body: payload
        });

        appState.inviteSyncEndpoint = endpoint;
        localStorage.setItem(STORAGE_KEYS.inviteSyncEndpoint, endpoint);

        if (!silentSuccess) {
          addSyncEvent(
            "邀请码同步成功",
            `服务端已同步 ${result?.storedInviteCount ?? appState.invites.length} 条邀请码。`,
            "success"
          );
        }
        if (successToast) {
          showToast(successToast, 1000);
        }
        return true;
      } catch (error) {
        lastError = error;
      }
    }

    if (round < 2) {
      await new Promise((resolve) => setTimeout(resolve, 280 * (round + 1)));
    }
  }

  showToast(failureToast, 1500);
  addSyncEvent(failureEventTitle, lastError?.message || "未知错误", "danger");
  return false;
}

async function refreshInvitesFromServer(options = {}) {
  const { silent = true } = options;
  try {
    const result = await inviteApiFetch("/api/invite/list");
    const ok = applyInvitesFromServer(result?.invites, { persist: true, render: true });
    if (!ok) {
      return false;
    }
    return true;
  } catch (error) {
    if (!silent) {
      showToast("读取服务端邀请码失败", 1200);
      addSyncEvent("读取邀请码失败", error.message || "未知错误", "danger");
    }
    return false;
  }
}

async function refreshInviteSyncStatus(options = {}) {
  const { silent = true } = options;
  try {
    const result = await inviteApiFetch("/api/invite/sync-status");
    if (result?.embyConfig && typeof result.embyConfig === "object") {
      appState.config = normalizeAppConfig({ ...appState.config, ...result.embyConfig });
    }
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "embyConfig");
    applyInvitesFromServer(result?.invites || [], { persist: true, render: true });
    persistLocalState();
    hydrateInputs();
    renderEnvControlledState();
    return Boolean(result?.synced !== false);
  } catch (error) {
    if (!silent) {
      showToast("邀请码同步状态获取失败", 1200);
      addSyncEvent("邀请码状态读取失败", error.message || "未知错误", "danger");
    }
    return false;
  }
}

async function generateInvitesOnServer(payload) {
  const result = await inviteApiFetch("/api/invite/generate", {
    method: "POST",
    body: JSON.stringify(payload || {})
  });
  if (Array.isArray(result?.invites)) {
    applyInvitesFromServer(result.invites, { persist: true, render: true });
  }
  return result;
}

function addSyncEvent(title, description, tone = "neutral") {
  appState.syncEvents.unshift({
    title,
    description,
    tone,
    time: new Date().toLocaleString("zh-CN")
  });
  appState.syncEvents = appState.syncEvents.slice(0, 6);
  renderOverview();
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatScanDateTime(value) {
  if (!value) {
    return "未执行重扫";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "未执行重扫";
  }
  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function renderQualityScanMeta() {
  if (elements.qualityLastScanTime) {
    elements.qualityLastScanTime.textContent = formatScanDateTime(appState.qualityLastScanAt);
  }
}

function commitQualityScanTime(value = Date.now()) {
  const timestamp = Number(value) || Date.now();
  appState.qualityLastScanAt = timestamp;
  localStorage.setItem(STORAGE_KEYS.qualityLastScanAt, String(timestamp));
  renderQualityScanMeta();
}

function extractTitleFromLog(log) {
  const combined = [log.Name, log.ShortOverview, log.Overview].filter(Boolean).join(" ");
  const inBrackets = combined.match(/《([^》]+)》/);
  if (inBrackets?.[1]) {
    return inBrackets[1].trim();
  }

  const inQuotes = combined.match(/"([^"]+)"/);
  if (inQuotes?.[1]) {
    return inQuotes[1].trim();
  }

  return null;
}

function extractMediaTitleCandidate(log) {
  const direct =
    log.ItemName ||
    log.MediaName ||
    log.Item?.Name ||
    extractTitleFromLog(log);
  if (direct) {
    return direct.trim();
  }

  const text = [log.Name, log.ShortOverview, log.Overview].filter(Boolean).join(" ").trim();
  if (!text) {
    return "";
  }

  const playedMatch = text.match(/(?:开始播放|已停止播放|停止播放|播放)\s+(.+)$/);
  if (playedMatch?.[1]) {
    return playedMatch[1].trim();
  }

  return text;
}

function normalizeMediaLookupKey(title) {
  return (title || "")
    .toLowerCase()
    .replace(/《|》|“|”|"|'/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function formatDurationLabelFromSeconds(seconds) {
  const normalizedSeconds = Math.max(0, Math.round(Number(seconds) || 0));
  if (normalizedSeconds <= 0) {
    return "-";
  }
  const displayMinutes = Math.floor(normalizedSeconds / 60);
  const displaySeconds = normalizedSeconds % 60;
  return `${displayMinutes}分${String(displaySeconds).padStart(2, "0")}秒`;
}

function minutesFromDurationSeconds(seconds) {
  const normalizedSeconds = Math.max(0, Math.round(Number(seconds) || 0));
  if (normalizedSeconds <= 0) {
    return 0;
  }
  return Math.max(1, Math.round(normalizedSeconds / 60));
}

function inferPlaybackAction(text) {
  const content = String(text || "").toLowerCase();
  if (!content) {
    return "other";
  }

  if (/(停止播放|已停止播放|结束播放|播放结束|stopped|ended|finished|session ended)/i.test(content)) {
    return "stop";
  }
  if (/(暂停播放|播放暂停|paused)/i.test(content)) {
    return "pause";
  }
  if (/(开始播放|已开始播放|继续播放|恢复播放|start(?:ed)?|resum(?:e|ed)|playing)/i.test(content)) {
    return "start";
  }
  return "other";
}

function parseDurationSecondsFromText(text) {
  if (!text) {
    return 0;
  }

  const lower = String(text)
    .toLowerCase()
    .replace(/：/g, ":");

  const hmsMatch = lower.match(/\b(\d{1,2}):(\d{2}):(\d{2})\b/);
  if (hmsMatch) {
    const h = Number(hmsMatch[1]);
    const m = Number(hmsMatch[2]);
    const s = Number(hmsMatch[3]);
    return h * 3600 + m * 60 + s;
  }

  const mmssWithHintMatch = lower.match(
    /(?:时长|持续|耗时|duration|elapsed|played|watch(?:ed|ing)?|播放|观看)[^0-9]{0,12}(\d{1,2}):(\d{2})/
  );
  if (mmssWithHintMatch) {
    const m = Number(mmssWithHintMatch[1]);
    const s = Number(mmssWithHintMatch[2]);
    return m * 60 + s;
  }

  const hourMatch = lower.match(/(\d+)\s*(?:小时|hour|hours|hr|hrs)/);
  const minuteMatch = lower.match(/(\d+)\s*(?:分钟|分|min(?:ute)?s?)/);
  const secondMatch = lower.match(/(\d+)\s*(?:秒|sec(?:ond)?s?)/);

  const h = hourMatch?.[1] ? Number(hourMatch[1]) : 0;
  const m = minuteMatch?.[1] ? Number(minuteMatch[1]) : 0;
  const s = secondMatch?.[1] ? Number(secondMatch[1]) : 0;
  if (h > 0 || m > 0 || s > 0) {
    return h * 3600 + m * 60 + s;
  }

  const shorthandMatch = lower.match(/(\d+)\s*m\s*(\d+)\s*s/);
  if (shorthandMatch) {
    return Number(shorthandMatch[1]) * 60 + Number(shorthandMatch[2]);
  }

  const minuteOnlyMatch = lower.match(/(\d+)\s*(?:分钟|分|min(?:ute)?s?|m)\b/);
  if (minuteOnlyMatch?.[1]) {
    return Number(minuteOnlyMatch[1]) * 60;
  }

  const secondOnlyMatch = lower.match(/(\d+)\s*(?:秒|sec(?:ond)?s?|s)\b/);
  if (secondOnlyMatch?.[1]) {
    return Number(secondOnlyMatch[1]);
  }

  return 0;
}

function parseDurationSecondsFromTicks(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) {
    return 0;
  }
  return Math.floor(num / 10000000);
}

function parsePlaybackDuration(log, text) {
  const tickCandidates = [
    log.PlaybackPositionTicks,
    log.PositionTicks,
    log.StopPositionTicks,
    log.LastPositionTicks,
    log.PlayState?.PositionTicks,
    log.PlayState?.LastPositionTicks,
    log.Item?.UserData?.PlaybackPositionTicks,
    log.RunTimeTicks,
    log.Item?.RunTimeTicks
  ];

  let seconds = 0;
  for (const candidate of tickCandidates) {
    const parsed = parseDurationSecondsFromTicks(candidate);
    if (parsed > 0) {
      seconds = parsed;
      break;
    }
  }

  if (seconds <= 0) {
    seconds = parseDurationSecondsFromText(text);
  }

  if (seconds <= 0) {
    return {
      seconds: 0,
      minutes: 0,
      label: "-"
    };
  }

  const normalizedSeconds = Math.max(1, Math.round(seconds));

  return {
    seconds: normalizedSeconds,
    minutes: minutesFromDurationSeconds(normalizedSeconds),
    label: formatDurationLabelFromSeconds(normalizedSeconds)
  };
}

function buildPlaybackMatchKey(row) {
  const userKey = String(row.userName || "").trim().toLowerCase();
  if (!userKey) {
    return "";
  }
  const itemKey = row.itemId ? `id:${row.itemId}` : `title:${normalizeMediaLookupKey(row.title)}`;
  return `${userKey}|${itemKey}`;
}

function findStartEventCandidate(stack, endTime) {
  if (!Array.isArray(stack) || stack.length === 0) {
    return null;
  }
  for (let i = stack.length - 1; i >= 0; i -= 1) {
    const entry = stack[i];
    if (entry.used || entry.time > endTime) {
      continue;
    }
    stack.splice(i, 1);
    return entry;
  }
  return null;
}

function patchDurationsByMatchedEvents(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return;
  }

  const ordered = rows
    .map((row) => ({
      row,
      time: new Date(row.date || 0).getTime()
    }))
    .filter((entry) => Number.isFinite(entry.time) && entry.time > 0)
    .sort((a, b) => a.time - b.time);

  const startsByKey = new Map();
  for (const entry of ordered) {
    const key = buildPlaybackMatchKey(entry.row);
    if (!key) {
      continue;
    }

    if (entry.row.playAction === "start") {
      const stack = startsByKey.get(key) || [];
      stack.push({
        row: entry.row,
        time: entry.time,
        used: false
      });
      startsByKey.set(key, stack);
      continue;
    }

    if (entry.row.playAction !== "stop" && entry.row.playAction !== "pause") {
      continue;
    }

    const stack = startsByKey.get(key);
    const startCandidate = findStartEventCandidate(stack, entry.time);
    if (!startCandidate) {
      continue;
    }

    const durationSeconds = Math.round((entry.time - startCandidate.time) / 1000);
    if (durationSeconds <= 0 || durationSeconds > 12 * 60 * 60) {
      continue;
    }

    if ((entry.row.durationSec || 0) <= 0) {
      entry.row.durationSec = durationSeconds;
      entry.row.durationMin = minutesFromDurationSeconds(durationSeconds);
      entry.row.durationText = formatDurationLabelFromSeconds(durationSeconds);
    }
    if ((startCandidate.row.durationSec || 0) <= 0) {
      startCandidate.row.durationSec = durationSeconds;
      startCandidate.row.durationMin = minutesFromDurationSeconds(durationSeconds);
      startCandidate.row.durationText = formatDurationLabelFromSeconds(durationSeconds);
    }
  }
}

function patchDurationsByAdjacentEvents(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return;
  }

  const grouped = new Map();
  rows.forEach((row) => {
    const key = buildPlaybackMatchKey(row);
    const time = new Date(row.date || 0).getTime();
    if (!key || !Number.isFinite(time) || time <= 0) {
      return;
    }
    const list = grouped.get(key) || [];
    list.push({ row, time });
    grouped.set(key, list);
  });

  grouped.forEach((list) => {
    if (list.length < 2) {
      return;
    }
    list.sort((a, b) => a.time - b.time);
    for (let i = 0; i < list.length - 1; i += 1) {
      const current = list[i];
      const next = list[i + 1];
      const deltaSeconds = Math.round((next.time - current.time) / 1000);
      if (deltaSeconds < 5 || deltaSeconds > 4 * 60 * 60) {
        continue;
      }

      if ((current.row.durationSec || 0) <= 0) {
        current.row.durationSec = deltaSeconds;
        current.row.durationMin = minutesFromDurationSeconds(deltaSeconds);
        current.row.durationText = formatDurationLabelFromSeconds(deltaSeconds);
      }
      if ((next.row.durationSec || 0) <= 0) {
        next.row.durationSec = deltaSeconds;
        next.row.durationMin = minutesFromDurationSeconds(deltaSeconds);
        next.row.durationText = formatDurationLabelFromSeconds(deltaSeconds);
      }
    }
  });
}

function patchDurationsByUserTimeline(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return;
  }

  const grouped = new Map();
  rows.forEach((row) => {
    const userKey = String(row.userName || "").trim().toLowerCase();
    const time = new Date(row.date || 0).getTime();
    if (!userKey || !Number.isFinite(time) || time <= 0) {
      return;
    }
    const list = grouped.get(userKey) || [];
    list.push({ row, time });
    grouped.set(userKey, list);
  });

  grouped.forEach((list) => {
    if (list.length < 2) {
      return;
    }
    list.sort((a, b) => a.time - b.time);
    for (let i = 0; i < list.length - 1; i += 1) {
      const current = list[i];
      const next = list[i + 1];
      const deltaSeconds = Math.round((next.time - current.time) / 1000);
      if (deltaSeconds < 5 || deltaSeconds > 4 * 60 * 60) {
        continue;
      }
      if ((current.row.durationSec || 0) <= 0) {
        current.row.durationSec = deltaSeconds;
        current.row.durationMin = minutesFromDurationSeconds(deltaSeconds);
        current.row.durationText = formatDurationLabelFromSeconds(deltaSeconds);
      }
    }
  });
}

function parseUserAndPlayerFromText(log) {
  const text = [log.Name, log.ShortOverview, log.Overview].filter(Boolean).join(" ");

  const patterns = [
    /(?<user>[\w\u4e00-\u9fa5.-]+)\s+在\s+(?<device>[^，。,.\s]+)\s+上(?:开始|停止|继续)?播放/i,
    /(?<device>[^，。,.\s]+)\s+上\s+(?<user>[\w\u4e00-\u9fa5.-]+)\s+(?:已)?(?:开始|停止|继续)?播放/i,
    /用户\s*(?<user>[\w\u4e00-\u9fa5.-]+).{0,12}(?:设备|客户端)\s*(?<device>[\w\u4e00-\u9fa5.-]+)/i
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.groups) {
      return {
        userName: (match.groups.user || "").trim(),
        device: (match.groups.device || "").trim()
      };
    }
  }

  return {
    userName: "",
    device: ""
  };
}

function inferPlayer(log, parsedDevice = "") {
  const device =
    log.DeviceName ||
    log.Device ||
    log.DeviceId ||
    parsedDevice ||
    "";
  const software =
    log.Client ||
    log.ClientName ||
    log.AppName ||
    log.Application ||
    log.ApplicationVersion ||
    "";

  const finalDevice = device || software || "未知播放器";
  const finalSoftware = software || (device ? "Emby" : "未知播放器");

  return {
    device: finalDevice,
    software: finalSoftware
  };
}

function buildEmbyPrimaryPosterUrl(itemId, { maxWidth = 140, quality = 80, imageTag = "" } = {}) {
  if (!itemId || !appState.config.serverUrl || !appState.config.apiKey) {
    return "";
  }
  const safeTag = String(imageTag || "").trim();
  const tagQuery = safeTag ? `&tag=${encodeURIComponent(safeTag)}` : "";
  const width = Number.isFinite(Number(maxWidth)) ? Math.max(40, Math.floor(Number(maxWidth))) : 140;
  const q = Number.isFinite(Number(quality)) ? Math.max(40, Math.min(100, Math.floor(Number(quality)))) : 80;
  return `${appState.config.serverUrl}/Items/${encodeURIComponent(itemId)}/Images/Primary?maxWidth=${width}&quality=${q}&api_key=${encodeURIComponent(
    appState.config.apiKey
  )}${tagQuery}`;
}

function buildEmbyPosterUrl(itemId, imageTag = "") {
  return buildEmbyPrimaryPosterUrl(itemId, { maxWidth: 140, quality: 80, imageTag });
}

function buildEmbyPortraitPosterCandidates(ids = []) {
  return dedupeStringList(
    (ids || [])
      .map((id) => String(id || "").trim())
      .filter(Boolean)
      .map((id) => buildEmbyPosterUrl(id))
      .filter(Boolean)
  );
}

function buildPrimaryIdChainFromNowPlayingItem(item = null) {
  return dedupeStringList(
    [item?.SeriesId, item?.ParentId, item?.Id]
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  );
}

function isLikelySamePlaybackItem(title = "", mediaKey = "", sessionItem = null) {
  if (!sessionItem) {
    return false;
  }
  const key = String(mediaKey || normalizeMediaLookupKey(title)).trim();
  if (!key) {
    return false;
  }
  const sessionNameKey = normalizeMediaLookupKey(sessionItem?.Name || "");
  const sessionSeriesKey = normalizeMediaLookupKey(sessionItem?.SeriesName || sessionItem?.Album || "");
  const sessionCombinedKey = normalizeMediaLookupKey(`${sessionItem?.SeriesName || ""} ${sessionItem?.Name || ""}`);
  const candidates = [sessionNameKey, sessionSeriesKey, sessionCombinedKey].filter(Boolean);
  return candidates.some((value) => value === key || value.includes(key) || key.includes(value));
}

function buildPlaybackStrictSourceIds({ itemId = "", title = "", mediaKey = "", sessionHint = null } = {}) {
  const ids = [];
  const safeItemId = String(itemId || "").trim();
  if (safeItemId) {
    ids.push(safeItemId);
  }
  const sessionItem = sessionHint?.NowPlayingItem || null;
  if (sessionItem) {
    const sessionChain = buildPrimaryIdChainFromNowPlayingItem(sessionItem);
    if (safeItemId) {
      if (sessionChain.includes(safeItemId)) {
        ids.push(...sessionChain);
      }
    } else if (isLikelySamePlaybackItem(title, mediaKey, sessionItem)) {
      ids.push(...sessionChain);
    }
  }
  return dedupeStringList(ids.map((value) => String(value || "").trim()).filter(Boolean));
}

function inferMediaTypeLabel(title) {
  const text = String(title || "").toLowerCase();
  if (/s\d+\s*[,.-]?\s*e?p?\d+/.test(text) || /第\s*\d+\s*集/.test(text)) {
    return "Episode";
  }
  if (/season|第\s*\d+\s*季|s\d+/.test(text)) {
    return "Series";
  }
  return "Media";
}

function buildPlaybackHistoryRows() {
  const playbackKeywords = ["播放", "观看", "playback", "playing", "watched", "stream"];
  const sessionsByUser = new Map();
  appState.sessions.forEach((session) => {
    const sessionUser =
      session.UserName ||
      session.User?.Name ||
      "";
    if (!sessionUser) {
      return;
    }
    const key = sessionUser.toLowerCase();
    const current = sessionsByUser.get(key);
    const currentTime = new Date(current?.LastActivityDate || current?.PlayState?.LastPositionTicks || 0).getTime();
    const nextTime = new Date(session.LastActivityDate || session.PlayState?.LastPositionTicks || 0).getTime();
    if (!current || nextTime >= currentTime) {
      sessionsByUser.set(key, session);
    }
  });

  const rows = appState.logs
    .filter((log) => {
      const text = [log.Name, log.ShortOverview, log.Overview, log.Type].filter(Boolean).join(" ").toLowerCase();
      return playbackKeywords.some((keyword) => text.includes(keyword));
    })
    .map((log) => {
      const relatedUser = appState.users.find((user) => user.Id === log.UserId);
      const parsed = parseUserAndPlayerFromText(log);
      const combined = [log.Name, log.ShortOverview, log.Overview, log.Message, log.Description]
        .filter(Boolean)
        .join(" ");
      const title = extractMediaTitleCandidate(log) || log.Name || "未知媒体";
      const duration = parsePlaybackDuration(log, combined);
      const playAction = inferPlaybackAction(combined);
      const itemId = log.ItemId || log.Item?.Id || null;
      const mediaKey = normalizeMediaLookupKey(title);
      const sessionHint = sessionsByUser.get((log.UserName || relatedUser?.Name || parsed.userName || "").toLowerCase());
      const coverSourceIds = buildPlaybackStrictSourceIds({ itemId, title, mediaKey, sessionHint });
      const rowCoverKeySeed = String(log.Id || `${log.Date || ""}-${title}`).trim();
      const coverCacheKey = coverSourceIds[0] ? `source:${coverSourceIds[0]}` : `none:${rowCoverKeySeed || mediaKey || "unknown"}`;
      const coverCandidates = coverSourceIds[0] ? appState.playbackCoverCandidateCache?.[coverCacheKey] || [] : [];
      const coverUrlFromSources = coverSourceIds.map((id) => appState.playbackItemPosterCache?.[id] || "").find(Boolean) || "";
      const coverUrl = coverCandidates[0] || coverUrlFromSources || "";
      const enrichedLog = sessionHint
        ? {
            ...log,
            DeviceName: log.DeviceName || sessionHint.DeviceName || sessionHint.DeviceId,
            Client: log.Client || sessionHint.Client,
            AppName: log.AppName || sessionHint.ApplicationVersion || sessionHint.Client
          }
        : log;

      return {
        id: log.Id || `${log.Date || ""}-${title}`,
        date: log.Date || log.StartDate || null,
        userName: log.UserName || relatedUser?.Name || log.ByUserName || parsed.userName || "未知用户",
        title,
        mediaType: inferMediaTypeLabel(title),
        mediaKey,
        itemId,
        durationSec: duration.seconds,
        durationMin: duration.minutes,
        durationText: duration.label,
        playAction,
        player: inferPlayer(enrichedLog, parsed.device),
        coverUrl,
        coverCacheKey,
        coverCandidates,
        coverSourceIds
      };
    });

  patchDurationsByMatchedEvents(rows);
  patchDurationsByAdjacentEvents(rows);
  patchDurationsByUserTimeline(rows);
  rows.sort((a, b) => new Date(b.date || 0).getTime() - new Date(a.date || 0).getTime());

  return rows;
}

function escapeQueryValue(value) {
  return encodeURIComponent((value || "").trim());
}

function selectBestCoverItem(items, mediaKey) {
  const key = normalizeMediaLookupKey(mediaKey);
  if (!Array.isArray(items) || items.length === 0) {
    return null;
  }

  const getPosterPriority = (item) => {
    const type = String(item?.Type || "").toLowerCase();
    if (type === "series" || type === "movie") {
      return 0;
    }
    if (type === "season") {
      return 1;
    }
    if (type === "episode") {
      return 2;
    }
    return 3;
  };
  const withImage = items.filter((item) => item?.ImageTags?.Primary || item?.PrimaryImageTag);
  const candidates = (withImage.length > 0 ? withImage : items).slice().sort((a, b) => getPosterPriority(a) - getPosterPriority(b));
  if (!key) {
    return candidates[0] || null;
  }

  const exact = candidates.find((item) => normalizeMediaLookupKey(item?.Name || "") === key);
  if (exact) {
    return exact;
  }

  const includesKey = candidates.find((item) => normalizeMediaLookupKey(item?.Name || "").includes(key));
  if (includesKey) {
    return includesKey;
  }

  const includedByKey = candidates.find((item) => {
    const nameKey = normalizeMediaLookupKey(item?.Name || "");
    return nameKey && key.includes(nameKey);
  });
  if (includedByKey) {
    return includedByKey;
  }

  return candidates[0] || null;
}

async function hydratePlaybackCovers(rows) {
  if (appState.playbackCoverLookupRunning) {
    return;
  }
  const canUseEmby = Boolean(appState?.config?.serverUrl && appState?.config?.apiKey);
  if (!canUseEmby) {
    return;
  }
  const pendingRows = [];
  const seen = new Set();
  (rows || []).forEach((row) => {
    const key = String(row?.coverCacheKey || "").trim();
    if (!key || seen.has(key) || Object.prototype.hasOwnProperty.call(appState.playbackCoverCandidateCache || {}, key)) {
      return;
    }
    seen.add(key);
    pendingRows.push(row);
  });
  if (!pendingRows.length) {
    return;
  }

  appState.playbackCoverLookupRunning = true;
  const stats = {
    rows: pendingRows.length,
    embyHit: 0,
    fallbackHit: 0
  };
  try {
    for (const row of pendingRows) {
      const itemId = String(row?.itemId || "").trim();
      const mediaKey = String(row?.mediaKey || "").trim();
      const sourceIds = dedupeStringList(
        [itemId, ...((row?.coverSourceIds || []).map((value) => String(value || "").trim()).filter(Boolean))]
          .map((value) => String(value || "").trim())
          .filter(Boolean)
      );
      const cacheKey = String(
        row?.coverCacheKey || (sourceIds[0] ? `source:${sourceIds[0]}` : "") || itemId || (mediaKey ? `media:${mediaKey}` : "")
      ).trim();
      if (!cacheKey) {
        continue;
      }
      let resolvedSourceIds = sourceIds;
      let mergedList = [];
      try {
        const detailId = itemId || sourceIds[0] || "";
        if (detailId) {
          const detail = await embyFetch(`/Items/${detailId}?Fields=SeriesId,ParentId,Type,ParentIndexNumber`);
          resolvedSourceIds = dedupeStringList([
            ...buildPrimaryIdChainFromNowPlayingItem(detail),
            ...sourceIds
          ]);
        }
        mergedList = buildEmbyPortraitPosterCandidates(resolvedSourceIds);
      } catch {
        mergedList = buildEmbyPortraitPosterCandidates(resolvedSourceIds);
      }
      appState.playbackCoverCandidateCache[cacheKey] = mergedList;
      const first = mergedList[0] || "";
      if (first) {
        resolvedSourceIds.forEach((id) => {
          appState.playbackItemPosterCache[id] = first;
        });
      }
      if (!first) {
        stats.fallbackHit += 1;
      } else {
        stats.embyHit += 1;
      }
    }
    console.info(
      `[PlaybackCover] rows=${stats.rows}, emby-hit=${stats.embyHit}, fallback-hit=${stats.fallbackHit}`
    );
  } finally {
    appState.playbackCoverLookupRunning = false;
    if (appState.activeView === "logs") {
      renderLogs();
    }
  }
}

function encodePlaybackCoverList(urls) {
  return (urls || []).map((url) => encodeURIComponent(url)).join("|");
}

function bindPlaybackCoverFallbacks() {
  if (!elements.logList) {
    return;
  }
  elements.logList.querySelectorAll("img[data-playback-cover-list]").forEach((node) => {
    if (!(node instanceof HTMLImageElement)) {
      return;
    }
    if (node.dataset.coverBound === "1") {
      return;
    }
    node.dataset.coverBound = "1";
    node.addEventListener("error", () => {
      const list = String(node.dataset.playbackCoverList || "")
        .split("|")
        .map((value) => {
          try {
            return decodeURIComponent(value);
          } catch {
            return "";
          }
        })
        .filter(Boolean);
      const nextIndex = Number(node.dataset.coverIndex || "0") + 1;
      node.dataset.coverIndex = String(nextIndex);
      if (nextIndex < list.length) {
        node.src = list[nextIndex];
        return;
      }
      const wrapper = node.parentElement;
      if (!wrapper) {
        return;
      }
      const fallback = document.createElement("div");
      fallback.className = "playback-cover playback-cover-fallback";
      fallback.textContent = "No";
      wrapper.replaceChildren(fallback);
    });
  });
}

function buildTmdbStatusMeta() {
  const enabled = Boolean(elements.tmdbEnabled?.checked);
  const token = String(elements.tmdbToken?.value || "").trim();
  if (!enabled) {
    return {
      badgeClass: "is-off",
      badgeText: "已关闭",
      hintClass: "is-off",
      hintText: "TMDB 兜底未启用，系统将仅使用 Emby 海报。"
    };
  }
  if (!token) {
    return {
      badgeClass: "is-pending",
      badgeText: "待补全",
      hintClass: "is-warning",
      hintText: "已启用 TMDB 兜底，但尚未填写 Bearer Token。"
    };
  }
  return {
    badgeClass: "is-on",
    badgeText: "已启用",
    hintClass: "is-ok",
    hintText: "当 Emby 海报缺失或加载失败时，会自动尝试 TMDB 海报。"
  };
}

function refreshTmdbUiState() {
  const meta = buildTmdbStatusMeta();
  if (elements.tmdbStatusTip) {
    elements.tmdbStatusTip.classList.remove("is-off", "is-pending", "is-on");
    elements.tmdbStatusTip.classList.add(meta.badgeClass);
    elements.tmdbStatusTip.textContent = meta.badgeText;
  }
  if (elements.tmdbTokenHint) {
    elements.tmdbTokenHint.classList.remove("is-off", "is-warning", "is-ok");
    elements.tmdbTokenHint.classList.add(meta.hintClass);
    elements.tmdbTokenHint.textContent = meta.hintText;
  }
}

function buildPlaybackCoverHtml(row) {
  const candidateList = dedupeStringList([...(row.coverCandidates || []), row.coverUrl || ""]);
  const first = candidateList[0] || "";
  const fallback = `<div class="playback-cover playback-cover-fallback">No</div>`;
  if (!first) {
    return fallback;
  }
  const encoded = encodePlaybackCoverList(candidateList);
  return `<img class="playback-cover" src="${escapeHtml(first)}" alt="${escapeHtml(row.title)}" loading="lazy" data-playback-cover-list="${encoded}" data-cover-index="0">`;
}

function buildRankingFromLogs(logs) {
  const keywords = ["播放", "观看", "playback", "playing", "watched", "stream"];
  const map = new Map();

  logs.forEach((log) => {
    const text = [log.Name, log.ShortOverview, log.Overview].filter(Boolean).join(" ").toLowerCase();
    const matchesPlayback = keywords.some((keyword) => text.includes(keyword));
    if (!matchesPlayback) {
      return;
    }

    const title = extractTitleFromLog(log);
    if (!title) {
      return;
    }

    const current = map.get(title) || { title, playCount: 0, lastPlayed: null };
    current.playCount += 1;

    if (log.Date) {
      const date = new Date(log.Date);
      if (!current.lastPlayed || date > current.lastPlayed) {
        current.lastPlayed = date;
      }
    }

    map.set(title, current);
  });

  return Array.from(map.values())
    .sort((a, b) => b.playCount - a.playCount)
    .slice(0, 50)
    .map((item) => ({
      title: item.title,
      playCount: item.playCount,
      lastPlayed: item.lastPlayed ? item.lastPlayed.toISOString() : null
    }));
}

function buildRankingFromItems(items) {
  const normalized = (items || [])
    .map((item) => {
      const playCount = Number(item.PlayCount ?? item.UserData?.PlayCount ?? 0);
      return {
        title: item.Name || "未命名条目",
        playCount,
        lastPlayed: item.PremiereDate || item.DateLastMediaAdded || item.DateCreated || null
      };
    })
    .filter((item) => item.playCount > 0)
    .sort((a, b) => b.playCount - a.playCount)
    .slice(0, 50);

  return normalized;
}

function normalizeDeviceList(result) {
  if (!result) {
    return [];
  }
  if (Array.isArray(result)) {
    return result;
  }
  if (Array.isArray(result.Items)) {
    return result.Items;
  }
  if (Array.isArray(result.Devices)) {
    return result.Devices;
  }
  return [];
}

function getQualityItemResolution(item) {
  const directWidth = Number(item?.Width || item?.MediaSources?.[0]?.Width || 0);
  const directHeight = Number(item?.Height || item?.MediaSources?.[0]?.Height || 0);
  const stream =
    (item?.MediaStreams || []).find((entry) => String(entry?.Type || "").toLowerCase() === "video") ||
    (item?.MediaSources || [])
      .flatMap((source) => source?.MediaStreams || [])
      .find((entry) => String(entry?.Type || "").toLowerCase() === "video");

  const width = Math.max(directWidth, Number(stream?.Width || 0));
  const height = Math.max(directHeight, Number(stream?.Height || 0));
  const label = width && height ? `${width}x${height}` : "未知分辨率";
  return { width, height, label };
}

function getQualityPosterPayload(item) {
  const itemType = String(item?.Type || "").toLowerCase();
  const itemId = String(item?.Id || "").trim();
  const itemPrimaryTag = String(item?.ImageTags?.Primary || "").trim();
  const seriesId = String(item?.SeriesId || "").trim();
  const parentId = String(item?.ParentId || "").trim();
  const seriesPrimaryTag = String(item?.SeriesPrimaryImageTag || "").trim();
  const parentPrimaryTag = String(item?.ParentPrimaryImageTag || "").trim();
  const candidates = [];
  const pushCandidate = (id, imageTag = "") => {
    const normalizedId = String(id || "").trim();
    if (!normalizedId || candidates.some((candidate) => candidate.id === normalizedId)) {
      return;
    }
    candidates.push({ id: normalizedId, imageTag: String(imageTag || "").trim() });
  };

  if (itemType === "episode") {
    pushCandidate(seriesId, seriesPrimaryTag);
    pushCandidate(parentId, parentPrimaryTag);
    pushCandidate(itemId, itemPrimaryTag);
  } else {
    pushCandidate(itemId, itemPrimaryTag);
    pushCandidate(seriesId, seriesPrimaryTag);
    pushCandidate(parentId, parentPrimaryTag);
  }

  for (const candidate of candidates) {
    const posterUrl = buildEmbyPrimaryPosterUrl(candidate.id, { maxWidth: 220, quality: 90, imageTag: candidate.imageTag });
    if (posterUrl) {
      return { url: posterUrl, itemId: candidate.id, imageTag: candidate.imageTag };
    }
  }

  return { url: "", itemId: "", imageTag: "" };
}

function classifyQualityResolution({ width = 0, height = 0 } = {}) {
  if (width >= 3840 || height >= 2160) {
    return "uhd";
  }
  if (width >= 1920 || height >= 1080) {
    return "fhd";
  }
  if (width >= 1280 || height >= 720) {
    return "hd";
  }
  return "low";
}

function sanitizeQualityTitlePart(value) {
  return String(value || "")
    .replace(/^[\s"'“”‘’《》〈〉「」『』【】\[\]（）()]+|[\s"'“”‘’《》〈〉「」『』【】\[\]（）()]+$/g, "")
    .trim();
}

function padEpisodeNo(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    return "";
  }
  return String(Math.floor(n)).padStart(2, "0");
}

function buildQualityDisplayTitle(item) {
  const type = String(item?.Type || "").toLowerCase();
  const rawName = sanitizeQualityTitlePart(item?.Name || "未命名条目");
  if (type !== "episode") {
    return rawName || "未命名条目";
  }

  const seriesTitle = sanitizeQualityTitlePart(item?.SeriesName || "");
  const seasonNo = padEpisodeNo(item?.ParentIndexNumber);
  const episodeNo = padEpisodeNo(item?.IndexNumber);
  const episodeCode = seasonNo && episodeNo ? `s${seasonNo}e${episodeNo}` : "";
  const episodeTitle = rawName || "未命名单集";
  if (seriesTitle && episodeCode) {
    return `${seriesTitle} - ${episodeCode} - ${episodeTitle}`;
  }
  if (seriesTitle) {
    return `${seriesTitle} - ${episodeTitle}`;
  }
  return episodeTitle;
}

function buildQualityResolutionStats(items = []) {
  const buckets = [
    { key: "uhd", title: "Ultra HD / 4K", subtitle: "2160P 与更高规格", count: 0, className: "resolution-uhd" },
    { key: "fhd", title: "1080P Full HD", subtitle: "主流高清片源", count: 0, className: "resolution-fhd" },
    { key: "hd", title: "720P HD", subtitle: "轻量高清片源", count: 0, className: "resolution-hd" },
    { key: "low", title: "低清 / 需洗版", subtitle: "低于 720P 或缺少分辨率", count: 0, className: "resolution-low" }
  ];
  const bucketMap = new Map(buckets.map((bucket) => [bucket.key, bucket]));
  const itemsByBucket = Object.fromEntries(buckets.map((bucket) => [bucket.key, []]));
  const usableItems = (Array.isArray(items) ? items : []).filter((item) => item && item.Id);

  usableItems.forEach((item) => {
    const resolution = getQualityItemResolution(item);
    const bucketKey = classifyQualityResolution(resolution);
    const bucket = bucketMap.get(bucketKey) || bucketMap.get("low");
    const posterPayload = getQualityPosterPayload(item);
    bucket.count += 1;
    itemsByBucket[bucketKey].push({
      itemId: String(item.Id || ""),
      title: buildQualityDisplayTitle(item),
      type: String(item.Type || "Item"),
      resolution: resolution.label,
      width: resolution.width,
      height: resolution.height,
      path: String(item.Path || item.MediaSources?.[0]?.Path || "路径未知"),
      posterUrl: posterPayload.url,
      imageTag: posterPayload.imageTag
    });
  });

  Object.values(itemsByBucket).forEach((bucketItems) => {
    bucketItems.sort((a, b) => {
      const scoreA = Number(a.width || 0) * Number(a.height || 0);
      const scoreB = Number(b.width || 0) * Number(b.height || 0);
      if (scoreA !== scoreB) {
        return scoreB - scoreA;
      }
      return String(a.title || "").localeCompare(String(b.title || ""), "zh-Hans-CN");
    });
  });

  return {
    total: usableItems.length,
    buckets,
    itemsByBucket
  };
}

function normalizeQualityResolutionFilters(filters) {
  const payload = filters && typeof filters === "object" ? filters : {};
  const type = String(payload.type || "all").trim().toLowerCase();
  const sort = String(payload.sort || "resolution_desc").trim().toLowerCase();
  return {
    type: ["all", "movie", "episode", "series"].includes(type) ? type : "all",
    keyword: String(payload.keyword || "").trim(),
    sort: ["resolution_desc", "resolution_asc", "title_asc", "title_desc", "path_asc"].includes(sort) ? sort : "resolution_desc"
  };
}

function getQualityResolutionArea(entry) {
  return Math.max(0, Number(entry?.width || 0)) * Math.max(0, Number(entry?.height || 0));
}

function sortQualityResolutionEntries(entries = [], sortMode = "resolution_desc") {
  const rows = Array.isArray(entries) ? entries.slice() : [];
  rows.sort((a, b) => {
    if (sortMode === "resolution_asc") {
      const delta = getQualityResolutionArea(a) - getQualityResolutionArea(b);
      if (delta !== 0) {
        return delta;
      }
    } else if (sortMode === "title_asc") {
      return String(a?.title || "").localeCompare(String(b?.title || ""), "zh-Hans-CN");
    } else if (sortMode === "title_desc") {
      return String(b?.title || "").localeCompare(String(a?.title || ""), "zh-Hans-CN");
    } else if (sortMode === "path_asc") {
      return String(a?.path || "").localeCompare(String(b?.path || ""), "zh-Hans-CN");
    } else {
      const delta = getQualityResolutionArea(b) - getQualityResolutionArea(a);
      if (delta !== 0) {
        return delta;
      }
    }
    return String(a?.title || "").localeCompare(String(b?.title || ""), "zh-Hans-CN");
  });
  return rows;
}

function applyQualityResolutionFilters(entries = []) {
  const filters = normalizeQualityResolutionFilters(appState.qualityResolutionFilters);
  appState.qualityResolutionFilters = filters;
  let rows = Array.isArray(entries) ? entries.slice() : [];

  if (filters.type !== "all") {
    rows = rows.filter((entry) => String(entry?.type || "").toLowerCase() === filters.type);
  }

  if (filters.keyword) {
    const keyword = filters.keyword.toLowerCase();
    rows = rows.filter((entry) => {
      const text = [entry?.title, entry?.path, entry?.resolution, entry?.type].map((value) => String(value || "").toLowerCase()).join(" ");
      return text.includes(keyword);
    });
  }

  return sortQualityResolutionEntries(rows, filters.sort);
}

function buildQualityResolutionRiskMetrics(entries, filteredEntries, stats, activeBucket) {
  const total = Number(stats?.total || 0);
  const selectedCount = Array.isArray(entries) ? entries.length : 0;
  const filteredCount = Array.isArray(filteredEntries) ? filteredEntries.length : 0;
  const lowBucketCount = Number(stats?.buckets?.find((bucket) => bucket.key === "low")?.count || 0);
  const missingPosterCount = filteredEntries.filter((entry) => !entry?.posterUrl).length;
  const episodeCount = filteredEntries.filter((entry) => String(entry?.type || "").toLowerCase() === "episode").length;
  const typeMix = filteredCount > 0 ? `${Math.round((episodeCount / filteredCount) * 100)}% 单集` : "无样本";
  const lowRatio = total > 0 ? `${Math.round((lowBucketCount / total) * 100)}%` : "0%";

  return [
    { label: "当前分组", value: `${selectedCount.toLocaleString("zh-CN")} 条`, hint: activeBucket.title },
    { label: "筛选命中", value: `${filteredCount.toLocaleString("zh-CN")} 条`, hint: "实时过滤结果" },
    { label: "低清风险", value: `${lowBucketCount.toLocaleString("zh-CN")} 条`, hint: `占总库 ${lowRatio}` },
    { label: "封面缺失", value: `${missingPosterCount.toLocaleString("zh-CN")} 条`, hint: typeMix }
  ];
}

function exportQualityResolutionCurrentList() {
  const rows = Array.isArray(appState.qualityResolutionFilteredEntries) ? appState.qualityResolutionFilteredEntries : [];
  if (!rows.length) {
    showToast("当前筛选结果为空，暂无可导出数据。", 1200);
    return;
  }
  const escapeCsv = (value) => {
    const raw = String(value ?? "");
    return /[",\n]/.test(raw) ? `"${raw.replace(/"/g, "\"\"")}"` : raw;
  };
  const header = ["title", "resolution", "type", "path", "itemId"];
  const lines = [header.join(",")];
  rows.forEach((entry) => {
    lines.push([
      escapeCsv(entry?.title),
      escapeCsv(entry?.resolution),
      escapeCsv(entry?.type),
      escapeCsv(entry?.path),
      escapeCsv(entry?.itemId)
    ].join(","));
  });
  const csvBody = `\uFEFF${lines.join("\n")}`;
  const blob = new Blob([csvBody], { type: "text/csv;charset=utf-8;" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  const bucket = String(appState.qualityResolutionFocusBucketTitle || "quality").replace(/[^\w\u4e00-\u9fa5-]+/g, "_");
  const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  link.download = `${bucket}-${stamp}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
  showToast("片单已导出为 CSV。", 1000);
}

function bindQualityResolutionFocusControls() {
  elements.qualityResolutionFilterType = document.getElementById("quality-resolution-filter-type");
  elements.qualityResolutionFilterKeyword = document.getElementById("quality-resolution-filter-keyword");
  elements.qualityResolutionFilterSort = document.getElementById("quality-resolution-filter-sort");
  elements.qualityResolutionFilterSummary = document.getElementById("quality-resolution-filter-summary");
  elements.qualityResolutionExportBtn = document.getElementById("quality-resolution-export-btn");

  elements.qualityResolutionFilterType?.addEventListener("change", () => {
    appState.qualityResolutionFilters = normalizeQualityResolutionFilters({
      ...appState.qualityResolutionFilters,
      type: elements.qualityResolutionFilterType?.value || "all"
    });
    renderQualityResolutionDetail();
  });
  elements.qualityResolutionFilterKeyword?.addEventListener("input", () => {
    appState.qualityResolutionFilters = normalizeQualityResolutionFilters({
      ...appState.qualityResolutionFilters,
      keyword: elements.qualityResolutionFilterKeyword?.value || ""
    });
    renderQualityResolutionDetail();
  });
  elements.qualityResolutionFilterSort?.addEventListener("change", () => {
    appState.qualityResolutionFilters = normalizeQualityResolutionFilters({
      ...appState.qualityResolutionFilters,
      sort: elements.qualityResolutionFilterSort?.value || "resolution_desc"
    });
    renderQualityResolutionDetail();
  });
  elements.qualityResolutionExportBtn?.addEventListener("click", exportQualityResolutionCurrentList);
}

function renderQualityResolutionFocusPanel(activeBucket, entries, filteredEntries, stats) {
  if (!elements.qualityResolutionFocusBody) {
    return;
  }
  const filters = normalizeQualityResolutionFilters(appState.qualityResolutionFilters);
  appState.qualityResolutionFilters = filters;
  const metrics = buildQualityResolutionRiskMetrics(entries, filteredEntries, stats, activeBucket);
  const metricRows = metrics
    .map(
      (metric) => `
      <article class="quality-resolution-focus-metric">
        <span>${escapeHtml(metric.label)}</span>
        <strong>${escapeHtml(metric.value)}</strong>
        <small>${escapeHtml(metric.hint)}</small>
      </article>
    `
    )
    .join("");
  const previewRows = filteredEntries
    .slice(0, 4)
    .map(
      (entry) => `
      <div class="quality-resolution-focus-item">
        <span class="quality-resolution-focus-item-name" title="${escapeHtml(entry.title)}">${escapeHtml(entry.title)}</span>
        <span class="quality-resolution-focus-item-res">${escapeHtml(entry.resolution)}</span>
      </div>
    `
    )
    .join("");

  elements.qualityResolutionFocusBody.innerHTML = `
    <div class="quality-resolution-risk-grid">${metricRows}</div>
    <div class="quality-resolution-filter-row">
      <label class="quality-filter-field">
        <span>类型</span>
        <select id="quality-resolution-filter-type">
          <option value="all" ${filters.type === "all" ? "selected" : ""}>全部</option>
          <option value="movie" ${filters.type === "movie" ? "selected" : ""}>Movie</option>
          <option value="episode" ${filters.type === "episode" ? "selected" : ""}>Episode</option>
          <option value="series" ${filters.type === "series" ? "selected" : ""}>Series</option>
        </select>
      </label>
      <label class="quality-filter-field keyword">
        <span>关键词</span>
        <input id="quality-resolution-filter-keyword" type="text" value="${escapeHtml(filters.keyword)}" placeholder="输入片名 / 路径搜索">
      </label>
      <label class="quality-filter-field">
        <span>排序</span>
        <select id="quality-resolution-filter-sort">
          <option value="resolution_desc" ${filters.sort === "resolution_desc" ? "selected" : ""}>分辨率从高到低</option>
          <option value="resolution_asc" ${filters.sort === "resolution_asc" ? "selected" : ""}>分辨率从低到高</option>
          <option value="title_asc" ${filters.sort === "title_asc" ? "selected" : ""}>片名 A-Z</option>
          <option value="title_desc" ${filters.sort === "title_desc" ? "selected" : ""}>片名 Z-A</option>
          <option value="path_asc" ${filters.sort === "path_asc" ? "selected" : ""}>路径顺序</option>
        </select>
      </label>
      <button id="quality-resolution-export-btn" class="quality-mini-action-btn" type="button">导出 CSV</button>
    </div>
    <p id="quality-resolution-filter-summary" class="quality-resolution-filter-summary">当前命中 ${filteredEntries.length.toLocaleString("zh-CN")} / ${entries.length.toLocaleString("zh-CN")} 条</p>
    <div class="quality-resolution-focus-list">
      ${previewRows || `<div class="quality-resolution-focus-empty">当前筛选条件下没有命中条目</div>`}
    </div>
  `;
  bindQualityResolutionFocusControls();
}

function renderQualityResolutionDetail() {
  if (!elements.qualityResolutionDetailList) {
    return;
  }
  const stats = appState.qualityResolutionStats;
  if (!stats || !Array.isArray(stats.buckets)) {
    elements.qualityResolutionDetailList.innerHTML = `<div class="quality-resolution-skeleton">正在加载分辨率片单...</div>`;
    if (elements.qualityResolutionFocusBody) {
      elements.qualityResolutionFocusBody.innerHTML = `<div class="quality-resolution-focus-empty">正在加载分辨率概览...</div>`;
    }
    return;
  }

  const activeKey = appState.qualityResolutionActiveBucket || stats.buckets[0]?.key || "uhd";
  const activeBucket = stats.buckets.find((bucket) => bucket.key === activeKey) || stats.buckets[0];
  const entries = Array.isArray(stats.itemsByBucket?.[activeBucket.key]) ? stats.itemsByBucket[activeBucket.key] : [];
  const filteredEntries = applyQualityResolutionFilters(entries);
  appState.qualityResolutionFilteredEntries = filteredEntries;
  appState.qualityResolutionFocusBucketKey = activeBucket.key;
  appState.qualityResolutionFocusBucketTitle = activeBucket.title;

  if (elements.qualityResolutionFocusTitle) {
    elements.qualityResolutionFocusTitle.textContent = `${activeBucket.title} 风险摘要 + 过滤控制台`;
  }
  if (elements.qualityResolutionFocusSubtitle) {
    elements.qualityResolutionFocusSubtitle.textContent = `${entries.length.toLocaleString("zh-CN")} 部影片 / 剧集（可筛选）`;
  }
  renderQualityResolutionFocusPanel(activeBucket, entries, filteredEntries, stats);

  if (elements.qualityResolutionDetailTitle) {
    elements.qualityResolutionDetailTitle.textContent = `${activeBucket.title} 影片明细`;
  }
  if (elements.qualityResolutionDetailSubtitle) {
    elements.qualityResolutionDetailSubtitle.textContent = `${filteredEntries.length.toLocaleString("zh-CN")} / ${entries.length.toLocaleString("zh-CN")} 部影片 / 剧集`;
  }

  if (!entries.length) {
    elements.qualityResolutionDetailList.innerHTML = `<div class="quality-resolution-empty">该分辨率分类暂时没有可显示的影片</div>`;
    return;
  }
  if (!filteredEntries.length) {
    elements.qualityResolutionDetailList.innerHTML = `<div class="quality-resolution-empty">当前筛选条件下没有命中结果，请调整关键词或类型。</div>`;
    return;
  }

  elements.qualityResolutionDetailList.innerHTML = filteredEntries
    .slice(0, 120)
    .map((entry, index) => {
      const poster = entry.posterUrl
        ? `<img src="${escapeHtml(entry.posterUrl)}" alt="${escapeHtml(entry.title)}" loading="${index < 3 ? "eager" : "lazy"}" onerror="this.outerHTML='&lt;div class=&quot;quality-resolution-poster-fallback&quot;&gt;No&lt;/div&gt;'">`
        : `<div class="quality-resolution-poster-fallback">No</div>`;
      return `
        <article class="quality-resolution-media-item">
          <div class="quality-resolution-poster">${poster}</div>
          <div class="quality-resolution-media-main">
            <h4 title="${escapeHtml(entry.title)}">${escapeHtml(entry.title)}</h4>
            <div class="quality-resolution-media-meta">
              <span class="quality-resolution-badge">${escapeHtml(entry.resolution)}</span>
              <span class="quality-resolution-type">${escapeHtml(entry.type)}</span>
            </div>
            <p class="quality-resolution-path" title="${escapeHtml(entry.path)}">${escapeHtml(entry.path)}</p>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderQualityResolutionMatrix() {
  if (!elements.qualityResolutionBars) {
    return;
  }

  const stats = appState.qualityResolutionStats;
  if (!stats) {
    elements.qualityResolutionBars.innerHTML = `<div class="quality-resolution-skeleton">正在统计媒体分辨率...</div>`;
    if (elements.qualityResolutionTotal) {
      elements.qualityResolutionTotal.textContent = "等待媒体库扫描";
    }
    renderQualityResolutionDetail();
    return;
  }

  const buckets = Array.isArray(stats.buckets) ? stats.buckets : [];
  const maxCount = Math.max(1, ...buckets.map((bucket) => Number(bucket.count || 0)));
  if (elements.qualityResolutionTotal) {
    elements.qualityResolutionTotal.textContent = stats.total
      ? `已统计 ${stats.total.toLocaleString("zh-CN")} 部电影 / 剧集`
      : "暂未获取到媒体分辨率";
  }

  if (!stats.total) {
    elements.qualityResolutionBars.innerHTML = `<div class="quality-resolution-empty">暂无可统计的媒体分辨率数据</div>`;
    renderQualityResolutionDetail();
    return;
  }

  elements.qualityResolutionBars.innerHTML = buckets
    .map((bucket) => {
      const count = Number(bucket.count || 0);
      const width = Math.max(count > 0 ? 6 : 0, Math.round((count / maxCount) * 100));
      const activeClass = bucket.key === appState.qualityResolutionActiveBucket ? "is-active" : "";
      return `
        <article class="quality-resolution-row ${bucket.className || ""} ${activeClass}" data-resolution-bucket="${bucket.key}">
          <div class="quality-resolution-row-head">
            <div class="quality-resolution-label">
              <strong>${bucket.title}</strong>
              <span>${bucket.subtitle}</span>
            </div>
            <div class="quality-resolution-count">${count.toLocaleString("zh-CN")}<small>部</small></div>
          </div>
          <div class="quality-resolution-track" aria-hidden="true">
            <div class="quality-resolution-fill" style="--bar-width: ${width}%"></div>
          </div>
        </article>
      `;
    })
    .join("");

  elements.qualityResolutionBars.querySelectorAll("[data-resolution-bucket]").forEach((card) => {
    card.addEventListener("click", () => {
      const key = String(card.dataset.resolutionBucket || "").trim();
      if (!key || key === appState.qualityResolutionActiveBucket) {
        return;
      }
      appState.qualityResolutionActiveBucket = key;
      renderQualityResolutionMatrix();
    });
  });
  renderQualityResolutionDetail();
}

function formatDateOnly(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toISOString().slice(0, 10);
}

function daysUntil(dateString) {
  if (!dateString) {
    return null;
  }
  const target = new Date(`${dateString}T00:00:00`);
  const now = new Date();
  const diff = target.getTime() - new Date(now.toDateString()).getTime();
  return Math.ceil(diff / (1000 * 60 * 60 * 24));
}

function initials(name) {
  return (name || "EP").slice(0, 2).toUpperCase();
}

function statusBadgeClass(status) {
  return {
    online: "status-online",
    offline: "status-offline",
    expiring: "status-expiring",
    disabled: "status-disabled"
  }[status] || "status-neutral";
}

function statusLabel(status) {
  return {
    online: "在线",
    offline: "离线",
    expiring: "即将到期",
    disabled: "已禁用"
  }[status] || "未知";
}

function getUserSessions(userId) {
  return appState.sessions.filter((session) => session.UserId === userId);
}

function getUserRenewal(userId) {
  return appState.renewals[userId] || { plan: "", expiry: "", note: "" };
}

function deriveUserStatus(user) {
  if (user.Policy?.IsDisabled) {
    return "disabled";
  }
  const sessions = getUserSessions(user.Id);
  const expiry = getUserRenewal(user.Id).expiry;
  const days = daysUntil(expiry);
  if (days !== null && days <= 7) {
    return "expiring";
  }
  if (sessions.length > 0) {
    return "online";
  }
  return "offline";
}

function isUserCenterVipUser(user, renewal) {
  const plan = String(renewal?.plan || "").toLowerCase();
  const note = String(renewal?.note || "").toLowerCase();
  const connectName = String(user?.ConnectUserName || "").toLowerCase();
  return /vip|高级|尊享|premium/.test(plan) || /vip|高级|尊享|premium/.test(note) || /vip/.test(connectName);
}

function deriveUserCenterStatus(user, renewal) {
  const days = daysUntil(renewal.expiry);
  const isDisabled = Boolean(user.Policy?.IsDisabled);
  const isExpired = days !== null && days < 0;
  const isExpiring = days !== null && days >= 0 && days <= 7;

  if (isDisabled || isExpired) {
    return { key: "blocked", label: "已封禁/过期", className: "status-disabled", rank: 3 };
  }
  if (isExpiring) {
    return { key: "expiring", label: "7天内到期", className: "status-expiring", rank: 2 };
  }
  return { key: "active", label: "活跃正常", className: "status-online", rank: 1 };
}

function buildUserCenterRows() {
  return appState.users.map((user) => {
    const renewal = getUserRenewal(user.Id);
    const status = deriveUserCenterStatus(user, renewal);
    const days = daysUntil(renewal.expiry);
    const streamLimit = Number(user.Policy?.SimultaneousStreamLimit ?? user.Policy?.MaxActiveSessions ?? 0);
    const isVip = isUserCenterVipUser(user, renewal);
    const lastLoginRaw = user.LastLoginDate || user.LastActivityDate || user.DateCreated || null;
    const lastLoginTime = new Date(lastLoginRaw || 0).getTime();

    return {
      id: user.Id,
      name: user.Name || "未命名用户",
      connectName: user.ConnectUserName || "",
      note: renewal.note || "",
      expiry: renewal.expiry || "",
      expiryDays: days,
      expiryText: renewal.expiry ? renewal.expiry : "永久有效",
      concurrencyText: Number.isFinite(streamLimit) && streamLimit > 0 ? `${streamLimit} 并发` : "未配置",
      isAdmin: Boolean(user.Policy?.IsAdministrator),
      isVip,
      status,
      lastLoginRaw,
      lastLoginTime: Number.isFinite(lastLoginTime) ? lastLoginTime : 0
    };
  });
}

function getFilteredUsers() {
  const keyword = appState.userSearch.trim().toLowerCase();
  return appState.users.filter((user) => {
    const renewal = getUserRenewal(user.Id);
    const status = deriveUserStatus(user);
    const matchesKeyword =
      (user.Name || "").toLowerCase().includes(keyword) ||
      (user.ConnectUserName || "").toLowerCase().includes(keyword) ||
      (renewal.note || "").toLowerCase().includes(keyword);
    const matchesFilter = appState.userFilter === "all" || status === appState.userFilter;
    return matchesKeyword && matchesFilter;
  });
}

function getSelectedUser() {
  return appState.users.find((user) => user.Id === appState.selectedUserId) || null;
}

function getUserById(userId) {
  return appState.users.find((user) => user.Id === userId) || null;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeLibraryItems(source) {
  const rows = Array.isArray(source?.Items) ? source.Items : Array.isArray(source) ? source : [];
  const map = new Map();
  rows.forEach((row) => {
    const id = String(row?.ItemId || row?.Id || "").trim();
    const name = String(row?.Name || row?.CollectionType || "").trim();
    if (!id || !name || map.has(id)) {
      return;
    }
    map.set(id, { id, name });
  });
  return Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name, "zh-CN"));
}

async function fetchAllLibraries() {
  const paths = [
    "/Library/VirtualFolders",
    "/Library/MediaFolders",
    "/Items?IncludeItemTypes=CollectionFolder&Recursive=true&Limit=200"
  ];
  for (const path of paths) {
    try {
      const result = await embyFetch(path);
      const normalized = normalizeLibraryItems(result);
      if (normalized.length > 0) {
        return normalized;
      }
    } catch {
      // try next endpoint
    }
  }
  return [];
}

function switchUserConfigTab(tab) {
  elements.ucConfigTabs?.forEach((item) => {
    item.classList.toggle("active", item.dataset.configTab === tab);
  });
  elements.ucConfigPanes?.forEach((item) => {
    item.classList.toggle("active", item.dataset.configPane === tab);
  });
}

function renderUserConfigFolders(selectedIds = []) {
  if (!elements.ucConfigFoldersList) {
    return;
  }
  if (!appState.userConfigLibraries.length) {
    elements.ucConfigFoldersList.innerHTML = `<div class="empty-state">未读取到媒体库，请先确认 Emby 已连接且账号有库读取权限。</div>`;
    return;
  }
  const selected = new Set(selectedIds);
  elements.ucConfigFoldersList.innerHTML = appState.userConfigLibraries
    .map(
      (item) => `
        <label class="user-config-folder-item">
          <input type="checkbox" value="${escapeHtml(item.id)}" data-config-folder-id="${escapeHtml(item.id)}" ${
            selected.has(item.id) ? "checked" : ""
          }>
          <span>${escapeHtml(item.name)}</span>
        </label>
      `
    )
    .join("");
}

function setUserConfigWhitelistDisabled(disabled) {
  document.querySelectorAll("[data-config-folder-id]").forEach((input) => {
    if (input instanceof HTMLInputElement) {
      input.disabled = disabled;
    }
  });
  if (elements.ucConfigResetFolders) {
    elements.ucConfigResetFolders.disabled = disabled;
  }
}

function updateCreateUserFeedback(message, tone = "neutral") {
  if (!elements.ucCreateFeedback) {
    return;
  }
  let normalizedTone = typeof tone === "string" ? tone : tone ? "success" : "neutral";
  if (!["neutral", "success", "error"].includes(normalizedTone)) {
    normalizedTone = "neutral";
  }
  elements.ucCreateFeedback.textContent = message;
  elements.ucCreateFeedback.classList.remove("feedback-success", "feedback-error");
  if (normalizedTone === "success") {
    elements.ucCreateFeedback.classList.add("feedback-success");
  } else if (normalizedTone === "error") {
    elements.ucCreateFeedback.classList.add("feedback-error");
  }
}

function switchCreateUserTab(tab) {
  elements.ucCreateTabs?.forEach((item) => {
    item.classList.toggle("active", item.dataset.createTab === tab);
  });
  elements.ucCreatePanes?.forEach((item) => {
    item.classList.toggle("active", item.dataset.createPane === tab);
  });
}

function renderCreateUserRatings() {
  if (!elements.ucCreateRatingList) {
    return;
  }
  const selected = new Set(appState.createUserDraft.selectedRatings);
  elements.ucCreateRatingList.innerHTML = CREATE_USER_RATING_OPTIONS
    .map(
      (item) => `
        <label class="create-rating-item">
          <input type="checkbox" data-create-rating="${item.value}" ${selected.has(item.value) ? "checked" : ""}>
          <span>${item.label}</span>
        </label>
      `
    )
    .join("");
}

function syncCreateUserRatingsFromChecks() {
  const checked = Array.from(document.querySelectorAll("[data-create-rating]:checked"))
    .map((item) => String(item.getAttribute("data-create-rating") || ""))
    .filter(Boolean);
  const normalized = checked.length > 0 ? checked : ["0"];
  appState.createUserDraft.selectedRatings = normalized;
  const maxValue = String(
    normalized.reduce((max, value) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? Math.max(max, parsed) : max;
    }, 0)
  );
  appState.createUserDraft.maxParentalRating = maxValue;
  if (elements.ucCreateMaxRating) {
    elements.ucCreateMaxRating.value = maxValue;
  }
}

function syncCreateUserChecksFromMaxRating() {
  const maxValue = String(elements.ucCreateMaxRating?.value || "0");
  appState.createUserDraft.maxParentalRating = maxValue;
  appState.createUserDraft.selectedRatings = [maxValue];
  renderCreateUserRatings();
}

function renderCreateUserFolders(selectedIds = []) {
  if (!elements.ucCreateFoldersList) {
    return;
  }
  if (!appState.userConfigLibraries.length) {
    elements.ucCreateFoldersList.innerHTML = `<div class="empty-state">未读取到媒体库，请先确认 Emby 已连接且账号有库读取权限。</div>`;
    return;
  }
  const selected = new Set(selectedIds.map((item) => String(item)));
  elements.ucCreateFoldersList.innerHTML = appState.userConfigLibraries
    .map(
      (item) => `
        <label class="create-folder-item">
          <input type="checkbox" value="${escapeHtml(item.id)}" data-create-folder-id="${escapeHtml(item.id)}" ${
            selected.has(item.id) ? "checked" : ""
          }>
          <span>${escapeHtml(item.name)}</span>
        </label>
      `
    )
    .join("");
  setCreateUserWhitelistDisabled(Boolean(elements.ucCreateEnableAllFolders?.checked));
}

function setCreateUserWhitelistDisabled(disabled) {
  document.querySelectorAll("[data-create-folder-id]").forEach((input) => {
    if (input instanceof HTMLInputElement) {
      input.disabled = disabled;
    }
  });
  if (elements.ucCreateResetFolders) {
    elements.ucCreateResetFolders.disabled = disabled;
  }
}

function populateCreateUserSourceUsers() {
  if (!elements.ucCreateSourceUser) {
    return;
  }
  const selected = elements.ucCreateSourceUser.value;
  const options = appState.users
    .slice()
    .sort((a, b) => String(a.Name || "").localeCompare(String(b.Name || ""), "zh-CN"))
    .map((user) => `<option value="${escapeHtml(user.Id)}">${escapeHtml(user.Name || user.Id)}</option>`);
  elements.ucCreateSourceUser.innerHTML = `<option value="">请选择用户</option>${options.join("")}`;
  elements.ucCreateSourceUser.value = selected && appState.users.some((user) => user.Id === selected) ? selected : "";
}

function applyCreateTemplate(templateKey) {
  const key = String(templateKey || "default");
  const next = {
    enableAllFolders: true,
    selectedFolders: [],
    allowRemoteAccess: true,
    allowLiveTvAccess: false,
    allowDeleteMedia: false,
    allowManageCollections: false,
    allowDownload: false,
    allowVideoTranscoding: true,
    allowAudioTranscoding: true,
    selectedRatings: ["18"],
    maxParentalRating: "18",
    streamLimit: 0
  };

  if (key === "home") {
    next.allowLiveTvAccess = true;
    next.allowDownload = true;
    next.selectedRatings = ["13"];
    next.maxParentalRating = "13";
    next.streamLimit = 2;
  } else if (key === "strict") {
    next.allowRemoteAccess = false;
    next.allowLiveTvAccess = false;
    next.allowDownload = false;
    next.allowVideoTranscoding = false;
    next.allowAudioTranscoding = false;
    next.selectedRatings = ["9"];
    next.maxParentalRating = "9";
    next.streamLimit = 1;
  }

  appState.createUserDraft = {
    ...appState.createUserDraft,
    ...next,
    template: key
  };
}

function applyCreatePolicyPreset(policy) {
  const source = policy && typeof policy === "object" ? policy : {};
  const enableAllFolders = source.EnableAllFolders !== false;
  const enabledFolders = Array.isArray(source.EnabledFolders) ? source.EnabledFolders.map(String) : [];
  const maxParental = Number(source.MaxParentalRating);
  const maxParentalRating = Number.isFinite(maxParental) && maxParental >= 0 ? String(Math.floor(maxParental)) : "18";
  const streamLimit = Number(source.SimultaneousStreamLimit || source.MaxActiveSessions || 0);

  appState.createUserDraft = {
    ...appState.createUserDraft,
    status: source.IsDisabled ? "disabled" : "active",
    enableAllFolders,
    selectedFolders: enableAllFolders ? [] : enabledFolders,
    allowRemoteAccess: Boolean(source.EnableRemoteAccess),
    allowLiveTvAccess: Boolean(source.EnableLiveTvAccess),
    allowDeleteMedia: Boolean(source.EnableContentDeletion),
    allowManageCollections: Boolean(source.EnableCollectionManagement),
    allowDownload: Boolean(source.EnableContentDownloading),
    allowVideoTranscoding: source.EnableVideoPlaybackTranscoding !== false,
    allowAudioTranscoding: source.EnableAudioPlaybackTranscoding !== false,
    selectedRatings: [maxParentalRating],
    maxParentalRating,
    streamLimit: Number.isFinite(streamLimit) && streamLimit > 0 ? Math.floor(streamLimit) : 0
  };
}

function applyCreateUserDraftToForm() {
  const draft = appState.createUserDraft;
  if (elements.ucCreateUsername) {
    elements.ucCreateUsername.value = draft.username || "";
  }
  if (elements.ucCreateNote) {
    elements.ucCreateNote.value = draft.note || "";
  }
  if (elements.ucCreatePassword) {
    elements.ucCreatePassword.value = draft.password || "";
  }
  if (elements.ucCreateConfirmPassword) {
    elements.ucCreateConfirmPassword.value = draft.confirmPassword || "";
  }
  if (elements.ucCreateStatus) {
    elements.ucCreateStatus.value = draft.status || "active";
  }
  if (elements.ucCreateExpiry) {
    elements.ucCreateExpiry.value = draft.expiry || "";
  }
  if (elements.ucCreateTemplate) {
    elements.ucCreateTemplate.value = draft.template || "default";
  }
  if (elements.ucCreateSourceUser) {
    elements.ucCreateSourceUser.value = draft.sourceUserId || "";
  }
  if (elements.ucCreateRemote) {
    elements.ucCreateRemote.checked = Boolean(draft.allowRemoteAccess);
  }
  if (elements.ucCreateLivetv) {
    elements.ucCreateLivetv.checked = Boolean(draft.allowLiveTvAccess);
  }
  if (elements.ucCreateDeleteMedia) {
    elements.ucCreateDeleteMedia.checked = Boolean(draft.allowDeleteMedia);
  }
  if (elements.ucCreateManageCollections) {
    elements.ucCreateManageCollections.checked = Boolean(draft.allowManageCollections);
  }
  if (elements.ucCreateEnableAllFolders) {
    elements.ucCreateEnableAllFolders.checked = Boolean(draft.enableAllFolders);
  }
  if (elements.ucCreateMaxRating) {
    elements.ucCreateMaxRating.value = String(draft.maxParentalRating || "18");
  }
  if (elements.ucCreateStreamLimit) {
    elements.ucCreateStreamLimit.value = String(Number(draft.streamLimit || 0));
  }
  if (elements.ucCreateDownload) {
    elements.ucCreateDownload.checked = Boolean(draft.allowDownload);
  }
  if (elements.ucCreateVideoTranscode) {
    elements.ucCreateVideoTranscode.checked = Boolean(draft.allowVideoTranscoding);
  }
  if (elements.ucCreateAudioTranscode) {
    elements.ucCreateAudioTranscode.checked = Boolean(draft.allowAudioTranscoding);
  }
  renderCreateUserRatings();
  renderCreateUserFolders(draft.selectedFolders || []);
}

async function resetCreateUserModalDraft() {
  appState.createUserDraft = buildDefaultCreateUserDraft();
  if (!appState.userConfigLibraries.length) {
    appState.userConfigLibraries = await fetchAllLibraries();
  }
  populateCreateUserSourceUsers();
  applyCreateUserDraftToForm();
  updateCreateUserFeedback("请填写创建用户参数。");
  switchCreateUserTab("basic");
}

async function openCreateUserModal() {
  if (!elements.ucCreateUserModal) {
    return;
  }
  if (appState.toastTimer) {
    clearTimeout(appState.toastTimer);
    appState.toastTimer = null;
  }
  if (elements.globalToast) {
    elements.globalToast.hidden = true;
    elements.globalToast.textContent = "";
  }
  await resetCreateUserModalDraft();
  closeUserCenterInviteModal();
  closeUserCenterInviteManageModal();
  closeUserCenterInviteResultModal();
  closeUserConfigModal();
  elements.ucCreateUserModal.hidden = false;
}

function closeCreateUserModal() {
  if (!elements.ucCreateUserModal) {
    return;
  }
  elements.ucCreateUserModal.hidden = true;
  appState.createUserDraft = buildDefaultCreateUserDraft();
}

function readCreateUserSelectedFolders() {
  return Array.from(document.querySelectorAll("[data-create-folder-id]:checked"))
    .map((item) => item.getAttribute("data-create-folder-id") || "")
    .filter(Boolean);
}

function validateCreateUserForm() {
  const username = String(elements.ucCreateUsername?.value || "").trim();
  const password = String(elements.ucCreatePassword?.value || "");
  const confirmPassword = String(elements.ucCreateConfirmPassword?.value || "");
  if (!username) {
    return "用户名不能为空。";
  }
  const duplicated = appState.users.some((user) => String(user.Name || "").trim().toLowerCase() === username.toLowerCase());
  if (duplicated) {
    return "用户名已存在，请使用其他用户名。";
  }
  if (!password || password.length < 6) {
    return "密码至少 6 位。";
  }
  if (password !== confirmPassword) {
    return "两次输入的密码不一致。";
  }
  return "";
}

function collectCreateUserPolicy() {
  const enableAllFolders = Boolean(elements.ucCreateEnableAllFolders?.checked);
  const selectedFolders = enableAllFolders ? [] : readCreateUserSelectedFolders();
  const streamLimitRaw = Number(elements.ucCreateStreamLimit?.value || 0);
  const maxRatingRaw = Number(elements.ucCreateMaxRating?.value || 0);

  return {
    IsDisabled: elements.ucCreateStatus?.value === "disabled",
    EnableAllFolders: enableAllFolders,
    EnabledFolders: selectedFolders,
    EnableRemoteAccess: Boolean(elements.ucCreateRemote?.checked),
    EnableLiveTvAccess: Boolean(elements.ucCreateLivetv?.checked),
    EnableContentDeletion: Boolean(elements.ucCreateDeleteMedia?.checked),
    EnableCollectionManagement: Boolean(elements.ucCreateManageCollections?.checked),
    EnableContentDownloading: Boolean(elements.ucCreateDownload?.checked),
    EnableVideoPlaybackTranscoding: Boolean(elements.ucCreateVideoTranscode?.checked),
    EnableAudioPlaybackTranscoding: Boolean(elements.ucCreateAudioTranscode?.checked),
    MaxParentalRating: Number.isFinite(maxRatingRaw) && maxRatingRaw >= 0 ? Math.floor(maxRatingRaw) : 18,
    SimultaneousStreamLimit: Number.isFinite(streamLimitRaw) && streamLimitRaw > 0 ? Math.floor(streamLimitRaw) : 0
  };
}

async function embyRawFetch(path, options = {}) {
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    throw new Error("请先填写 Emby 地址和 API Key。");
  }
  const useProxy = shouldUseLocalProxy();
  const optionHeaders = options.headers || {};
  const mergedHeaders = {
    ...getHeaders(),
    ...optionHeaders
  };
  if (!Object.prototype.hasOwnProperty.call(optionHeaders, "Content-Type")) {
    delete mergedHeaders["Content-Type"];
  }

  const targetUrl = useProxy ? `/api/emby${path}` : `${appState.config.serverUrl}${path}`;
  if (useProxy) {
    mergedHeaders["X-Emby-Base-Url"] = appState.config.serverUrl;
    mergedHeaders["X-Emby-Api-Key"] = appState.config.apiKey;
  } else {
    mergedHeaders["X-Emby-Token"] = appState.config.apiKey;
  }

  const requestOptions = {
    ...options,
    headers: mergedHeaders
  };

  try {
    return await fetch(targetUrl, requestOptions);
  } catch (error) {
    if (!useProxy) {
      throw error;
    }
    const directPath = appendApiKeyToPath(path, appState.config.apiKey);
    const directHeaders = { ...(options.headers || {}) };
    return fetch(`${appState.config.serverUrl}${directPath}`, {
      ...options,
      headers: directHeaders
    });
  }
}

async function applyCreateUserPreset() {
  const basicSnapshot = {
    username: String(elements.ucCreateUsername?.value || ""),
    note: String(elements.ucCreateNote?.value || ""),
    password: String(elements.ucCreatePassword?.value || ""),
    confirmPassword: String(elements.ucCreateConfirmPassword?.value || ""),
    expiry: String(elements.ucCreateExpiry?.value || "")
  };
  const selectedUserId = String(elements.ucCreateSourceUser?.value || "");
  const selectedTemplate = String(elements.ucCreateTemplate?.value || "default");
  if (selectedUserId) {
    const sourceUser = getUserById(selectedUserId);
    if (!sourceUser) {
      updateCreateUserFeedback("未找到镜像用户，请刷新列表后重试。");
      return;
    }
    applyCreatePolicyPreset(sourceUser.Policy || {});
    appState.createUserDraft.sourceUserId = selectedUserId;
    appState.createUserDraft.template = selectedTemplate;
    appState.createUserDraft = { ...appState.createUserDraft, ...basicSnapshot };
    applyCreateUserDraftToForm();
    updateCreateUserFeedback(`已加载用户 ${sourceUser.Name || sourceUser.Id} 的策略预设。`, "success");
    return;
  }

  applyCreateTemplate(selectedTemplate);
  appState.createUserDraft = { ...appState.createUserDraft, ...basicSnapshot };
  applyCreateUserDraftToForm();
  updateCreateUserFeedback("已应用默认模板策略。", "success");
}

async function createUserFromModal() {
  const validationMessage = validateCreateUserForm();
  if (validationMessage) {
    updateCreateUserFeedback(validationMessage, "error");
    return;
  }
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    updateCreateUserFeedback("请先完成 Emby 连接配置后再创建用户。", "error");
    return;
  }

  const username = String(elements.ucCreateUsername?.value || "").trim();
  const password = String(elements.ucCreatePassword?.value || "").trim();
  const note = String(elements.ucCreateNote?.value || "").trim();
  const expiry = String(elements.ucCreateExpiry?.value || "").trim();
  const policyPayload = collectCreateUserPolicy();

  if (elements.ucCreateSaveBtn) {
    elements.ucCreateSaveBtn.disabled = true;
  }
  updateCreateUserFeedback("正在创建用户，请稍候...");

  try {
    const created = await embyFetch("/Users/New", {
      method: "POST",
      body: JSON.stringify({ Name: username })
    });
    const createdUserId = String(created?.Id || "");
    if (!createdUserId) {
      throw new Error("创建成功但未返回用户 ID");
    }

    await embyFetch(`/Users/${createdUserId}/Password`, {
      method: "POST",
      body: JSON.stringify({
        Id: createdUserId,
        CurrentPw: "",
        NewPw: password,
        ResetPassword: false
      })
    });

    await embyFetch(`/Users/${createdUserId}/Policy`, {
      method: "POST",
      body: JSON.stringify(policyPayload)
    });

    appState.renewals[createdUserId] = {
      ...getUserRenewal(createdUserId),
      note,
      expiry
    };
    persistLocalState();
    addSyncEvent("用户创建成功", `用户 ${username} 已创建并写入策略。`, "success");
    updateCreateUserFeedback(`用户 ${username} 创建成功。`, "success");
    await loadEmbyData();
    setTimeout(() => {
      closeCreateUserModal();
    }, 900);
  } catch (error) {
    const message = error.message || "未知错误";
    updateCreateUserFeedback(`创建失败：${message}`, "error");
    addSyncEvent("创建用户失败", message, "danger");
  } finally {
    if (elements.ucCreateSaveBtn) {
      elements.ucCreateSaveBtn.disabled = false;
    }
  }
}

async function openUserConfigModal(userId) {
  const user = getUserById(userId);
  if (!user || !elements.ucUserConfigModal) {
    return;
  }

  appState.userConfigEditingId = user.Id;
  const renewal = getUserRenewal(user.Id);
  const policy = { ...(user.Policy || {}) };
  const enabledFolders = Array.isArray(policy.EnabledFolders) ? policy.EnabledFolders.map(String) : [];
  const enableAllFolders = policy.EnableAllFolders !== false;

  if (elements.ucConfigUsername) {
    elements.ucConfigUsername.value = user.Name || "";
  }
  if (elements.ucConfigNote) {
    elements.ucConfigNote.value = renewal.note || "";
  }
  if (elements.ucConfigPassword) {
    elements.ucConfigPassword.value = "";
  }
  if (elements.ucConfigStatus) {
    elements.ucConfigStatus.value = policy.IsDisabled ? "disabled" : "active";
  }
  if (elements.ucConfigExpiry) {
    elements.ucConfigExpiry.value = renewal.expiry || "";
  }
  if (elements.ucConfigEnableAllFolders) {
    elements.ucConfigEnableAllFolders.checked = enableAllFolders;
  }
  if (elements.ucConfigStreamLimit) {
    elements.ucConfigStreamLimit.value = String(Number(policy.SimultaneousStreamLimit || 0));
  }
  if (elements.ucConfigAdmin) {
    elements.ucConfigAdmin.value = policy.IsAdministrator ? "true" : "false";
  }
  if (elements.ucConfigRemote) {
    elements.ucConfigRemote.checked = Boolean(policy.EnableRemoteAccess);
  }
  if (elements.ucConfigDownload) {
    elements.ucConfigDownload.checked = Boolean(policy.EnableContentDownloading);
  }
  if (elements.ucConfigLivetv) {
    elements.ucConfigLivetv.checked = Boolean(policy.EnableLiveTvAccess);
  }

  if (!appState.userConfigLibraries.length) {
    appState.userConfigLibraries = await fetchAllLibraries();
  }
  renderUserConfigFolders(enabledFolders);
  setUserConfigWhitelistDisabled(enableAllFolders);
  switchUserConfigTab("basic");
  closeUserCenterInviteModal();
  closeUserCenterInviteManageModal();
  closeUserCenterInviteResultModal();
  closeCreateUserModal();
  elements.ucUserConfigModal.hidden = false;
}

function closeUserConfigModal() {
  if (!elements.ucUserConfigModal) {
    return;
  }
  elements.ucUserConfigModal.hidden = true;
  appState.userConfigEditingId = null;
}

async function saveUserConfig() {
  const userId = appState.userConfigEditingId;
  const user = userId ? getUserById(userId) : null;
  if (!user) {
    showToast("未找到用户，无法保存。", 1200);
    return;
  }

  const basePolicy = { ...(user.Policy || {}) };
  const selectedFolderIds = Array.from(document.querySelectorAll("[data-config-folder-id]:checked"))
    .map((item) => item.getAttribute("data-config-folder-id") || "")
    .filter(Boolean);
  const enableAllFolders = Boolean(elements.ucConfigEnableAllFolders?.checked);
  const streamLimitRaw = Number(elements.ucConfigStreamLimit?.value || 0);
  const nextPolicy = {
    ...basePolicy,
    IsDisabled: elements.ucConfigStatus?.value === "disabled",
    SimultaneousStreamLimit: Number.isFinite(streamLimitRaw) && streamLimitRaw > 0 ? Math.floor(streamLimitRaw) : 0,
    IsAdministrator: elements.ucConfigAdmin?.value === "true",
    EnableRemoteAccess: Boolean(elements.ucConfigRemote?.checked),
    EnableContentDownloading: Boolean(elements.ucConfigDownload?.checked),
    EnableLiveTvAccess: Boolean(elements.ucConfigLivetv?.checked),
    EnableAllFolders: enableAllFolders,
    EnabledFolders: enableAllFolders ? [] : selectedFolderIds
  };

  const newPassword = String(elements.ucConfigPassword?.value || "").trim();
  const note = String(elements.ucConfigNote?.value || "").trim();
  const expiry = String(elements.ucConfigExpiry?.value || "").trim();
  if (newPassword && newPassword.length < 6) {
    showToast("新密码至少 6 位。", 1200);
    return;
  }

  try {
    await embyFetch(`/Users/${user.Id}/Policy`, {
      method: "POST",
      body: JSON.stringify(nextPolicy)
    });

    if (newPassword) {
      await embyFetch(`/Users/${user.Id}/Password`, {
        method: "POST",
        body: JSON.stringify({
          Id: user.Id,
          CurrentPw: "",
          NewPw: newPassword,
          ResetPassword: false
        })
      });
    }

    user.Policy = nextPolicy;
    appState.renewals[user.Id] = {
      ...getUserRenewal(user.Id),
      note,
      expiry
    };
    persistLocalState();
    addSyncEvent("用户配置已保存", `${user.Name} 的用户配置已更新。`, "success");
    closeUserConfigModal();
    renderAll();
  } catch (error) {
    addSyncEvent("用户配置保存失败", error.message || "未知错误", "danger");
    showToast("保存失败，请检查连接或权限。", 1200);
  }
}

async function deleteUserDirectly(userId) {
  const user = getUserById(userId);
  if (!user) {
    return;
  }
  const ok = window.confirm(`确认删除用户 ${user.Name || user.Id}？删除后不可恢复。`);
  if (!ok) {
    return;
  }

  try {
    await embyFetch(`/Users/${user.Id}`, { method: "DELETE" });
    appState.users = appState.users.filter((item) => item.Id !== user.Id);
    delete appState.renewals[user.Id];
    if (appState.selectedUserId === user.Id) {
      appState.selectedUserId = appState.users[0]?.Id || null;
    }
    persistLocalState();
    closeUserConfigModal();
    renderAll();
    addSyncEvent("用户已删除", `${user.Name} 已从 Emby 删除。`, "success");
    showToast("用户删除成功", 1000);
  } catch (error) {
    addSyncEvent("删除用户失败", error.message || "未知错误", "danger");
    showToast("删除失败，请检查账号权限。", 1200);
  }
}

function renderUserCenter() {
  if (!elements.userCenterBody) {
    return;
  }

  const allRows = buildUserCenterRows();
  const total = allRows.length;
  const vipCount = allRows.filter((row) => row.isVip).length;
  const activeCount = allRows.filter((row) => row.status.key === "active").length;
  const expiringCount = allRows.filter((row) => row.status.key === "expiring").length;
  const blockedCount = allRows.filter((row) => row.status.key === "blocked").length;

  if (elements.ucStatTotal) {
    elements.ucStatTotal.textContent = String(total);
  }
  if (elements.ucStatVip) {
    elements.ucStatVip.textContent = String(vipCount);
  }
  if (elements.ucStatActive) {
    elements.ucStatActive.textContent = String(activeCount);
  }
  if (elements.ucStatExpiring) {
    elements.ucStatExpiring.textContent = String(expiringCount);
  }
  if (elements.ucStatBlocked) {
    elements.ucStatBlocked.textContent = String(blockedCount);
  }

  if (elements.userCenterSort) {
    elements.userCenterSort.value = appState.userCenterSort;
  }

  const keyword = appState.userCenterSearch.trim().toLowerCase();
  let rows = allRows.filter((row) => {
    if (!keyword) {
      return true;
    }
    return (
      row.name.toLowerCase().includes(keyword) ||
      row.connectName.toLowerCase().includes(keyword) ||
      row.note.toLowerCase().includes(keyword) ||
      row.id.toLowerCase().includes(keyword)
    );
  });

  switch (appState.userCenterSort) {
    case "last-login":
      rows = rows.sort((a, b) => b.lastLoginTime - a.lastLoginTime);
      break;
    case "name":
      rows = rows.sort((a, b) => a.name.localeCompare(b.name, "zh-CN"));
      break;
    case "status":
      rows = rows.sort((a, b) => a.status.rank - b.status.rank || b.lastLoginTime - a.lastLoginTime);
      break;
    case "recommend":
    default:
      rows = rows.sort(
        (a, b) =>
          Number(b.isAdmin) - Number(a.isAdmin) ||
          Number(b.isVip) - Number(a.isVip) ||
          a.status.rank - b.status.rank ||
          b.lastLoginTime - a.lastLoginTime
      );
      break;
  }

  if (rows.length === 0) {
    elements.userCenterBody.innerHTML = `<tr><td colspan="6">暂无匹配的用户记录，请调整搜索关键词。</td></tr>`;
    return;
  }

  elements.userCenterBody.innerHTML = rows
    .map((row) => {
      const roleTagClass = row.isAdmin ? "admin" : "user";
      const roleTagText = row.isAdmin ? "ADMIN" : "user";

      return `
        <tr>
          <td>
            <div class="user-cell">
              <div class="user-avatar">${initials(row.name)}</div>
              <div class="user-meta">
                <span class="user-center-tag ${roleTagClass}">${roleTagText}</span>
                <strong>${row.name}</strong>
              </div>
            </div>
          </td>
          <td>${row.concurrencyText}</td>
          <td><span class="status-badge ${row.status.className}">${row.status.label}</span></td>
          <td>${row.expiryText}</td>
          <td>${formatDateOnly(row.lastLoginRaw)}</td>
          <td>
            <div class="user-center-op-group">
              <button class="text-btn" type="button" data-config-user-id="${row.id}">配置</button>
              <button class="text-btn user-center-delete-btn" type="button" data-delete-user-id="${row.id}">删除</button>
            </div>
          </td>
        </tr>
      `;
    })
    .join("");

  document.querySelectorAll("[data-config-user-id]").forEach((button) => {
    button.addEventListener("click", async () => {
      const userId = button.getAttribute("data-config-user-id") || "";
      await openUserConfigModal(userId);
    });
  });

  document.querySelectorAll("[data-delete-user-id]").forEach((button) => {
    button.addEventListener("click", async () => {
      const userId = button.getAttribute("data-delete-user-id") || "";
      await deleteUserDirectly(userId);
    });
  });
}

function renderConnectionState(connected, message, tone = "neutral") {
  if (elements.connectionBadge) {
    elements.connectionBadge.textContent = connected ? "已连接" : "未连接";
    elements.connectionBadge.className = `status-badge ${connected ? "status-online" : "neutral"}`;
    if (tone === "danger") {
      elements.connectionBadge.className = "status-badge status-disabled";
    }
  }
  if (elements.connectionMessage) {
    elements.connectionMessage.textContent = message;
  }
}

function renderStats() {
  const media = appState.mediaCounts || {};
  const movieCount = Number(media.MovieCount ?? media.Movies ?? 0);
  const seriesCount = Number(media.SeriesCount ?? media.Series ?? 0);
  const episodeCount = Number(media.EpisodeCount ?? media.Episodes ?? 0);

  if (elements.statMovies) {
    elements.statMovies.textContent = appState.mediaCounts ? movieCount.toLocaleString("zh-CN") : "-";
  }
  if (elements.statSeries) {
    elements.statSeries.textContent = appState.mediaCounts ? seriesCount.toLocaleString("zh-CN") : "-";
  }
  if (elements.statEpisodes) {
    elements.statEpisodes.textContent = appState.mediaCounts ? episodeCount.toLocaleString("zh-CN") : "-";
  }
  if (elements.statMoviesSub) {
    elements.statMoviesSub.textContent = appState.mediaCounts ? "电影总数" : "等待连接 Emby";
  }
  if (elements.statSeriesSub) {
    elements.statSeriesSub.textContent = appState.mediaCounts ? "电视剧总数" : "等待连接 Emby";
  }
  if (elements.statEpisodesSub) {
    elements.statEpisodesSub.textContent = appState.mediaCounts ? "剧集总数" : "等待连接 Emby";
  }
  if (elements.statUsers) {
    elements.statUsers.textContent = String(appState.users.length);
  }
  if (elements.statUsersSub) {
    elements.statUsersSub.textContent = appState.systemInfo?.ServerName
      ? `${appState.systemInfo.ServerName} 用户`
      : "等待连接 Emby";
  }
}

function renderOverview() {
  const remoteEnabled = appState.users.filter((user) => user.Policy?.EnableRemoteAccess).length;
  const liveTvEnabled = appState.users.filter((user) => user.Policy?.EnableLiveTvAccess).length;
  const activeInvites = appState.invites.filter((invite) => invite.status === "active").length;

  if (elements.overviewRemote) {
    elements.overviewRemote.textContent = String(remoteEnabled);
  }
  if (elements.overviewLivetv) {
    elements.overviewLivetv.textContent = String(liveTvEnabled);
  }
  if (elements.overviewInvites) {
    elements.overviewInvites.textContent = String(activeInvites);
  }

  if (appState.syncEvents.length === 0) {
    elements.syncFeed.innerHTML = `<div class="empty-state">还没有同步记录。连接 Emby 后这里会显示最近的数据拉取和管理动作。</div>`;
    return;
  }

  elements.syncFeed.innerHTML = appState.syncEvents
    .map(
      (event) => `
        <article class="activity-item">
          <time>${event.time}</time>
          <strong>${event.title}</strong>
          <p>${event.description}</p>
        </article>
      `
    )
    .join("");
}

function renderContentRanking() {
  if (!elements.contentRankingBody || !elements.contentRankingSource) {
    return;
  }

  if (!appState.contentRanking.length) {
    elements.contentRankingBody.innerHTML = `<tr><td colspan="4">暂无排行数据。请先连接 Emby 并完成一次同步。</td></tr>`;
    elements.contentRankingSource.textContent = "暂无数据";
    elements.contentRankingSource.className = "badge neutral";
    return;
  }

  elements.contentRankingBody.innerHTML = appState.contentRanking
    .map(
      (item, index) => `
        <tr>
          <td>#${index + 1}</td>
          <td>${item.title}</td>
          <td>${item.playCount.toLocaleString("zh-CN")}</td>
          <td>${formatDate(item.lastPlayed)}</td>
        </tr>
      `
    )
    .join("");

  if (appState.contentRankingSource === "items") {
    elements.contentRankingSource.textContent = "来源：Emby 播放计数";
    elements.contentRankingSource.className = "badge badge-success";
  } else if (appState.contentRankingSource === "logs") {
    elements.contentRankingSource.textContent = "来源：活动日志聚合";
    elements.contentRankingSource.className = "badge badge-warning";
  } else {
    elements.contentRankingSource.textContent = "来源：未知";
    elements.contentRankingSource.className = "badge neutral";
  }
}

function buildClientDeviceRows() {
  if (appState.devices.length > 0) {
    return appState.devices
      .map((device) => ({
        userName: device.LastUserName || device.UserName || device.UserId || "未知用户",
        deviceName: device.Name || device.DeviceName || device.AppName || "未命名设备",
        deviceId: device.Id || device.DeviceId || "-",
        software: [device.AppName || device.Client, device.AppVersion || device.Version]
          .filter(Boolean)
          .join(" ")
          .trim() || "未知客户端",
        lastActivity: device.LastActivityDate || device.DateLastActivity || device.LastLoginDate || null
      }))
      .sort((a, b) => new Date(b.lastActivity || 0).getTime() - new Date(a.lastActivity || 0).getTime());
  }

  return appState.sessions
    .map((session) => ({
      userName: session.UserName || session.UserId || "未知用户",
      deviceName: session.DeviceName || session.Client || "未命名设备",
      deviceId: session.DeviceId || session.Id || "-",
      software: [session.Client, session.ApplicationVersion].filter(Boolean).join(" ").trim() || "未知客户端",
      lastActivity: session.LastActivityDate || session.StartDate || null
    }))
    .sort((a, b) => new Date(b.lastActivity || 0).getTime() - new Date(a.lastActivity || 0).getTime());
}

const CLIENT_ECOSYSTEM_COLORS = [
  "#4f46e5",
  "#2563eb",
  "#14b8a6",
  "#ec4899",
  "#f59e0b",
  "#8b5cf6",
  "#22c55e",
  "#ef4444",
  "#0ea5e9",
  "#6366f1",
  "#10b981",
  "#a855f7",
  "#64748b"
];

function normalizeClientSoftwareName(software) {
  const raw = String(software || "").trim();
  if (!raw) {
    return "未知客户端";
  }
  const withoutVersion = raw.replace(/\s+((v|ver)\s*)?\d+([._-]\d+){0,4}[a-z0-9._-]*$/i, "").trim();
  return withoutVersion || raw;
}

function truncateLabel(value, max = 14) {
  const text = String(value || "").trim();
  if (text.length <= max) {
    return text;
  }
  return `${text.slice(0, max - 1)}…`;
}

function normalizeClientDeviceName(deviceName) {
  return String(deviceName || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function matchesClientDeviceFilter(deviceName, filterName) {
  const device = normalizeClientDeviceName(deviceName);
  const filter = normalizeClientDeviceName(filterName);
  if (!filter) {
    return true;
  }
  if (!device) {
    return false;
  }
  return device === filter || device.includes(filter) || filter.includes(device);
}

function buildClientEcosystemDistribution(rows, limit = 12) {
  const counts = new Map();
  rows.forEach((row) => {
    const key = normalizeClientSoftwareName(row.software);
    counts.set(key, (counts.get(key) || 0) + 1);
  });

  const sorted = Array.from(counts.entries())
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count);

  if (sorted.length <= limit) {
    return sorted;
  }

  const head = sorted.slice(0, limit);
  const tailCount = sorted.slice(limit).reduce((sum, item) => sum + item.count, 0);
  return tailCount > 0 ? [...head, { label: "其他", count: tailCount }] : head;
}

function buildClientTopDevices(limit = 10) {
  const playbackRows = buildPlaybackHistoryRows();
  const counts = new Map();

  playbackRows.forEach((row) => {
    const key = String(row?.player?.device || "").trim() || "未知设备";
    counts.set(key, (counts.get(key) || 0) + 1);
  });

  return Array.from(counts.entries())
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, limit);
}

function countBlockedEvents(logs) {
  const pattern = /(拦截|封禁|阻止|blocked|denied|forbidden)/i;
  return logs.reduce((sum, log) => {
    const text = [log?.Name, log?.ShortOverview, log?.Overview, log?.Message, log?.Description, log?.Type]
      .filter(Boolean)
      .join(" ");
    return pattern.test(text) ? sum + 1 : sum;
  }, 0);
}

function renderClientEcosystemChart(items) {
  if (
    !elements.clientEcosystemChart ||
    !elements.clientEcosystemLegend ||
    !elements.clientEcosystemEmpty ||
    !elements.clientEcosystemTooltip
  ) {
    return;
  }

  if (!items.length) {
    elements.clientEcosystemChart.hidden = true;
    elements.clientEcosystemLegend.hidden = true;
    elements.clientEcosystemEmpty.hidden = false;
    elements.clientEcosystemChart.innerHTML = "";
    elements.clientEcosystemLegend.innerHTML = "";
    elements.clientEcosystemTooltip.hidden = true;
    return;
  }

  const total = items.reduce((sum, item) => sum + item.count, 0);
  const radius = 76;
  const center = 100;
  let startAngle = -Math.PI / 2;

  function pointAt(angle) {
    return {
      x: center + radius * Math.cos(angle),
      y: center + radius * Math.sin(angle)
    };
  }

  const arcs = items
    .map((item, index) => {
      const fraction = total > 0 ? item.count / total : 0;
      const delta = fraction * Math.PI * 2;
      const midAngle = startAngle + delta / 2;
      const endAngle = startAngle + delta;
      const start = pointAt(startAngle);
      const end = pointAt(endAngle);
      const largeArc = delta > Math.PI ? 1 : 0;
      const stroke = CLIENT_ECOSYSTEM_COLORS[index % CLIENT_ECOSYSTEM_COLORS.length];
      const percent = total > 0 ? Math.round((item.count / total) * 100) : 0;
      if (delta >= Math.PI * 2 - 0.0001) {
        return `
          <circle
            class="client-donut-segment"
            data-index="${index}"
            data-label="${escapeHtml(item.label)}"
            data-count="${item.count}"
            data-percent="${percent}"
            data-mid-angle="${-Math.PI / 2}"
            cx="${center}"
            cy="${center}"
            r="${radius}"
            fill="none"
            stroke="${stroke}"
            style="stroke:${stroke};"
            stroke-width="24"
          ></circle>
        `;
      }
      const segment = `
        <path
          class="client-donut-segment"
          data-index="${index}"
          data-label="${escapeHtml(item.label)}"
          data-count="${item.count}"
          data-percent="${percent}"
          data-mid-angle="${midAngle}"
          d="M ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArc} 1 ${end.x} ${end.y}"
          fill="none"
          stroke="${stroke}"
          style="stroke:${stroke};"
          stroke-width="24"
        ></path>
      `;
      startAngle = endAngle;
      return segment;
    })
    .join("");

  elements.clientEcosystemChart.innerHTML = `
    <svg viewBox="0 0 200 200" aria-hidden="true">
      <circle class="client-donut-ring" cx="100" cy="100" r="${radius}" fill="none" stroke="#e7ecf5" stroke-width="24"></circle>
      ${arcs}
      <text x="100" y="98" text-anchor="middle" class="client-donut-total">${total}</text>
      <text x="100" y="120" text-anchor="middle" class="client-donut-total-label">设备总数</text>
    </svg>
  `;
  elements.clientEcosystemChart.appendChild(elements.clientEcosystemTooltip);

  elements.clientEcosystemLegend.innerHTML = items
    .map((item, index) => {
      const color = CLIENT_ECOSYSTEM_COLORS[index % CLIENT_ECOSYSTEM_COLORS.length];
      const percent = total > 0 ? Math.round((item.count / total) * 100) : 0;
      return `
        <li>
          <span class="client-donut-dot" style="--dot-color:${color};"></span>
          <span class="client-donut-label" title="${escapeHtml(item.label)}">${escapeHtml(truncateLabel(item.label, 18))}</span>
          <strong>${item.count}</strong>
          <em>${percent}%</em>
        </li>
      `;
    })
    .join("");

  elements.clientEcosystemChart.hidden = false;
  elements.clientEcosystemLegend.hidden = false;
  elements.clientEcosystemEmpty.hidden = true;

  const chartHost = elements.clientEcosystemChart;
  const segments = chartHost.querySelectorAll(".client-donut-segment");
  const tooltip = elements.clientEcosystemTooltip;
  const activeFilter = String(appState.clientSoftwareFilter || "").trim();

  function applySelectedState() {
    segments.forEach((seg) => {
      const label = String(seg.getAttribute("data-label") || "").trim();
      seg.classList.toggle("is-selected", Boolean(activeFilter) && label === activeFilter);
    });
  }

  function hideTooltip() {
    tooltip.hidden = true;
    chartHost.classList.remove("is-hovering");
    segments.forEach((seg) => seg.classList.remove("is-active"));
    applySelectedState();
  }

  function showTooltip(segment, index, count, percent) {
    const label = items[index]?.label || "未知设备";
    tooltip.innerHTML = `
      <strong>${escapeHtml(label)}</strong>
      <span>${count} 台 · ${percent}%</span>
    `;
    tooltip.hidden = false;
    const bounds = chartHost.getBoundingClientRect();
    const tipRect = tooltip.getBoundingClientRect();
    const padding = 8;
    const midAngle = Number(segment.getAttribute("data-mid-angle") || 0);
    const anchorRadius = radius + 20;
    const anchorX = center + anchorRadius * Math.cos(midAngle);
    const anchorY = center + anchorRadius * Math.sin(midAngle);
    const rawLeft = anchorX;
    const rawTop = anchorY;
    const maxLeft = Math.max(padding, bounds.width - tipRect.width - padding);
    const left = Math.min(maxLeft, Math.max(padding, rawLeft));
    const maxTop = Math.max(padding, bounds.height - tipRect.height - padding);
    const top = Math.min(maxTop, Math.max(padding, rawTop));
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  }

  hideTooltip();
  applySelectedState();

  segments.forEach((segment) => {
    segment.onpointerenter = () => {
      const dataIndex = Number(segment.getAttribute("data-index"));
      const count = Number(segment.getAttribute("data-count") || 0);
      const percent = Number(segment.getAttribute("data-percent") || 0);
      segments.forEach((seg) => seg.classList.remove("is-active"));
      segment.classList.add("is-active");
      chartHost.classList.add("is-hovering");
      showTooltip(segment, dataIndex, count, percent);
    };

    segment.onpointermove = () => {
      const dataIndex = Number(segment.getAttribute("data-index"));
      const count = Number(segment.getAttribute("data-count") || 0);
      const percent = Number(segment.getAttribute("data-percent") || 0);
      showTooltip(segment, dataIndex, count, percent);
    };

    segment.onpointerleave = () => {
      hideTooltip();
    };

    segment.onclick = () => {
      const label = String(segment.getAttribute("data-label") || "").trim();
      if (!label) {
        return;
      }
      const nextFilter = appState.clientSoftwareFilter === label ? "" : label;
      appState.clientSoftwareFilter = nextFilter;
      appState.clientDeviceFilter = "";
      appState.pendingClientListScroll = Boolean(nextFilter);
      renderClientControl();
    };
  });

  chartHost.onpointerleave = () => {
    hideTooltip();
  };
}

function renderClientTopDevicesChart(items) {
  if (!elements.clientTopDevicesChart || !elements.clientTopDevicesEmpty) {
    return;
  }

  if (!items.length) {
    elements.clientTopDevicesChart.hidden = true;
    elements.clientTopDevicesEmpty.hidden = false;
    elements.clientTopDevicesChart.innerHTML = "";
    return;
  }

  const maxCount = Math.max(...items.map((item) => item.count), 1);
  const stepCount = 5;
  const axisMax = Math.max(50, Math.ceil(maxCount / 50) * 50);
  const stepValue = axisMax / stepCount;
  const yTicks = Array.from({ length: stepCount + 1 }, (_, index) =>
    String(Math.round(axisMax - stepValue * index))
  );
  const bars = items
    .map((item, index) => {
      const ratio = Math.max(8, Math.round((item.count / axisMax) * 100));
      const shortLabel = truncateLabel(item.label, 14);
      return `
        <div class="client-bar-col" data-index="${index}" data-label="${escapeHtml(item.label)}" data-count="${item.count}">
          <span class="client-bar-value">${item.count}</span>
          <div class="client-bar-track">
            <span class="client-bar-fill" style="height:${ratio}%"></span>
          </div>
          <span class="client-bar-label" title="${escapeHtml(item.label)}">${escapeHtml(shortLabel)}</span>
        </div>
      `;
    })
    .join("");

  const yAxis = yTicks.map((tick) => `<span>${tick}</span>`).join("");
  elements.clientTopDevicesChart.innerHTML = `
    <div class="client-bar-chart-shell">
      <div class="client-bar-y-axis">${yAxis}</div>
      <div class="client-bar-plot" style="--bar-count:${Math.max(1, items.length)};">
        ${bars}
        <div class="client-bar-tooltip" hidden></div>
      </div>
    </div>
  `;

  const plot = elements.clientTopDevicesChart.querySelector(".client-bar-plot");
  const tooltip = elements.clientTopDevicesChart.querySelector(".client-bar-tooltip");
  const barCols = elements.clientTopDevicesChart.querySelectorAll(".client-bar-col");
  if (!plot || !tooltip || !barCols.length) {
    elements.clientTopDevicesChart.hidden = false;
    elements.clientTopDevicesEmpty.hidden = true;
    return;
  }

  function hideTooltip() {
    tooltip.hidden = true;
    barCols.forEach((col) => col.classList.remove("is-active"));
  }

  function showTooltip(col) {
    const label = String(col.getAttribute("data-label") || "").trim() || "未知设备";
    const count = Number(col.getAttribute("data-count") || 0);
    tooltip.innerHTML = `
      <strong>${escapeHtml(label)}</strong>
      <span>历史播放次数：${count}</span>
    `;
    tooltip.hidden = false;

    const fill = col.querySelector(".client-bar-fill");
    const colRect = col.getBoundingClientRect();
    const fillRect = fill ? fill.getBoundingClientRect() : colRect;
    const plotRect = plot.getBoundingClientRect();
    const tipRect = tooltip.getBoundingClientRect();
    const anchorX = fillRect.left - plotRect.left + fillRect.width / 2;
    const anchorY = fillRect.top - plotRect.top + Math.min(14, fillRect.height * 0.25);
    const padding = 8;
    const left = Math.min(
      Math.max(padding, anchorX - tipRect.width / 2),
      Math.max(padding, plotRect.width - tipRect.width - padding)
    );
    const top = Math.min(
      Math.max(padding, anchorY - tipRect.height - 6),
      Math.max(padding, plotRect.height - tipRect.height - padding)
    );
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  }

  barCols.forEach((col) => {
    col.onpointerenter = () => {
      barCols.forEach((item) => item.classList.remove("is-active"));
      col.classList.add("is-active");
      showTooltip(col);
    };
    col.onpointermove = null;
    col.onpointerleave = () => {
      hideTooltip();
    };
    col.onclick = () => {
      const label = String(col.getAttribute("data-label") || "").trim();
      if (!label) {
        return;
      }
      const nextFilter = appState.clientDeviceFilter === label ? "" : label;
      appState.clientDeviceFilter = nextFilter;
      appState.clientSoftwareFilter = "";
      appState.pendingClientListScroll = Boolean(nextFilter);
      renderClientControl();
    };
  });

  plot.onpointerleave = () => {
    hideTooltip();
  };

  elements.clientTopDevicesChart.hidden = false;
  elements.clientTopDevicesEmpty.hidden = true;
}

function scrollClientListIntoViewIfNeeded() {
  if (!appState.pendingClientListScroll) {
    return;
  }
  appState.pendingClientListScroll = false;

  const listWrap =
    elements.clientDevicesBody?.closest(".user-table-wrap") ||
    document.querySelector("#view-client-control .user-table-wrap");
  if (!listWrap) {
    return;
  }
  listWrap.scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderClientControl() {
  if (!elements.clientDevicesBody || !elements.clientDeviceSource) {
    return;
  }

  const rows = buildClientDeviceRows();
  const activeDeviceFilter = String(appState.clientDeviceFilter || "").trim();
  const activeSoftwareFilter = String(appState.clientSoftwareFilter || "").trim();
  const filteredRows = activeDeviceFilter
    ? rows.filter((row) => matchesClientDeviceFilter(row.deviceName, activeDeviceFilter))
    : activeSoftwareFilter
      ? rows.filter((row) => normalizeClientSoftwareName(row.software) === activeSoftwareFilter)
      : rows;
  const uniqueDevices = new Set(rows.map((row) => row.deviceId).filter(Boolean)).size;
  const onlineDevices = new Set(
    appState.sessions.map((session) => session.DeviceId || session.Id).filter(Boolean)
  ).size;
  const blockedEvents = countBlockedEvents(appState.logs);
  const ecosystem = buildClientEcosystemDistribution(rows, 12);
  const topDevices = buildClientTopDevices(10);

  elements.clientStatTotal.textContent = String(uniqueDevices || rows.length);
  elements.clientStatOnline.textContent = String(onlineDevices);
  if (elements.clientStatBlocked) {
    elements.clientStatBlocked.textContent = String(blockedEvents);
  }
  if (elements.clientSoftwareFilterFeedback) {
    if (activeDeviceFilter) {
      elements.clientSoftwareFilterFeedback.hidden = false;
      elements.clientSoftwareFilterFeedback.textContent = `已按设备筛选：${activeDeviceFilter}（再次点击同柱可取消）`;
    } else if (activeSoftwareFilter) {
      elements.clientSoftwareFilterFeedback.hidden = false;
      elements.clientSoftwareFilterFeedback.textContent = `已按软件筛选：${activeSoftwareFilter}（再次点击同扇区可取消）`;
    } else {
      elements.clientSoftwareFilterFeedback.hidden = true;
      elements.clientSoftwareFilterFeedback.textContent = "";
    }
  }
  renderClientEcosystemChart(ecosystem);
  renderClientTopDevicesChart(topDevices);

  if (appState.devices.length > 0) {
    elements.clientDeviceSource.textContent = "来源：Emby 设备库";
    elements.clientDeviceSource.className = "badge badge-success";
  } else if (appState.sessions.length > 0) {
    elements.clientDeviceSource.textContent = "来源：在线会话回退";
    elements.clientDeviceSource.className = "badge badge-warning";
  } else {
    elements.clientDeviceSource.textContent = "等待连接 Emby";
    elements.clientDeviceSource.className = "badge neutral";
  }

  if (filteredRows.length === 0) {
    if (activeDeviceFilter) {
      elements.clientDevicesBody.innerHTML = `<tr><td colspan="4">当前筛选“${escapeHtml(activeDeviceFilter)}”下暂无设备记录。</td></tr>`;
      scrollClientListIntoViewIfNeeded();
      return;
    }
    if (activeSoftwareFilter) {
      elements.clientDevicesBody.innerHTML = `<tr><td colspan="4">当前筛选“${escapeHtml(activeSoftwareFilter)}”下暂无设备记录。</td></tr>`;
      scrollClientListIntoViewIfNeeded();
      return;
    }
    elements.clientDevicesBody.innerHTML = `<tr><td colspan="4">暂无设备数据。请先连接 Emby 并确保有会话或设备记录。</td></tr>`;
    scrollClientListIntoViewIfNeeded();
    return;
  }

  elements.clientDevicesBody.innerHTML = filteredRows
    .slice(0, 300)
    .map(
      (row) => `
        <tr>
          <td>${row.userName}</td>
          <td>
            <div class="user-meta">
              <strong>${row.deviceName}</strong>
              <span>${row.deviceId}</span>
            </div>
          </td>
          <td>${row.software}</td>
          <td>${formatDate(row.lastActivity)}</td>
        </tr>
      `
    )
    .join("");

  scrollClientListIntoViewIfNeeded();
}

function renderUsers() {
  const users = getFilteredUsers();

  if (!users.some((user) => user.Id === appState.selectedUserId) && users.length > 0) {
    appState.selectedUserId = users[0].Id;
  }

  if (users.length === 0) {
    elements.usersBody.innerHTML = `<tr><td colspan="7">没有匹配的用户，请调整筛选条件。</td></tr>`;
    renderUserDetails(null);
    return;
  }

  elements.usersBody.innerHTML = users
    .map((user) => {
      const status = deriveUserStatus(user);
      const renewal = getUserRenewal(user.Id);
      const isActive = user.Id === appState.selectedUserId ? "active" : "";
      const renewalText = renewal.expiry ? `${renewal.plan || "未命名套餐"} · ${renewal.expiry}` : "未设置";
      return `
        <tr class="user-row ${isActive}" data-user-id="${user.Id}">
          <td>
            <div class="user-cell">
              <div class="user-avatar">${initials(user.Name)}</div>
              <div class="user-meta">
                <strong>${user.Name || "未命名用户"}</strong>
                <span>${user.Id}</span>
              </div>
            </div>
          </td>
          <td><span class="status-badge ${statusBadgeClass(status)}">${statusLabel(status)}</span></td>
          <td>${user.Policy?.IsAdministrator ? "是" : "否"}</td>
          <td>${formatDate(user.LastActivityDate)}</td>
          <td>${user.ConnectUserName || "-"}</td>
          <td>${renewalText}</td>
          <td><button class="text-btn" type="button">查看</button></td>
        </tr>
      `;
    })
    .join("");

  document.querySelectorAll(".user-row").forEach((row) => {
    row.addEventListener("click", () => {
      appState.selectedUserId = row.dataset.userId;
      renderUsers();
      renderUserDetails(getSelectedUser());
    });
  });

  renderUserDetails(getSelectedUser());
}

function renderUserDetails(user) {
  const qualityRiskValue = document.getElementById("quality-risk-value");

  if (!user) {
    elements.detailName.textContent = "未选择用户";
    if (elements.detailStatus) {
      elements.detailStatus.textContent = "";
    }
    elements.detailAvatar.textContent = "EP";
    elements.detailEmail.textContent = "-";
    elements.detailNote.textContent = "请先连接 Emby 并选择一个用户。";
    elements.detailLastLogin.textContent = "-";
    elements.detailLastActivity.textContent = "-";
    elements.detailDeviceCount.textContent = "0";
    elements.detailMeta.innerHTML = "";
    elements.detailActivity.innerHTML = `<div class="empty-state">还没有用户活动。</div>`;
    if (qualityRiskValue) {
      qualityRiskValue.textContent = "待查看";
    }
    return;
  }

  const status = deriveUserStatus(user);
  const renewal = getUserRenewal(user.Id);
  const userSessions = getUserSessions(user.Id);
  const relatedLogs = appState.logs.filter((log) => log.UserId === user.Id).slice(0, 5);

  elements.detailName.textContent = user.Name || "未命名用户";
  if (elements.detailStatus) {
    elements.detailStatus.textContent = "";
  }
  elements.detailAvatar.textContent = initials(user.Name);
  elements.detailEmail.textContent = user.ConnectUserName || "未绑定 Emby Connect";
  elements.detailNote.textContent = renewal.note || "这个用户还没有管理备注。";
  elements.detailLastLogin.textContent = formatDate(user.LastLoginDate);
  elements.detailLastActivity.textContent = formatDate(user.LastActivityDate);
  elements.detailDeviceCount.textContent = String(userSessions.length);
  if (qualityRiskValue) {
    qualityRiskValue.textContent = statusLabel(status);
  }

  elements.detailMeta.innerHTML = [
    ["创建时间", formatDate(user.DateCreated)],
    ["套餐名称", renewal.plan || "未设置"],
    ["到期日期", renewal.expiry || "未设置"],
    ["会话设备", userSessions.map((session) => session.DeviceName || session.Client).join(" / ") || "无在线设备"]
  ]
    .map(([label, value], index) => {
      const icon = ["⏱", "⌘", "◌", "▣"][index] || "•";
      return `
        <article class="quality-detail-item">
          <div class="quality-detail-thumb" aria-hidden="true">${icon}</div>
          <div class="quality-detail-copy">
            <strong>${value}</strong>
            <span>${label}</span>
          </div>
        </article>
      `;
    })
    .join("");

  elements.toggleRemote.checked = Boolean(user.Policy?.EnableRemoteAccess);
  elements.toggleDownload.checked = Boolean(user.Policy?.EnableContentDownloading);
  elements.toggleLivetv.checked = Boolean(user.Policy?.EnableLiveTvAccess);
  elements.toggleDisabled.checked = Boolean(user.Policy?.IsDisabled);

  if (relatedLogs.length === 0) {
    elements.detailActivity.innerHTML = `<div class="empty-state">最近没有查到这个用户相关的活动日志。</div>`;
  } else {
    elements.detailActivity.innerHTML = relatedLogs
      .map(
        (log) => `
          <article class="activity-item">
            <time>${formatDate(log.Date)}</time>
            <strong>${log.Name || log.Type || "活动事件"}</strong>
            <p>${log.ShortOverview || log.Overview || "没有更多描述。"}</p>
          </article>
        `
      )
      .join("");
  }
}

function getInviteUsedUsername(invite) {
  return (
    String(invite.usedUsername || "").trim() ||
    String(invite.usedUserName || "").trim() ||
    String(invite.createdUserName || "").trim()
  );
}

function inferInviteInitialDays(invite) {
  const raw = invite.initialDays;
  if (raw === null || raw === "permanent") {
    return null;
  }
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    return Math.floor(numeric);
  }

  if (!invite.createdAt || !invite.expiresAt) {
    return null;
  }
  const start = new Date(invite.createdAt);
  const end = new Date(`${invite.expiresAt}T00:00:00`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return null;
  }
  const diff = Math.ceil((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));
  return diff > 0 ? diff : null;
}

function formatInviteInitialDuration(invite) {
  const days = inferInviteInitialDays(invite);
  if (days === null) {
    return "永久";
  }
  return `${days} 天`;
}

function formatInviteCreatedAt(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  const month = date.getMonth() + 1;
  const day = date.getDate();
  const hh = String(date.getHours()).padStart(2, "0");
  const mm = String(date.getMinutes()).padStart(2, "0");
  return `${month}/${day} ${hh}:${mm}`;
}

function buildInviteStatusMeta(invite) {
  const usedName = getInviteUsedUsername(invite);
  if (invite.status === "used") {
    return {
      className: "invite-status-used",
      text: usedName ? `已用 · ${usedName}` : "已用"
    };
  }
  return {
    className: "invite-status-idle",
    text: "空闲"
  };
}

function renderInvites() {
  if (!elements.inviteList && !elements.ucInviteManageList) {
    return;
  }

  const reversedInvites = appState.invites.slice().reverse();

  if (appState.invites.length === 0) {
    if (elements.inviteList) {
      elements.inviteList.innerHTML = `<div class="empty-state">还没有邀请记录。你可以先生成一个邀请模板。</div>`;
    }
    if (elements.ucInviteManageList) {
      elements.ucInviteManageList.innerHTML = `<tr><td colspan="5">暂无邀请码记录。</td></tr>`;
    }
    if (elements.ucInviteSelectAll) {
      elements.ucInviteSelectAll.checked = false;
    }
    return;
  }

  if (elements.inviteList) {
    const html = reversedInvites
      .map((invite) => {
        const status = buildInviteStatusMeta(invite);
        return `
          <article class="invite-card">
            <div class="panel-head compact">
              <div>
                <strong>${invite.label || "未命名邀请"}</strong>
                <p>${invite.code}</p>
              </div>
              <span class="badge ${status.className}">${status.text}</span>
            </div>
            <p>默认用户名：${invite.username || "未指定"} | 套餐：${invite.plan || "未设置"} | 初始时长：${formatInviteInitialDuration(invite)} | 到期：${invite.expiresAt || "永久"}</p>
            <div class="card-actions">
              <button class="ghost-btn" type="button" data-copy-code="${invite.code}">复制邀请码</button>
              <button class="primary-btn" type="button" data-create-user="${invite.id}">在 Emby 创建用户</button>
            </div>
          </article>
        `;
      })
      .join("");
    elements.inviteList.innerHTML = html;
  }

  if (elements.ucInviteManageList) {
    elements.ucInviteManageList.innerHTML = reversedInvites
      .map((invite) => {
        const status = buildInviteStatusMeta(invite);
        const link = buildInviteLink(invite.code);
        return `
          <tr class="invite-manage-row">
            <td><input type="checkbox" data-select-invite-id="${invite.id || invite.code}"></td>
            <td>
              <div class="invite-link-cell">
                <span>${invite.code}</span>
                <button class="invite-copy-icon-btn" type="button" data-copy-invite-link="${link}" aria-label="复制链接">📋</button>
              </div>
            </td>
            <td>${formatInviteInitialDuration(invite)}</td>
            <td><span class="${status.className}">${status.text}</span></td>
            <td>${formatInviteCreatedAt(invite.createdAt)}</td>
          </tr>
        `;
      })
      .join("");
  }

  if (elements.ucInviteSelectAll) {
    elements.ucInviteSelectAll.checked = false;
  }

  document.querySelectorAll("[data-copy-code]").forEach((button) => {
    button.addEventListener("click", async () => {
      await navigator.clipboard.writeText(button.dataset.copyCode);
      addSyncEvent("邀请已复制", `邀请码 ${button.dataset.copyCode} 已复制到剪贴板。`, "success");
    });
  });

  document.querySelectorAll("[data-copy-invite-link]").forEach((button) => {
    button.addEventListener("click", async () => {
      await copyToClipboardWithToast(button.dataset.copyInviteLink, "链接已复制");
    });
  });

  document.querySelectorAll("[data-select-invite-id]").forEach((input) => {
    input.addEventListener("change", () => {
      if (!elements.ucInviteSelectAll) {
        return;
      }
      const all = Array.from(document.querySelectorAll("[data-select-invite-id]"))
        .filter((item) => item instanceof HTMLInputElement);
      const checkedCount = all.filter((item) => item.checked).length;
      elements.ucInviteSelectAll.checked = all.length > 0 && checkedCount === all.length;
    });
  });

  document.querySelectorAll("[data-create-user]").forEach((button) => {
    button.addEventListener("click", async () => {
      const invite = appState.invites.find((item) => item.id === button.dataset.createUser);
      if (!invite) {
        return;
      }
      if (!appState.config.serverUrl || !appState.config.apiKey) {
        addSyncEvent("创建失败", "请先连接 Emby 再使用邀请创建用户。", "danger");
        return;
      }
      if (!invite.username) {
        addSyncEvent("创建失败", "该邀请没有默认用户名，请先补充用户名。", "danger");
        return;
      }

      try {
        const created = await embyFetch("/Users/New", {
          method: "POST",
          body: JSON.stringify({ Name: invite.username })
        });

        appState.invites = appState.invites.map((item) =>
          item.id === invite.id
            ? { ...item, status: "used", createdUserId: created.Id, usedUsername: created.Name || invite.username || "" }
            : item
        );
        appState.renewals[created.Id] = {
          plan: invite.plan || "",
          expiry: invite.expiresAt,
          note: `由邀请 ${invite.code} 创建`
        };
        persistLocalState();
        await syncInviteStore({
          silentSuccess: true,
          failureToast: "用户创建成功，但邀请码状态同步失败，请稍后手动重试同步。"
        });
        addSyncEvent("Emby 用户已创建", `已使用邀请 ${invite.code} 创建用户 ${created.Name}。`, "success");
        await loadEmbyData();
      } catch (error) {
        addSyncEvent("创建 Emby 用户失败", error.message, "danger");
      }
    });
  });
}

function renderRenewals() {
  const users = appState.users.slice().sort((a, b) => {
    const aDays = daysUntil(getUserRenewal(a.Id).expiry) ?? 999999;
    const bDays = daysUntil(getUserRenewal(b.Id).expiry) ?? 999999;
    return aDays - bDays;
  });

  if (users.length === 0) {
    elements.renewalList.innerHTML = `<div class="empty-state">连接 Emby 后才能管理用户续费信息。</div>`;
  } else {
    elements.renewalList.innerHTML = users
      .map((user) => {
        const renewal = getUserRenewal(user.Id);
        const days = daysUntil(renewal.expiry);
        const badgeClass =
          days === null ? "neutral" : days <= 0 ? "badge-danger" : days <= 7 ? "badge-warning" : "badge-success";
        const badgeText = days === null ? "未设置" : days <= 0 ? "已到期" : `${days} 天后到期`;
        return `
          <article class="renewal-card">
            <div class="panel-head compact">
              <div>
                <strong>${user.Name || user.Id}</strong>
                <p>${renewal.plan || "未设置套餐"} | ${renewal.expiry || "未设置到期日"}</p>
              </div>
              <span class="badge ${badgeClass}">${badgeText}</span>
            </div>
            <p>${renewal.note || "没有备注。"} </p>
            <div class="card-actions">
              <button class="ghost-btn" type="button" data-fill-renewal="${user.Id}">编辑</button>
              <button class="primary-btn" type="button" data-renew-30="${user.Id}">续期 30 天</button>
            </div>
          </article>
        `;
      })
      .join("");

    document.querySelectorAll("[data-fill-renewal]").forEach((button) => {
      button.addEventListener("click", () => fillRenewalForm(button.dataset.fillRenewal));
    });

    document.querySelectorAll("[data-renew-30]").forEach((button) => {
      button.addEventListener("click", () => quickRenew(button.dataset.renew30, 30));
    });
  }

  const options = appState.users
    .map((user) => `<option value="${user.Id}">${user.Name || user.Id}</option>`)
    .join("");
  elements.renewalUser.innerHTML = options || `<option value="">暂无用户</option>`;
}

function renderLogs() {
  if (!elements.logList) {
    return;
  }

  const allRows = buildPlaybackHistoryRows();
  const keyword = appState.logSearch.trim().toLowerCase();
  const selectedUser = (elements.logUserFilter?.value || "").trim();

  const users = Array.from(new Set(allRows.map((row) => row.userName))).sort((a, b) => a.localeCompare(b, "zh-CN"));
  if (elements.logUserFilter) {
    const current = elements.logUserFilter.value;
    elements.logUserFilter.innerHTML = `
      <option value="">所有用户大盘</option>
      ${users.map((user) => `<option value="${user}">${user}</option>`).join("")}
    `;
    if (users.includes(current)) {
      elements.logUserFilter.value = current;
    }
  }

  const filteredRows = allRows.filter((row) => {
    const matchKeyword =
      !keyword ||
      row.title.toLowerCase().includes(keyword) ||
      row.player.device.toLowerCase().includes(keyword) ||
      row.player.software.toLowerCase().includes(keyword) ||
      row.userName.toLowerCase().includes(keyword);
    const matchUser = !selectedUser || row.userName === selectedUser;
    return matchKeyword && matchUser;
  });

  const today = new Date().toDateString();
  const todayRows = filteredRows.filter((row) => row.date && new Date(row.date).toDateString() === today);
  const todayDuration = todayRows.reduce((sum, row) => sum + row.durationMin, 0);
  const activeUsers = new Set(todayRows.map((row) => row.userName)).size;

  if (elements.playbackTodayCount) {
    elements.playbackTodayCount.textContent = String(todayRows.length);
  }
  if (elements.playbackTodayDuration) {
    elements.playbackTodayDuration.textContent = `${todayDuration} 分钟`;
  }
  if (elements.playbackActiveUsers) {
    elements.playbackActiveUsers.textContent = String(activeUsers);
  }
  if (elements.playbackTotalCount) {
    elements.playbackTotalCount.textContent = String(filteredRows.length);
  }

  if (filteredRows.length === 0) {
    elements.logList.innerHTML = `<tr><td colspan="6">暂无播放历史记录。请先连接 Emby 并产生播放行为。</td></tr>`;
    return;
  }

  elements.logList.innerHTML = filteredRows
    .slice(0, 300)
    .map(
      (row) => `
        <tr>
          <td>
            <div class="user-meta">
              <strong>${formatDate(row.date).split(" ")[0] || "-"}</strong>
              <span>${formatDate(row.date).split(" ")[1] || "-"}</span>
            </div>
          </td>
          <td>
            <div class="playback-user-cell">
              <span class="playback-user-avatar">${initials(row.userName)}</span>
              <strong>${row.userName}</strong>
            </div>
          </td>
          <td>
            <div class="playback-media-cell">
              <div class="playback-media-cell-text">
                <strong>${row.title}</strong>
              </div>
            </div>
          </td>
          <td>${row.durationText || "-"}</td>
          <td>
            <span class="playback-client">${row.player.software}</span>
          </td>
          <td>
            <span class="playback-device">${row.player.device}</span>
          </td>
        </tr>
      `
    )
    .join("");
}

function persistLocalState() {
  saveJson(STORAGE_KEYS.config, appState.config);
  saveJson(STORAGE_KEYS.invites, appState.invites);
  saveJson(STORAGE_KEYS.renewals, appState.renewals);
  saveJson(STORAGE_KEYS.botConfig, appState.botConfig);
}

function getCurrentSiteOriginForBot() {
  const protocol = String(window.location?.protocol || "").toLowerCase();
  const origin = String(window.location?.origin || "").trim();
  if ((protocol === "http:" || protocol === "https:") && origin) {
    return origin.replace(/\/$/, "");
  }
  return "http://127.0.0.1:8080";
}

function getWebhookUrlForBot(token = "vistamirror") {
  const base = getCurrentSiteOriginForBot();
  const safeToken = encodeURIComponent(String(token || "").trim() || "vistamirror");
  return `${base}/api/v1/webhook?token=${safeToken}`;
}

function getWechatCallbackUrlForBot() {
  const base = appState.config.serverUrl
    ? appState.config.serverUrl.replace(/\/emby$/i, "")
    : "http://127.0.0.1:8080";
  return `${base}/api/bot/wecom_webhook`;
}

function showToast(message, duration = 1000) {
  if (!elements.globalToast) {
    return;
  }
  if (elements.ucCreateUserModal && !elements.ucCreateUserModal.hidden) {
    return;
  }
  if (appState.toastTimer) {
    clearTimeout(appState.toastTimer);
  }
  elements.globalToast.textContent = message;
  elements.globalToast.hidden = false;
  appState.toastTimer = setTimeout(() => {
    if (elements.globalToast) {
      elements.globalToast.hidden = true;
    }
    appState.toastTimer = null;
  }, duration);
}

function resetSettingsSaveFeedback() {
  if (appState.settingsSaveTimer) {
    clearTimeout(appState.settingsSaveTimer);
    appState.settingsSaveTimer = null;
  }
  if (elements.settingsSaveBtn) {
    const originalText = elements.settingsSaveBtn.dataset.originalText || "保存配置";
    elements.settingsSaveBtn.textContent = originalText;
    elements.settingsSaveBtn.classList.remove("bot-save-success");
    elements.settingsSaveBtn.disabled = false;
  }
}

async function saveSettingsConfig() {
  if (!elements.settingsSaveBtn || elements.settingsSaveBtn.disabled) {
    return;
  }

  applyConfigFromInputs({ persist: true });
  const hasManaged = (appState?.envControlledFields?.embyConfig || []).length > 0;
  addSyncEvent(
    "系统配置已保存",
    hasManaged ? "普通配置已保存，受环境变量接管的字段未被覆盖。" : "服务器地址与 API Key 已写入本地配置。",
    "success"
  );
  showToast(hasManaged ? "配置已保存（环境变量字段未覆盖）" : "配置已保存", 1200);
  await syncInviteStore({
    silentSuccess: true,
    failureToast: "配置已保存，但服务端同步失败，邀请注册链接可能无法注册。",
    failureEventTitle: "配置同步失败"
  });

  const original = elements.settingsSaveBtn.dataset.originalText || elements.settingsSaveBtn.textContent || "保存配置";
  elements.settingsSaveBtn.dataset.originalText = original;
  elements.settingsSaveBtn.textContent = "保存成功";
  elements.settingsSaveBtn.classList.add("bot-save-success");
  elements.settingsSaveBtn.disabled = true;

  if (appState.settingsSaveTimer) {
    clearTimeout(appState.settingsSaveTimer);
  }
  appState.settingsSaveTimer = setTimeout(() => {
    if (elements.settingsSaveBtn) {
      elements.settingsSaveBtn.textContent = original;
      elements.settingsSaveBtn.classList.remove("bot-save-success");
      elements.settingsSaveBtn.disabled = false;
    }
    appState.settingsSaveTimer = null;
  }, 1000);
}

function readBotConfigFromInputs() {
  return normalizeBotConfig({
    enableCore: Boolean(elements.botEnableCore?.checked),
    enablePlayback: Boolean(elements.botEnablePlayback?.checked),
    enableLibrary: Boolean(elements.botEnableLibrary?.checked),
    telegramToken: elements.botTelegramToken?.value.trim() || "",
    telegramChatId: elements.botTelegramChatId?.value.trim() || "",
    enableCommands: Boolean(elements.botEnableCommands?.checked),
    notifyEvents: {
      start: Boolean(elements.botEventStart?.checked),
      pause: Boolean(elements.botEventPause?.checked),
      resume: Boolean(elements.botEventResume?.checked),
      stop: Boolean(elements.botEventStop?.checked)
    },
    showIp: Boolean(elements.botShowIp?.checked),
    showIpGeo: Boolean(elements.botShowIpGeo?.checked),
    showOverview: Boolean(elements.botShowOverview?.checked),
    eventDedupSeconds: elements.botDedupeSeconds?.value || DEFAULT_BOT_CONFIG.eventDedupSeconds,
    wechatCorpId: elements.botWechatCorpId?.value.trim() || "",
    wechatAgentId: elements.botWechatAgentId?.value.trim() || "",
    wechatSecret: elements.botWechatSecret?.value.trim() || "",
    wechatToUser: elements.botWechatToUser?.value.trim() || "@all",
    wechatCallbackToken: elements.botWechatCallbackToken?.value.trim() || "",
    wechatCallbackAes: elements.botWechatCallbackAes?.value.trim() || ""
  });
}

function normalizeBotWebhookProcessed(raw) {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  return {
    at: String(raw.at || "").trim(),
    eventType: String(raw.eventType || "unknown").trim() || "unknown",
    result: String(raw.result || "unknown").trim() || "unknown",
    detail: String(raw.detail || "").trim()
  };
}

function normalizeBotWebhookState(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const processed = normalizeBotWebhookProcessed(source.lastProcessed || source.lastWebhook);
  const lastReceivedAt = String(source.lastReceivedAt || "").trim();
  return {
    lastReceivedAt,
    lastProcessed: processed
  };
}

function isPrivateOrLocalHost(hostname) {
  const host = String(hostname || "").trim().toLowerCase();
  if (!host) {
    return true;
  }
  if (host === "localhost" || host === "127.0.0.1" || host === "::1") {
    return true;
  }
  if (host.endsWith(".local") || host.endsWith(".in-addr.arpa") || host.endsWith(".ip6.arpa")) {
    return true;
  }
  if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(host)) {
    return false;
  }
  const parts = host.split(".").map((part) => Number.parseInt(part, 10));
  if (parts.some((num) => !Number.isFinite(num) || num < 0 || num > 255)) {
    return true;
  }
  if (parts[0] === 10 || parts[0] === 127) {
    return true;
  }
  if (parts[0] === 192 && parts[1] === 168) {
    return true;
  }
  if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) {
    return true;
  }
  return false;
}

function getWebhookUrlWarning(url) {
  const text = String(url || "").trim();
  if (!text) {
    return "";
  }
  try {
    const parsed = new URL(text);
    if (isPrivateOrLocalHost(parsed.hostname)) {
      return "⚠️ 当前是本地/私网地址，远端 Emby 无法回调。请配置 BOT_PUBLIC_BASE_URL 为公网域名。";
    }
  } catch {
    return "⚠️ Webhook 地址格式异常，请检查 BOT_PUBLIC_BASE_URL 配置。";
  }
  return "";
}

function getBotWebhookResultLabel(result) {
  const labels = {
    sent: "sent（已发送）",
    core_disabled: "core_disabled（总开关关闭）",
    playback_disabled: "playback_disabled（播放通知关闭）",
    playback_event_disabled: "playback_event_disabled（该播放事件已关闭）",
    library_disabled: "library_disabled（入库通知关闭）",
    unsupported_event: "unsupported_event（未识别事件）",
    telegram_not_configured: "telegram_not_configured（TG 未配置）",
    token_invalid: "token_invalid（token 无效）",
    invalid_payload: "invalid_payload（请求体无效）",
    telegram_error: "telegram_error（发送失败）",
    duplicate_skipped: "duplicate_skipped（重复事件已去重）",
    playback_event_filtered: "playback_event_filtered（非开始/停止事件）"
  };
  const key = String(result || "").trim() || "unknown";
  return labels[key] || key;
}

function renderBotWebhookStatus() {
  if (!elements.botWebhookStatus) {
    return;
  }
  const state = normalizeBotWebhookState(appState.botWebhookState || {});
  const receivedAt = state.lastReceivedAt;
  const processed = state.lastProcessed;
  if (!receivedAt && !processed) {
    elements.botWebhookStatus.textContent = "最近接收：暂无 webhook 请求";
    if (appState.botWebhookWarning) {
      elements.botWebhookStatus.textContent += `\n${appState.botWebhookWarning}`;
      elements.botWebhookStatus.classList.add("route-status-warning");
    } else {
      elements.botWebhookStatus.classList.remove("route-status-warning");
    }
    return;
  }
  const receivedText = receivedAt ? formatDate(receivedAt) : "未知";
  if (!processed) {
    elements.botWebhookStatus.textContent = `最近接收：${receivedText} · 最近处理：暂无`;
  } else {
    const atText = formatDate(processed.at);
    const resultText = getBotWebhookResultLabel(processed.result);
    const detailText = processed.detail ? `，${processed.detail}` : "";
    elements.botWebhookStatus.textContent = `最近接收：${receivedText} · 最近处理：${resultText} · event=${processed.eventType} · ${atText}${detailText}`;
  }
  if (appState.botWebhookWarning) {
    elements.botWebhookStatus.textContent += `\n${appState.botWebhookWarning}`;
    elements.botWebhookStatus.classList.add("route-status-warning");
  } else {
    elements.botWebhookStatus.classList.remove("route-status-warning");
  }
}

async function refreshBotWebhookInfo(options = {}) {
  const { silent = true, statusOnly = false } = options;
  if (appState.botWebhookRefreshPromise) {
    return appState.botWebhookRefreshPromise;
  }

  appState.botWebhookRefreshPromise = (async () => {
    const fallbackUrl = getWebhookUrlForBot();
    if (!statusOnly && elements.botWebhookUrl) {
      elements.botWebhookUrl.textContent = appState.botWebhookUrl || fallbackUrl;
    }

    try {
      const requests = [inviteApiFetch("/api/bot/webhook-status")];
      if (!statusOnly) {
        requests.unshift(inviteApiFetch("/api/bot/webhook-url"));
      }
      const [urlResult, statusResult] = statusOnly ? [null, await requests[0]] : await Promise.all(requests);

      if (!statusOnly) {
        const serverUrl = String(urlResult?.webhookUrl || "").trim();
        appState.botWebhookUrl = serverUrl || fallbackUrl;
        appState.botWebhookWarning = getWebhookUrlWarning(appState.botWebhookUrl);
        if (elements.botWebhookUrl) {
          elements.botWebhookUrl.textContent = appState.botWebhookUrl;
          elements.botWebhookUrl.classList.toggle("route-copy-warning", Boolean(appState.botWebhookWarning));
        }
      }

      appState.botWebhookState = normalizeBotWebhookState(statusResult || {});
      renderBotWebhookStatus();
    } catch (error) {
      if (!statusOnly) {
        appState.botWebhookUrl = fallbackUrl;
        appState.botWebhookWarning = getWebhookUrlWarning(fallbackUrl);
        if (elements.botWebhookUrl) {
          elements.botWebhookUrl.textContent = fallbackUrl;
          elements.botWebhookUrl.classList.toggle("route-copy-warning", Boolean(appState.botWebhookWarning));
        }
      }
      if (!silent && elements.botFeedback) {
        elements.botFeedback.textContent = `读取 webhook 状态失败：${error.message || "未知错误"}`;
        elements.botFeedback.classList.remove("feedback-success");
      }
      renderBotWebhookStatus();
    } finally {
      appState.botWebhookRefreshPromise = null;
    }
  })();

  return appState.botWebhookRefreshPromise;
}

function ensureBotWebhookStatusPolling() {
  if (appState.botWebhookStatusTimer) {
    return;
  }
  appState.botWebhookStatusTimer = setInterval(() => {
    if (document.visibilityState === "visible") {
      refreshBotWebhookInfo({ silent: true, statusOnly: true });
    }
  }, 15000);
}

function renderBotAssistant() {
  if (!elements.botSaveBtn) {
    return;
  }

  const config = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...appState.botConfig });
  elements.botEnableCore.checked = Boolean(config.enableCore);
  elements.botEnablePlayback.checked = Boolean(config.enablePlayback);
  elements.botEnableLibrary.checked = Boolean(config.enableLibrary);
  if (elements.botEnableCommands) {
    elements.botEnableCommands.checked = Boolean(config.enableCommands);
  }
  if (elements.botEventStart) {
    elements.botEventStart.checked = Boolean(config.notifyEvents?.start);
  }
  if (elements.botEventPause) {
    elements.botEventPause.checked = Boolean(config.notifyEvents?.pause);
  }
  if (elements.botEventResume) {
    elements.botEventResume.checked = Boolean(config.notifyEvents?.resume);
  }
  if (elements.botEventStop) {
    elements.botEventStop.checked = Boolean(config.notifyEvents?.stop);
  }
  if (elements.botShowIp) {
    elements.botShowIp.checked = Boolean(config.showIp);
  }
  if (elements.botShowIpGeo) {
    elements.botShowIpGeo.checked = Boolean(config.showIpGeo);
  }
  if (elements.botShowOverview) {
    elements.botShowOverview.checked = Boolean(config.showOverview);
  }
  if (elements.botDedupeSeconds) {
    elements.botDedupeSeconds.value = String(config.eventDedupSeconds ?? DEFAULT_BOT_CONFIG.eventDedupSeconds);
  }
  elements.botTelegramToken.value = config.telegramToken || "";
  elements.botTelegramChatId.value = config.telegramChatId || "";
  elements.botWechatCorpId.value = config.wechatCorpId || "";
  elements.botWechatAgentId.value = config.wechatAgentId || "";
  elements.botWechatSecret.value = config.wechatSecret || "";
  elements.botWechatToUser.value = config.wechatToUser || "@all";
  elements.botWechatCallbackToken.value = config.wechatCallbackToken || "";
  elements.botWechatCallbackAes.value = config.wechatCallbackAes || "";
  const webhookUrl = appState.botWebhookUrl || getWebhookUrlForBot();
  appState.botWebhookWarning = getWebhookUrlWarning(webhookUrl);
  elements.botWebhookUrl.textContent = webhookUrl;
  elements.botWebhookUrl.classList.toggle("route-copy-warning", Boolean(appState.botWebhookWarning));
  renderBotWebhookStatus();
  if (elements.botWechatCallbackUrl) {
    elements.botWechatCallbackUrl.textContent = getWechatCallbackUrlForBot();
  }
  if (shouldUseLocalProxy()) {
    refreshBotWebhookInfo({ silent: true });
  }
  renderEnvControlledState();
}

async function loadBotConfigFromServer(options = {}) {
  const { silent = false } = options;
  try {
    const result = await inviteApiFetch("/api/bot/config");
    appState.botConfig = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...(result?.botConfig || {}) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "botConfig");
    persistLocalState();
    renderBotAssistant();
    return true;
  } catch (error) {
    if (!silent) {
      if (elements.botFeedback) {
        elements.botFeedback.textContent = `读取机器人配置失败：${error.message || "未知错误"}`;
        elements.botFeedback.classList.remove("feedback-success");
      }
      showToast("读取机器人配置失败", 1200);
    }
    return false;
  }
}

async function pushBotConfigToServer(nextConfig) {
  const payloadConfig = { ...nextConfig };
  const botManaged = appState?.envControlledFields?.botConfig || [];
  if (botManaged.includes("telegramToken")) {
    delete payloadConfig.telegramToken;
  }
  if (botManaged.includes("telegramChatId")) {
    delete payloadConfig.telegramChatId;
  }

  const result = await inviteApiFetch("/api/bot/config", {
    method: "POST",
    body: JSON.stringify({ botConfig: payloadConfig })
  });
  const saved = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...(result?.botConfig || nextConfig) });
  appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "botConfig");
  appState.botConfig = saved;
  persistLocalState();
  renderBotAssistant();
  return saved;
}

function setBotActionSuccessState(button) {
  if (!button) {
    return;
  }
  const original = button.dataset.originalText || button.textContent || "保存配置";
  button.dataset.originalText = original;
  button.textContent = "保存成功";
  button.classList.add("bot-save-success");
  button.disabled = true;
  setTimeout(() => {
    if (button) {
      button.textContent = original;
      button.classList.remove("bot-save-success");
      button.disabled = false;
    }
  }, 1000);
}

async function saveBotConfig() {
  const nextConfig = readBotConfigFromInputs();
  try {
    await pushBotConfigToServer(nextConfig);
  } catch (error) {
    if (elements.botFeedback) {
      elements.botFeedback.textContent = `保存失败：${error.message || "未知错误"}`;
      elements.botFeedback.classList.remove("feedback-success");
    }
    showToast("机器人配置保存失败", 1200);
    addSyncEvent("机器人配置保存失败", error.message || "未知错误", "danger");
    return;
  }

  if (elements.botFeedback) {
    elements.botFeedback.textContent = "保存成功";
    elements.botFeedback.classList.add("feedback-success");
  }
  setBotActionSuccessState(elements.botSaveBtn);
  showToast("配置已保存", 1000);
  addSyncEvent("机器人配置已保存", "Telegram / 企业微信通道配置已更新。", "success");
}

async function sendBotTest(channel) {
  const nextConfig = readBotConfigFromInputs();
  appState.botConfig = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...nextConfig });
  persistLocalState();
  renderBotAssistant();

  if (channel !== "telegram") {
    if (elements.botFeedback) {
      elements.botFeedback.textContent = "企业微信测试消息已触发（示例模式）。接入后端后可发送真实消息。";
      elements.botFeedback.classList.remove("feedback-success");
    }
    showToast("✅ 测试消息已成功发送至企业微信！", 1000);
    addSyncEvent("企业微信测试发送", "前端配置已验证，等待后端通知服务接入。", "success");
    return;
  }

  try {
    await pushBotConfigToServer(nextConfig);
    const result = await inviteApiFetch("/api/bot/test", {
      method: "POST",
      body: JSON.stringify({ channel: "telegram" })
    });
    if (elements.botFeedback) {
      elements.botFeedback.textContent = result?.detail || "Telegram 测试消息已发送。";
      elements.botFeedback.classList.add("feedback-success");
    }
    showToast("✅ 测试消息已成功发送至 Telegram！", 1000);
    addSyncEvent("Telegram 测试发送", result?.detail || "已发送真实 Telegram 测试消息。", "success");
  } catch (error) {
    if (elements.botFeedback) {
      elements.botFeedback.textContent = `Telegram 测试发送失败：${error.message || "未知错误"}`;
      elements.botFeedback.classList.remove("feedback-success");
    }
    showToast("Telegram 测试发送失败", 1400);
    addSyncEvent("Telegram 测试发送失败", error.message || "未知错误", "danger");
  }
}

function togglePasswordVisibility(button) {
  const targetId = button.dataset.target;
  if (!targetId) {
    return;
  }
  const input = document.getElementById(targetId);
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  const isOpen = button.dataset.open === "true";
  input.type = isOpen ? "password" : "text";
  button.dataset.open = isOpen ? "false" : "true";
  button.classList.toggle("is-open", !isOpen);
  button.innerHTML = isOpen ? EYE_CLOSED_SVG : EYE_OPEN_SVG;
  button.classList.remove("eye-animate");
  void button.offsetWidth;
  button.classList.add("eye-animate");
}

async function loadEmbyData() {
  try {
    renderConnectionState(true, "正在从 Emby 拉取数据...");
    const [systemInfo, usersResult, sessions, logsResult, mediaCounts, topItems, devicesResult, qualityItemsResult] = await Promise.all([
      embyFetch("/System/Info"),
      embyFetch("/Users/Query?Limit=200"),
      embyFetch("/Sessions"),
      embyFetch("/System/ActivityLog/Entries?Limit=100"),
      embyFetch("/Items/Counts").catch(() => null),
      embyFetch(
        "/Items?Recursive=true&IncludeItemTypes=Movie&SortBy=PlayCount&SortOrder=Descending&Limit=100"
      ).catch(() => null),
      embyFetch("/Devices").catch(() => null),
      embyFetch(QUALITY_RESOLUTION_ITEMS_QUERY).catch(() => null)
    ]);

    appState.systemInfo = systemInfo;
    appState.users = usersResult.Items || [];
    appState.sessions = Array.isArray(sessions) ? sessions : [];
    appState.devices = normalizeDeviceList(devicesResult);
    appState.logs = logsResult.Items || [];
    appState.mediaCounts = mediaCounts;
    appState.qualityResolutionStats = buildQualityResolutionStats(qualityItemsResult?.Items || []);
    appState.qualityResolutionItemsByBucket = appState.qualityResolutionStats?.itemsByBucket || {};

    const rankingFromItems = buildRankingFromItems(topItems?.Items || []);
    if (rankingFromItems.length > 0) {
      appState.contentRanking = rankingFromItems;
      appState.contentRankingSource = "items";
    } else {
      appState.contentRanking = buildRankingFromLogs(appState.logs);
      appState.contentRankingSource = appState.contentRanking.length > 0 ? "logs" : "none";
    }

    if (!appState.selectedUserId && appState.users[0]) {
      appState.selectedUserId = appState.users[0].Id;
    }

    renderConnectionState(true, `已连接 ${systemInfo.ServerName || "Emby Server"}，数据同步完成。`);
    addSyncEvent(
      "数据同步完成",
      `已同步 ${appState.users.length} 个用户、${appState.sessions.length} 个在线会话、${appState.logs.length} 条日志。`,
      "success"
    );
  } catch (error) {
    renderConnectionState(false, `连接失败：${error.message}`, "danger");
    addSyncEvent("连接 Emby 失败", error.message, "danger");
  }

  renderAll();
}

async function runQualityRescan() {
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    if (elements.userActionFeedback) {
      elements.userActionFeedback.textContent = "请先在系统设置里连接 Emby，再执行重扫。";
    }
    return;
  }

  if (appState.qualityRescanPromise) {
    if (elements.userActionFeedback) {
      elements.userActionFeedback.textContent = "正在重扫，请稍候...";
    }
    return appState.qualityRescanPromise;
  }

  const rescanTask = (async () => {
    if (elements.qualityRescanBtn) {
      elements.qualityRescanBtn.disabled = true;
      elements.qualityRescanBtn.setAttribute("aria-busy", "true");
      elements.qualityRescanBtn.textContent = "重扫中...";
    }
    if (elements.userActionFeedback) {
      elements.userActionFeedback.textContent = "正在重扫媒体库，请稍候...";
    }
    appState.qualityResolutionStats = null;
    appState.qualityResolutionItemsByBucket = {};
    appState.qualityResolutionFilteredEntries = [];
    renderQualityResolutionMatrix();

    const qualityItemsResult = await embyFetch(QUALITY_RESOLUTION_ITEMS_QUERY);
    appState.qualityResolutionStats = buildQualityResolutionStats(qualityItemsResult?.Items || []);
    appState.qualityResolutionItemsByBucket = appState.qualityResolutionStats?.itemsByBucket || {};

    const bucketKeys = appState.qualityResolutionStats?.buckets?.map((bucket) => bucket.key) || [];
    if (!bucketKeys.includes(appState.qualityResolutionActiveBucket)) {
      appState.qualityResolutionActiveBucket = bucketKeys[0] || "uhd";
    }

    renderQualityResolutionMatrix();
    const total = appState.qualityResolutionStats?.total || 0;
    commitQualityScanTime(Date.now());
    if (elements.userActionFeedback) {
      elements.userActionFeedback.textContent = `重扫完成，已更新 ${total.toLocaleString("zh-CN")} 部媒体分辨率数据。`;
    }
    addSyncEvent("质量盘点重扫完成", `已重新扫描媒体库并更新 ${total.toLocaleString("zh-CN")} 条分辨率样本。`, "success");
  })();

  appState.qualityRescanPromise = rescanTask;
  try {
    await rescanTask;
  } catch (error) {
    if (elements.userActionFeedback) {
      elements.userActionFeedback.textContent = `重扫失败：${error.message}`;
    }
    addSyncEvent("质量盘点重扫失败", error.message, "danger");
  } finally {
    appState.qualityRescanPromise = null;
    if (elements.qualityRescanBtn) {
      elements.qualityRescanBtn.disabled = false;
      elements.qualityRescanBtn.removeAttribute("aria-busy");
      elements.qualityRescanBtn.textContent = "重扫媒体库";
    }
  }

  return null;
}

async function runConnectionDiagnosis() {
  applyConfigFromInputs();

  if (!appState.config.serverUrl || !appState.config.apiKey) {
    elements.connectionMessage.textContent = "诊断失败：请先填写服务器地址和 API Key。";
    return;
  }

  const checks = [
    { name: "系统信息", path: "/System/Info" },
    { name: "用户列表", path: "/Users/Query?Limit=1" },
    { name: "在线会话", path: "/Sessions" },
    { name: "活动日志", path: "/System/ActivityLog/Entries?Limit=1" },
    { name: "媒体库计数", path: "/Items/Counts" }
  ];

  const results = [];
  for (const check of checks) {
    try {
      await embyFetch(check.path);
      results.push(`✓ ${check.name}`);
    } catch (error) {
      results.push(`✗ ${check.name}（${error.message}）`);
    }
  }

  const hasFailure = results.some((item) => item.startsWith("✗"));
  const summary = hasFailure ? "诊断完成：存在失败项" : "诊断完成：连接正常";
  elements.connectionMessage.textContent = `${summary}。${results.join("；")}`;
  addSyncEvent("连接诊断", results.join("；"), hasFailure ? "danger" : "success");
}

function renderAll() {
  renderQualityScanMeta();
  renderStats();
  renderOverview();
  renderContentRanking();
  renderClientControl();
  renderQualityResolutionMatrix();
  renderUsers();
  renderUserCenter();
  renderInvites();
  renderRenewals();
  renderLogs();
  renderBotAssistant();
}

function swapLogsActionBlocks(activeView) {
  if (
    !elements.topbarLogsToolbarHost ||
    !elements.logsToolbarHost ||
    !elements.logsToolbar
  ) {
    return;
  }

  const isLogsView = activeView === "logs";

  if (isLogsView) {
    if (elements.logsToolbar.parentElement !== elements.topbarLogsToolbarHost) {
      elements.topbarLogsToolbarHost.appendChild(elements.logsToolbar);
    }
    elements.topbarLogsToolbarHost.hidden = false;
    return;
  }

  if (elements.logsToolbar.parentElement !== elements.logsToolbarHost) {
    elements.logsToolbarHost.appendChild(elements.logsToolbar);
  }
  elements.topbarLogsToolbarHost.hidden = true;
}

function switchView(view) {
  const targetView = view && VIEW_META[view] ? view : "";

  if (!targetView) {
    resetSettingsSaveFeedback();
    closeUserCenterInviteModal();
    closeUserCenterInviteManageModal();
    closeUserCenterInviteResultModal();
    closeCreateUserModal();
    closeUserConfigModal();
    appState.activeView = "";
    localStorage.removeItem(STORAGE_KEYS.activeView);
    document.title = "镜界Vistamirror";
    if (elements.mainContent) {
      elements.mainContent.dataset.activeView = "";
    }
    if (elements.overviewStatsGrid) {
      elements.overviewStatsGrid.style.display = "none";
    }
    elements.navItems.forEach((item) => item.classList.remove("active"));
    elements.viewSections.forEach((section) => section.classList.remove("active"));
    if (elements.topbarTitle) {
      elements.topbarTitle.textContent = "请选择左侧菜单";
    }
    if (elements.topbarSubtitle) {
      elements.topbarSubtitle.textContent = "选择模块后再加载对应页面内容";
    }
    if (elements.topbarActions) {
      elements.topbarActions.hidden = true;
    }
    if (elements.settingsSaveBtn) {
      elements.settingsSaveBtn.hidden = true;
    }
    if (elements.topbarUserCenterActions) {
      elements.topbarUserCenterActions.hidden = true;
    }
    if (elements.topbarBotActions) {
      elements.topbarBotActions.hidden = true;
    }
    document.dispatchEvent(new CustomEvent("adaptive:viewchange", { detail: { view: "" } }));
    closeProfileMenu();
    return;
  }

  if (targetView !== "settings") {
    resetSettingsSaveFeedback();
  }
  if (targetView !== "user-center") {
    closeUserCenterInviteModal();
    closeUserCenterInviteManageModal();
    closeUserCenterInviteResultModal();
    closeCreateUserModal();
    closeUserConfigModal();
  }

  const meta = VIEW_META[targetView];

  appState.activeView = targetView;
  localStorage.setItem(STORAGE_KEYS.activeView, targetView);
  document.title = `${meta.title} - 镜界Vistamirror`;
  if (elements.mainContent) {
    elements.mainContent.dataset.activeView = targetView;
  }
  if (elements.overviewStatsGrid) {
    elements.overviewStatsGrid.style.display = targetView === "overview" ? "grid" : "none";
  }
  elements.navItems.forEach((item) => item.classList.toggle("active", item.dataset.view === targetView));
  elements.viewSections.forEach((section) => section.classList.toggle("active", section.id === `view-${targetView}`));

  if (elements.topbarTitle) {
    elements.topbarTitle.textContent = meta.title;
  }
  if (elements.topbarSubtitle) {
    elements.topbarSubtitle.textContent = meta.subtitle;
  }
  swapLogsActionBlocks(targetView);
  if (elements.topbarLogsToolbarHost) {
    elements.topbarLogsToolbarHost.hidden = targetView !== "logs";
  }
  if (elements.topbarActions) {
    elements.topbarActions.hidden = false;
  }
  if (elements.settingsSaveBtn) {
    elements.settingsSaveBtn.hidden = targetView !== "settings";
  }
  if (elements.topbarUserCenterActions) {
    elements.topbarUserCenterActions.hidden = targetView !== "user-center";
  }
  if (elements.topbarBotActions) {
    elements.topbarBotActions.hidden = targetView !== "bot-assistant";
  }
  if (elements.topbarIcon) {
    elements.topbarIcon.textContent = meta.icon || "👥";
  }
  if (elements.mainContent) {
    elements.mainContent.scrollTo({ top: 0, behavior: "smooth" });
  }
  document.dispatchEvent(new CustomEvent("adaptive:viewchange", { detail: { view: targetView } }));
  closeProfileMenu();
}

function openProfileMenu() {
  if (!elements.profileMenuPanel || !elements.profileMenuBtn) {
    return;
  }
  elements.profileMenuPanel.hidden = false;
  elements.profileMenuBtn.setAttribute("aria-expanded", "true");
}

function closeProfileMenu() {
  if (!elements.profileMenuPanel || !elements.profileMenuBtn) {
    return;
  }
  elements.profileMenuPanel.hidden = true;
  elements.profileMenuBtn.setAttribute("aria-expanded", "false");
}

function toggleProfileMenu() {
  if (!elements.profileMenuPanel) {
    return;
  }
  if (elements.profileMenuPanel.hidden) {
    openProfileMenu();
  } else {
    closeProfileMenu();
  }
}

async function savePolicy() {
  const user = getSelectedUser();
  if (!user) {
    return;
  }

  if (!user.Policy) {
    elements.userActionFeedback.textContent = "当前用户没有可用的 Policy 数据。";
    return;
  }

  const nextPolicy = {
    ...user.Policy,
    EnableRemoteAccess: elements.toggleRemote.checked,
    EnableContentDownloading: elements.toggleDownload.checked,
    EnableLiveTvAccess: elements.toggleLivetv.checked,
    IsDisabled: elements.toggleDisabled.checked
  };

  try {
    await embyFetch(`/Users/${user.Id}/Policy`, {
      method: "POST",
      body: JSON.stringify(nextPolicy)
    });
    user.Policy = nextPolicy;
    elements.userActionFeedback.textContent = `${user.Name} 的权限设置已保存到 Emby。`;
    addSyncEvent("用户权限已保存", `${user.Name} 的远程访问、下载、Live TV 或禁用状态已更新。`, "success");
    renderUsers();
  } catch (error) {
    elements.userActionFeedback.textContent = `保存失败：${error.message}`;
    addSyncEvent("保存用户权限失败", error.message, "danger");
  }
}

async function resetPassword() {
  const user = getSelectedUser();
  if (!user) {
    return;
  }

  try {
    await embyFetch(`/Users/${user.Id}/Password`, {
      method: "POST",
      body: JSON.stringify({
        Id: user.Id,
        NewPw: "",
        ResetPassword: true
      })
    });
    elements.userActionFeedback.textContent = `${user.Name} 的密码已重置，用户下次登录时需要重新设置。`;
    addSyncEvent("密码已重置", `已对 ${user.Name} 执行 Emby 密码重置。`, "success");
  } catch (error) {
    elements.userActionFeedback.textContent = `重置失败：${error.message}`;
    addSyncEvent("密码重置失败", error.message, "danger");
  }
}

function fillRenewalForm(userId) {
  const user = appState.users.find((item) => item.Id === userId);
  const renewal = getUserRenewal(userId);
  if (!user) {
    return;
  }
  elements.renewalUser.value = userId;
  elements.renewalPlan.value = renewal.plan || "";
  elements.renewalExpiry.value = renewal.expiry || "";
  elements.renewalNote.value = renewal.note || "";
  switchView("renewals");
}

function quickRenew(userId, days) {
  const renewal = getUserRenewal(userId);
  const baseDate = renewal.expiry ? new Date(`${renewal.expiry}T00:00:00`) : new Date();
  baseDate.setDate(baseDate.getDate() + days);
  appState.renewals[userId] = {
    ...renewal,
    expiry: baseDate.toISOString().slice(0, 10)
  };
  persistLocalState();
  renderAll();
  addSyncEvent("续期信息已更新", `用户 ${userId} 已续期 ${days} 天（本地管理层）。`, "success");
}

function openUserCenterInviteModal() {
  if (!elements.ucInviteModal) {
    return;
  }
  resetUserCenterInviteConfig();
  closeUserConfigModal();
  closeUserCenterInviteResultModal();
  closeUserCenterInviteManageModal();
  closeCreateUserModal();
  elements.ucInviteModal.hidden = false;
}

function closeUserCenterInviteModal() {
  if (!elements.ucInviteModal) {
    return;
  }
  elements.ucInviteModal.hidden = true;
}

async function openUserCenterInviteManageModal() {
  if (!elements.ucInviteManageModal) {
    return;
  }
  await refreshInvitesFromServer({ silent: true });
  renderInvites();
  closeUserConfigModal();
  closeUserCenterInviteModal();
  closeUserCenterInviteResultModal();
  closeCreateUserModal();
  elements.ucInviteManageModal.hidden = false;
}

function closeUserCenterInviteManageModal() {
  if (!elements.ucInviteManageModal) {
    return;
  }
  elements.ucInviteManageModal.hidden = true;
}

async function bulkDeleteSelectedInvites() {
  const selected = Array.from(document.querySelectorAll("[data-select-invite-id]:checked"))
    .map((item) => item.getAttribute("data-select-invite-id"))
    .filter(Boolean);

  if (selected.length === 0) {
    showToast("请先勾选要删除的邀请码", 1000);
    return;
  }

  appState.invites = appState.invites.filter((invite) => !selected.includes(invite.id || invite.code));
  persistLocalState();
  renderInvites();
  renderOverview();
  renderUserCenter();
  await syncInviteStore({
    silentSuccess: true,
    failureToast: "邀请码已删除，但服务端同步失败，请稍后重试。",
    successToast: "批量删除已同步"
  });
  addSyncEvent("邀请码已删除", `已删除 ${selected.length} 条邀请码。`, "success");
}

function openUserCenterInviteResultModal() {
  if (!elements.ucInviteResultModal) {
    return;
  }
  closeUserConfigModal();
  closeUserCenterInviteModal();
  closeUserCenterInviteManageModal();
  closeCreateUserModal();
  elements.ucInviteResultModal.hidden = false;
}

function closeUserCenterInviteResultModal() {
  if (!elements.ucInviteResultModal) {
    return;
  }
  elements.ucInviteResultModal.hidden = true;
}

function syncInvitePresetButtons() {
  elements.ucInvitePresetButtons?.forEach((button) => {
    button.classList.toggle("active", button.dataset.days === appState.invitePresetDays);
  });
}

function resetUserCenterInviteConfig() {
  appState.invitePresetDays = "30";
  appState.generatedInviteLinks = [];
  if (elements.ucInviteCustomDays) {
    elements.ucInviteCustomDays.value = "";
  }
  if (elements.ucInviteQuantity) {
    elements.ucInviteQuantity.value = "1";
  }
  if (elements.ucInviteTemplate) {
    elements.ucInviteTemplate.value = "default";
  }
  syncInvitePresetButtons();
}

function getSelectedInviteTemplateText() {
  const select = elements.ucInviteTemplate;
  if (!select) {
    return "默认模板 (全库)";
  }
  const option = select.options[select.selectedIndex];
  return option?.text || "默认模板 (全库)";
}

function resolveInviteDaysFromConfig() {
  const customDays = Number(elements.ucInviteCustomDays?.value || "");
  if (Number.isFinite(customDays) && customDays > 0) {
    return Math.floor(customDays);
  }
  if (appState.invitePresetDays === "permanent") {
    return null;
  }
  const preset = Number(appState.invitePresetDays);
  if (Number.isFinite(preset) && preset > 0) {
    return preset;
  }
  return 30;
}

function getInvitePublicBaseUrl() {
  if (typeof window !== "undefined" && window.location?.origin) {
    return window.location.origin.replace(/\/+$/, "");
  }

  const configured = String(appState.config.serverUrl || "").trim();
  if (configured) {
    return configured
      .replace(/\/emby$/i, "")
      .replace(/\/+$/, "");
  }
  return "http://127.0.0.1:8080";
}

function buildInviteLink(code) {
  return `${getInvitePublicBaseUrl()}/invite/${String(code || "").toLowerCase()}`;
}

function generateInviteCode() {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  const exists = new Set(appState.invites.map((item) => String(item.code || "").toLowerCase()));
  for (let attempt = 0; attempt < 40; attempt += 1) {
    let next = "";
    for (let i = 0; i < 6; i += 1) {
      next += chars[Math.floor(Math.random() * chars.length)];
    }
    if (!exists.has(next)) {
      return next;
    }
  }
  return Math.random().toString(36).slice(2, 8).padEnd(6, "0").slice(0, 6).toLowerCase();
}

async function copyToClipboardWithToast(text, successMessage) {
  try {
    await navigator.clipboard.writeText(text);
    showToast(successMessage, 1000);
  } catch {
    showToast("复制失败，请手动复制。", 1200);
  }
}

function renderUserCenterInviteResultList() {
  if (!elements.ucInviteResultList) {
    return;
  }
  if (!appState.generatedInviteLinks.length) {
    elements.ucInviteResultList.innerHTML = `<div class="empty-state">暂无生成结果。</div>`;
    return;
  }

  elements.ucInviteResultList.innerHTML = appState.generatedInviteLinks
    .map(
      (link) => `
        <div class="invite-result-row">
          <span class="invite-result-url">${link}</span>
          <button class="invite-result-copy" type="button" data-copy-invite-link="${link}">复制</button>
        </div>
      `
    )
    .join("");

  document.querySelectorAll("[data-copy-invite-link]").forEach((button) => {
    button.addEventListener("click", async () => {
      await copyToClipboardWithToast(button.dataset.copyInviteLink, "复制成功");
    });
  });
}

function createInviteRecord(payload, options = {}) {
  const { silent = false, deferRender = false, persist = true } = options;
  const permanent = Boolean(payload.permanent);
  const daysInput = Number(payload.days || 7);
  const normalizedDays = Number.isFinite(daysInput) && daysInput > 0 ? Math.floor(daysInput) : 7;
  let expiresAtText = "";
  if (!permanent) {
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + normalizedDays);
    expiresAtText = expiresAt.toISOString().slice(0, 10);
  }

  const invite = {
    id: crypto.randomUUID(),
    code: generateInviteCode(),
    label: String(payload.label || "").trim(),
    username: String(payload.username || "").trim(),
    plan: String(payload.plan || "").trim(),
    initialDays: permanent ? null : normalizedDays,
    expiresAt: expiresAtText,
    status: "active",
    createdAt: new Date().toISOString(),
    usedUsername: ""
  };

  appState.invites.push(invite);
  if (persist) {
    persistLocalState();
  }
  if (!deferRender) {
    renderInvites();
    renderOverview();
    renderUserCenter();
  }
  if (!silent) {
    addSyncEvent("邀请已生成", `已创建邀请码 ${invite.code}，有效期至 ${invite.expiresAt || "永久"}。`, "success");
  }
  return invite;
}

async function createInvite(event) {
  event.preventDefault();
  const daysInput = Number(elements.inviteDays.value || 7);
  const duration = Number.isFinite(daysInput) && daysInput > 0 ? Math.floor(daysInput) : 7;

  try {
    const result = await generateInvitesOnServer({
      label: elements.inviteLabel.value,
      username: elements.inviteUsername.value,
      plan: elements.invitePlan.value,
      duration,
      quantity: 1
    });
    await refreshInviteSyncStatus({ silent: false });

    const generatedCode =
      (Array.isArray(result?.generated) && result.generated[0]?.code) ||
      (Array.isArray(result?.invites) && result.invites[0]?.code) ||
      "";
    elements.inviteForm.reset();
    elements.inviteDays.value = "7";
    showToast(generatedCode ? `邀请码 ${generatedCode} 已生成并同步` : "邀请码已生成并同步", 1000);
    addSyncEvent("邀请已生成", generatedCode ? `已生成邀请码 ${generatedCode} 并写入服务端。` : "邀请码已写入服务端。", "success");
  } catch (error) {
    showToast("邀请码生成失败", 1200);
    addSyncEvent("邀请码生成失败", error.message || "未知错误", "danger");
  }
}

async function createUserCenterInvite(event) {
  event.preventDefault();
  const quantityRaw = Number(elements.ucInviteQuantity?.value || 1);
  const quantity = Math.min(50, Math.max(1, Number.isFinite(quantityRaw) ? Math.floor(quantityRaw) : 1));
  if (elements.ucInviteQuantity) {
    elements.ucInviteQuantity.value = String(quantity);
  }

  const effectiveDays = resolveInviteDaysFromConfig();
  const templateText = getSelectedInviteTemplateText();
  try {
    const result = await generateInvitesOnServer({
      label: templateText,
      plan: templateText,
      username: "",
      duration: effectiveDays,
      permanent: effectiveDays === null,
      quantity
    });
    await refreshInviteSyncStatus({ silent: false });

    const generatedRows = Array.isArray(result?.generated) ? result.generated : [];
    const links = generatedRows
      .map((invite) => String(invite?.code || "").trim().toLowerCase())
      .filter(Boolean)
      .map((code) => buildInviteLink(code));

    appState.generatedInviteLinks = links;
    renderUserCenterInviteResultList();
    closeUserCenterInviteModal();
    openUserCenterInviteResultModal();
    addSyncEvent("邀请已生成", `批量生成 ${links.length || quantity} 条邀请链接并写入服务端。`, "success");
  } catch (error) {
    showToast("批量邀请码生成失败", 1200);
    addSyncEvent("批量邀请码生成失败", error.message || "未知错误", "danger");
  }
}

function saveRenewal(event) {
  event.preventDefault();
  const userId = elements.renewalUser.value;
  if (!userId) {
    return;
  }

  appState.renewals[userId] = {
    plan: elements.renewalPlan.value.trim(),
    expiry: elements.renewalExpiry.value,
    note: elements.renewalNote.value.trim()
  };
  persistLocalState();
  renderAll();
  elements.renewalFeedback.textContent = "续费信息已保存到本地管理层。";
  addSyncEvent("续费信息已保存", `用户 ${userId} 的套餐与到期日已更新。`, "success");
}

function seedDemoConfig() {
  elements.serverUrl.value = "http://127.0.0.1:8096";
  if (elements.tmdbLanguage) {
    elements.tmdbLanguage.value = "zh-CN";
  }
  if (elements.tmdbRegion) {
    elements.tmdbRegion.value = "CN";
  }
  elements.connectionMessage.textContent = "已填入演示地址。请先运行 `python3 dev_server.py`，再粘贴 API Key 并连接。";
  refreshTmdbUiState();
}

function readConfigFromInputs() {
  return {
    serverUrl: normalizeServerUrl(elements.serverUrl.value),
    apiKey: elements.apiKey.value.trim(),
    clientName: DEFAULT_EMBY_CLIENT_NAME,
    tmdbEnabled: Boolean(elements.tmdbEnabled?.checked),
    tmdbToken: String(elements.tmdbToken?.value || "").trim(),
    tmdbLanguage: String(elements.tmdbLanguage?.value || "zh-CN").trim() || "zh-CN",
    tmdbRegion: String(elements.tmdbRegion?.value || "CN").trim().toUpperCase() || "CN"
  };
}

function applyConfigFromInputs(options = {}) {
  const { persist = false } = options;
  appState.config = normalizeAppConfig(readConfigFromInputs());
  if (persist) {
    persistLocalState();
  }
  return appState.config;
}

function clearConfig() {
  appState.config = normalizeAppConfig({
    serverUrl: "",
    apiKey: "",
    clientName: DEFAULT_EMBY_CLIENT_NAME,
    tmdbEnabled: false,
    tmdbToken: "",
    tmdbLanguage: "zh-CN",
    tmdbRegion: "CN"
  });
  appState.devices = [];
  appState.qualityResolutionStats = null;
  appState.qualityResolutionItemsByBucket = {};
  appState.qualityResolutionActiveBucket = "uhd";
  appState.qualityResolutionFilters = normalizeQualityResolutionFilters({ type: "all", keyword: "", sort: "resolution_desc" });
  appState.qualityResolutionFilteredEntries = [];
  appState.qualityResolutionFocusBucketKey = "uhd";
  appState.qualityResolutionFocusBucketTitle = "Ultra HD / 4K";
  appState.contentRanking = [];
  appState.contentRankingSource = "none";
  appState.playbackCoverCache = {};
  appState.playbackCoverCandidateCache = {};
  appState.playbackItemPosterCache = {};
  appState.playbackCoverLookupRunning = false;
  appState.tmdbPosterCache = {};
  appState.tmdbPosterCacheTimestamps = {};
  appState.tmdbInFlightMap = {};
  persistLocalState();
  elements.serverUrl.value = "";
  elements.apiKey.value = "";
  if (elements.tmdbEnabled) {
    elements.tmdbEnabled.checked = false;
  }
  if (elements.tmdbToken) {
    elements.tmdbToken.value = "";
  }
  if (elements.tmdbLanguage) {
    elements.tmdbLanguage.value = "zh-CN";
  }
  if (elements.tmdbRegion) {
    elements.tmdbRegion.value = "CN";
  }
  refreshTmdbUiState();
  renderConnectionState(false, "已清除配置。");
  renderContentRanking();
  renderClientControl();
}

function GlobalSearchModal() {
  const SEARCH_DEBOUNCE_MS = 300;
  const SEARCH_LIMIT = 20;
  const DISPLAY_LIMIT = 3;
  const INDEX_LIMIT = 3000;
  const SEARCH_TYPES = "Movie,Series,Season,Episode";
  const INDEX_SEARCH_TYPES = "Movie,Series,Season";
  const SEARCH_FIELDS = [
    "Overview",
    "ProductionYear",
    "PremiereDate",
    "SortName",
    "OriginalTitle",
    "SeriesName",
    "SeriesId",
    "SeasonId",
    "ParentId",
    "ImageTags",
    "MediaStreams",
    "MediaSources",
    "Genres",
    "Tags",
    "ChildCount",
    "RecursiveItemCount",
    "IndexNumber",
    "ParentIndexNumber"
  ].join(",");
  const PINYIN_FULL_MAP = {
    剑: "jian",
    来: "lai",
    长: "chang",
    安: "an",
    庆: "qing",
    余: "yu",
    年: "nian",
    凡: "fan",
    人: "ren",
    修: "xiu",
    仙: "xian",
    传: "zhuan",
    斗: "dou",
    破: "po",
    苍: "cang",
    穹: "qiong",
    吞: "tun",
    噬: "shi",
    星: "xing",
    空: "kong",
    完: "wan",
    美: "mei",
    世: "shi",
    界: "jie",
    遮: "zhe",
    天: "tian",
    牧: "mu",
    神: "shen",
    记: "ji",
    诡: "gui",
    秘: "mi",
    主: "zhu",
    之: "zhi",
    武: "wu",
    动: "dong",
    乾: "qian",
    坤: "kun",
    雪: "xue",
    中: "zhong",
    悍: "han",
    刀: "dao",
    行: "xing",
    狐: "hu",
    妖: "yao",
    小: "xiao",
    红: "hong",
    娘: "niang",
    灵: "ling",
    笼: "long",
    三: "san",
    体: "ti",
    流: "liu",
    浪: "lang",
    地: "di",
    球: "qiu",
    飞: "fei",
    驰: "chi",
    生: "sheng",
    活: "huo",
    热: "re",
    辣: "la",
    滚: "gun",
    烫: "tang"
  };
  const PINYIN_INITIAL_BOUNDS = [
    ["a", "阿"],
    ["b", "芭"],
    ["c", "嚓"],
    ["d", "哒"],
    ["e", "妸"],
    ["f", "发"],
    ["g", "旮"],
    ["h", "哈"],
    ["j", "击"],
    ["k", "喀"],
    ["l", "垃"],
    ["m", "妈"],
    ["n", "拿"],
    ["o", "噢"],
    ["p", "啪"],
    ["q", "期"],
    ["r", "然"],
    ["s", "撒"],
    ["t", "塌"],
    ["w", "挖"],
    ["x", "昔"],
    ["y", "压"],
    ["z", "匝"]
  ];

  const state = {
    mounted: false,
    open: false,
    query: "",
    loading: false,
    error: "",
    results: [],
    selectedIndex: 0,
    searchSeq: 0,
    debounceTimer: 0,
    indexPromise: null,
    indexItems: null,
    seasonTagCache: new Map(),
    itemDetailCache: new Map(),
    seriesSeasonCache: new Map(),
    seriesEpisodeCache: new Map()
  };

  const refs = {
    modal: null,
    panel: null,
    input: null,
    results: null
  };

  function normalizeSearchKey(value) {
    return String(value || "")
      .toLowerCase()
      .replace(/《|》|“|”|"|'/g, "")
      .replace(/[^\p{L}\p{N}]+/gu, "")
      .trim();
  }

  function isChineseChar(char) {
    return /[\u3400-\u9fff]/.test(char);
  }

  function getChineseInitial(char) {
    if (!isChineseChar(char)) {
      return normalizeSearchKey(char).slice(0, 1);
    }
    if (PINYIN_FULL_MAP[char]) {
      return PINYIN_FULL_MAP[char].slice(0, 1);
    }
    for (let index = PINYIN_INITIAL_BOUNDS.length - 1; index >= 0; index -= 1) {
      const [letter, bound] = PINYIN_INITIAL_BOUNDS[index];
      if (char.localeCompare(bound, "zh-CN") >= 0) {
        return letter;
      }
    }
    return "";
  }

  function toPinyinInitials(value) {
    return Array.from(String(value || ""))
      .map((char) => getChineseInitial(char))
      .join("");
  }

  function toSimplePinyin(value) {
    return Array.from(String(value || ""))
      .map((char) => {
        if (PINYIN_FULL_MAP[char]) {
          return PINYIN_FULL_MAP[char];
        }
        if (isChineseChar(char)) {
          return getChineseInitial(char);
        }
        return normalizeSearchKey(char);
      })
      .join("");
  }

  function isLikelyPinyinQuery(value) {
    return /^[a-z0-9]+$/i.test(String(value || "").trim());
  }

  function getItemTitle(item) {
    const type = String(item?.Type || "").toLowerCase();
    const name = String(item?.Name || "").trim();
    if (type === "season" && item?.SeriesName) {
      return `${String(item.SeriesName).trim()} - ${name}`;
    }
    return name || "未命名媒体";
  }

  function getItemYear(item) {
    const direct = Number(item?.ProductionYear || 0);
    if (direct >= 1900 && direct <= 2100) {
      return String(direct);
    }
    const dateText = String(item?.PremiereDate || item?.ProductionYear || "");
    const match = dateText.match(/^(19\d{2}|20\d{2}|21\d{2})/);
    return match?.[1] || "";
  }

  function buildGlobalSearchPosterUrl(item) {
    const imageTags = item?.ImageTags && typeof item.ImageTags === "object" ? item.ImageTags : {};
    return buildEmbyPrimaryPosterUrl(item?.Id || "", {
      maxWidth: 160,
      quality: 86,
      imageTag: imageTags.Primary || ""
    });
  }

  function buildItemSearchTokens(item) {
    const titles = dedupeStringList([
      getItemTitle(item),
      item?.Name,
      item?.OriginalTitle,
      item?.SortName,
      item?.SeriesName
    ]);
    return dedupeStringList(
      titles.flatMap((title) => [
        normalizeSearchKey(title),
        toPinyinInitials(title),
        toSimplePinyin(title)
      ])
    );
  }

  function scoreSearchItem(item, query) {
    const q = normalizeSearchKey(query);
    if (!q) {
      return 0;
    }
    const tokens = buildItemSearchTokens(item);
    const titleKey = normalizeSearchKey(getItemTitle(item));
    if (titleKey === q) {
      return 1000;
    }
    if (titleKey.startsWith(q)) {
      return 880;
    }
    if (tokens.some((token) => token === q)) {
      return 820;
    }
    if (tokens.some((token) => token.startsWith(q))) {
      return 760;
    }
    if (tokens.some((token) => token.includes(q))) {
      return 620;
    }
    return 0;
  }

  function isStrongSearchMatch(scoredRow) {
    return Number(scoredRow?.score || 0) >= 880;
  }

  function dedupeSearchItems(items) {
    const seen = new Set();
    return (items || []).filter((item) => {
      const id = String(item?.Id || "").trim();
      if (!id || seen.has(id)) {
        return false;
      }
      seen.add(id);
      return true;
    });
  }

  function normalizeAudioChannels(value) {
    const channels = Number(value || 0);
    if (channels === 1) {
      return "1.0";
    }
    if (channels === 2) {
      return "2.0";
    }
    if (channels === 6) {
      return "5.1";
    }
    if (channels === 8) {
      return "7.1";
    }
    return channels > 0 ? String(channels) : "";
  }

  function normalizeCodecLabel(value) {
    const codec = String(value || "").trim().toLowerCase();
    if (!codec) {
      return "";
    }
    if (codec.includes("truehd")) {
      return "TrueHD";
    }
    if (codec.includes("eac3") || codec.includes("e-ac-3") || codec.includes("ddp")) {
      return "EAC3";
    }
    if (codec.includes("ac3")) {
      return "AC3";
    }
    if (codec.includes("aac")) {
      return "AAC";
    }
    if (codec.includes("dts")) {
      return "DTS";
    }
    if (codec.includes("flac")) {
      return "FLAC";
    }
    return codec.toUpperCase();
  }

  function collectMediaStreams(item) {
    const streams = [];
    if (Array.isArray(item?.MediaStreams)) {
      streams.push(...item.MediaStreams);
    }
    if (Array.isArray(item?.MediaSources)) {
      item.MediaSources.forEach((source) => {
        if (Array.isArray(source?.MediaStreams)) {
          streams.push(...source.MediaStreams);
        }
      });
    }
    return streams;
  }

  function getVideoRank(item) {
    const text = [
      item?.Name,
      item?.DisplayTitle,
      item?.Path,
      ...(Array.isArray(item?.MediaSources) ? item.MediaSources.flatMap((source) => [source?.Name, source?.Path, source?.Container]) : []),
      ...collectMediaStreams(item).flatMap((stream) => [stream?.DisplayTitle, stream?.Codec, stream?.Title])
    ]
      .join(" ")
      .toLowerCase();
    const streams = collectMediaStreams(item);
    const dimensions = [
      Number(item?.Width || 0),
      Number(item?.Height || 0),
      ...streams.flatMap((stream) => [Number(stream?.Width || 0), Number(stream?.Height || 0)]),
      ...(Array.isArray(item?.MediaSources) ? item.MediaSources.flatMap((source) => [Number(source?.Width || 0), Number(source?.Height || 0)]) : [])
    ];
    const maxDimension = Math.max(0, ...dimensions.filter((value) => Number.isFinite(value)));
    if (maxDimension >= 3840 || text.includes("4k") || text.includes("2160")) {
      return 3;
    }
    if (maxDimension >= 1920 || text.includes("1080")) {
      return 2;
    }
    if (maxDimension >= 1280 || text.includes("720")) {
      return 1;
    }
    return 0;
  }

  function buildQualityTag(items) {
    const rank = Math.max(0, ...(items || []).map((item) => getVideoRank(item)));
    if (rank >= 3) {
      return { label: "4K", type: "quality" };
    }
    if (rank === 2) {
      return { label: "1080P", type: "quality" };
    }
    if (rank === 1) {
      return { label: "720P", type: "quality" };
    }
    return null;
  }

  function buildAudioTag(items) {
    const candidates = [];
    (items || []).forEach((item) => {
      collectMediaStreams(item)
        .filter((stream) => String(stream?.Type || "").toLowerCase() === "audio")
        .forEach((stream) => {
          const codec = normalizeCodecLabel(stream?.Codec || stream?.DisplayTitle || "");
          if (!codec) {
            return;
          }
          const channels = normalizeAudioChannels(stream?.Channels);
          const priority = ["TrueHD", "EAC3", "DTS", "AC3", "AAC", "FLAC"].indexOf(codec);
          candidates.push({
            label: channels ? `${codec} ${channels}` : codec,
            codec,
            channels: Number(stream?.Channels || 0),
            priority: priority === -1 ? 99 : priority
          });
        });
    });
    candidates.sort((a, b) => a.priority - b.priority || b.channels - a.channels);
    return candidates[0] ? { label: candidates[0].label, type: "audio" } : null;
  }

  function stripSeasonTitle(value) {
    return String(value || "")
      .replace(/\s*[-–—]\s*第\s*\d+\s*季\s*$/i, "")
      .replace(/\s*[-–—]\s*season\s*\d+\s*$/i, "")
      .replace(/\s*[-–—]\s*s\d+\s*$/i, "")
      .replace(/\s*第\s*\d+\s*季\s*$/i, "")
      .trim();
  }

  function getSeriesTitleFromItem(item) {
    const type = String(item?.Type || "").toLowerCase();
    const seriesName = String(item?.SeriesName || "").trim();
    if (seriesName) {
      return seriesName;
    }
    if (type === "season") {
      return stripSeasonTitle(item?.Name || "") || String(item?.Name || "").trim();
    }
    return String(item?.Name || "").trim();
  }

  function getAggregateKey(item) {
    const type = String(item?.Type || "").toLowerCase();
    if (type === "movie") {
      return `movie:${String(item?.Id || normalizeSearchKey(item?.Name || "")).trim()}`;
    }
    const seriesId = String(item?.SeriesId || (type === "series" ? item?.Id : "") || "").trim();
    if (seriesId) {
      return `series:${seriesId}`;
    }
    return `series-title:${normalizeSearchKey(getSeriesTitleFromItem(item))}`;
  }

  function buildAggregateGroups(scoredRows) {
    const groups = new Map();
    (scoredRows || []).forEach((row) => {
      const key = getAggregateKey(row.item);
      if (!key) {
        return;
      }
      const existing = groups.get(key) || {
        key,
        score: 0,
        title: getSeriesTitleFromItem(row.item),
        items: []
      };
      existing.score = Math.max(existing.score, Number(row.score || 0));
      existing.items.push(row.item);
      if (!existing.title || String(row.item?.Type || "").toLowerCase() === "series") {
        existing.title = getSeriesTitleFromItem(row.item);
      }
      groups.set(key, existing);
    });
    return Array.from(groups.values()).sort((a, b) => b.score - a.score);
  }

  function pickBestGroupItem(group, preferredType = "") {
    const targetType = String(preferredType || "").toLowerCase();
    if (targetType) {
      const found = group.items.find((item) => String(item?.Type || "").toLowerCase() === targetType);
      if (found) {
        return found;
      }
    }
    return group.items.find((item) => String(item?.Type || "").toLowerCase() === "series") ||
      group.items.find((item) => String(item?.Type || "").toLowerCase() === "season") ||
      group.items.find((item) => String(item?.Type || "").toLowerCase() === "episode") ||
      group.items[0] ||
      null;
  }

  async function fetchItemDetail(itemId) {
    const id = String(itemId || "").trim();
    if (!id) {
      return null;
    }
    if (state.itemDetailCache.has(id)) {
      return state.itemDetailCache.get(id);
    }
    try {
      const detail = await embyFetch(`/Items/${encodeURIComponent(id)}?Fields=${encodeURIComponent(SEARCH_FIELDS)}`);
      state.itemDetailCache.set(id, detail || null);
      return detail || null;
    } catch {
      state.itemDetailCache.set(id, null);
      return null;
    }
  }

  async function fetchSeriesSeasons(seriesId) {
    const id = String(seriesId || "").trim();
    if (!id) {
      return [];
    }
    if (state.seriesSeasonCache.has(id)) {
      return state.seriesSeasonCache.get(id);
    }
    try {
      const result = await embyFetch(
        `/Shows/${encodeURIComponent(id)}/Seasons?Fields=ChildCount,RecursiveItemCount,IndexNumber,ProductionYear,PremiereDate,ImageTags,Overview,SeriesId,ParentId&Limit=100`
      );
      const rows = Array.isArray(result?.Items) ? result.Items : [];
      state.seriesSeasonCache.set(id, rows);
      return rows;
    } catch {
      state.seriesSeasonCache.set(id, []);
      return [];
    }
  }

  async function fetchSeriesEpisodes(seriesId) {
    const id = String(seriesId || "").trim();
    if (!id) {
      return [];
    }
    if (state.seriesEpisodeCache.has(id)) {
      return state.seriesEpisodeCache.get(id);
    }
    try {
      const result = await embyFetch(
        `/Shows/${encodeURIComponent(id)}/Episodes?Fields=SeasonId,ParentId,SeriesId,ParentIndexNumber,IndexNumber,ProductionYear,PremiereDate,MediaStreams,MediaSources,Width,Height,ImageTags,Overview&Limit=1000`
      );
      const rows = Array.isArray(result?.Items) ? result.Items : [];
      state.seriesEpisodeCache.set(id, rows);
      return rows;
    } catch {
      state.seriesEpisodeCache.set(id, []);
      return [];
    }
  }

  function buildSeasonTags(seasons, episodes) {
    const episodeCounts = new Map();
    (episodes || []).forEach((episode) => {
      const seasonKey = String(episode?.SeasonId || episode?.ParentId || episode?.ParentIndexNumber || "").trim();
      if (!seasonKey) {
        return;
      }
      episodeCounts.set(seasonKey, (episodeCounts.get(seasonKey) || 0) + 1);
    });
    return (seasons || [])
      .map((season, index) => {
        const seasonNo = Number(season?.IndexNumber || index + 1);
        const seasonKey = String(season?.Id || season?.SeasonId || seasonNo || "").trim();
        const count = Number(season?.ChildCount || season?.RecursiveItemCount || episodeCounts.get(seasonKey) || episodeCounts.get(String(seasonNo)) || 0);
        if (!seasonNo || !count) {
          return null;
        }
        return { label: `第${seasonNo}季: ${count}集`, type: "season" };
      })
      .filter(Boolean);
  }

  function getEarliestYear(items) {
    const years = (items || [])
      .map((item) => Number(getItemYear(item)))
      .filter((year) => year >= 1900 && year <= 2100)
      .sort((a, b) => a - b);
    return years[0] ? String(years[0]) : "";
  }

  function pickPosterSource(items) {
    return (items || []).find((item) => item?.ImageTags?.Primary) || (items || []).find((item) => item?.Id) || null;
  }

  async function enrichSearchGroup(group) {
    const best = pickBestGroupItem(group);
    const bestType = String(best?.Type || "").toLowerCase();
    const isSeriesLike = bestType !== "movie";
    let seriesId = String(
      group.items.find((item) => String(item?.Type || "").toLowerCase() === "series")?.Id ||
      group.items.find((item) => item?.SeriesId)?.SeriesId ||
      ""
    ).trim();
    const seriesDetail = seriesId ? await fetchItemDetail(seriesId) : null;
    if (!seriesId && bestType === "series") {
      seriesId = String(best?.Id || "").trim();
    }
    const detail = !isSeriesLike && best?.Id ? await fetchItemDetail(best.Id) : null;
    const seasons = isSeriesLike && seriesId ? await fetchSeriesSeasons(seriesId) : group.items.filter((item) => String(item?.Type || "").toLowerCase() === "season");
    const episodes = isSeriesLike && seriesId ? await fetchSeriesEpisodes(seriesId) : group.items.filter((item) => String(item?.Type || "").toLowerCase() === "episode");
    const seriesSource = seriesDetail || pickBestGroupItem(group, "series") || best;
    const technicalItems = isSeriesLike ? [...episodes, ...group.items] : [detail || best, ...group.items];
    const seasonTags = isSeriesLike ? buildSeasonTags(seasons, episodes) : [];
    const qualityTag = buildQualityTag(technicalItems);
    const audioTag = buildAudioTag(technicalItems);
    const posterSource = pickPosterSource([seriesSource, ...seasons, ...group.items, ...episodes]);

    return {
      id: String(seriesId || seriesSource?.Id || best?.Id || ""),
      title: isSeriesLike ? (String(seriesSource?.Name || group.title || "").trim() || getSeriesTitleFromItem(best)) : getItemTitle(detail || best),
      year: getItemYear(seriesSource) || getEarliestYear([...seasons, ...episodes, ...group.items]),
      overview: String(seriesSource?.Overview || best?.Overview || detail?.Overview || "").trim(),
      posterUrl: buildGlobalSearchPosterUrl(posterSource),
      tags: [...seasonTags, qualityTag, audioTag].filter(Boolean),
      raw: seriesSource || detail || best
    };
  }

  async function fetchDirectSearch(query) {
    const params = new URLSearchParams({
      Recursive: "true",
      IncludeItemTypes: SEARCH_TYPES,
      SearchTerm: query,
      Fields: SEARCH_FIELDS,
      Limit: String(SEARCH_LIMIT)
    });
    const result = await embyFetch(`/Items?${params.toString()}`);
    return Array.isArray(result?.Items) ? result.Items : [];
  }

  async function fetchSearchIndex() {
    if (state.indexItems) {
      return state.indexItems;
    }
    if (!state.indexPromise) {
      const params = new URLSearchParams({
        Recursive: "true",
        IncludeItemTypes: INDEX_SEARCH_TYPES,
        Fields: SEARCH_FIELDS,
        SortBy: "SortName",
        SortOrder: "Ascending",
        Limit: String(INDEX_LIMIT)
      });
      state.indexPromise = embyFetch(`/Items?${params.toString()}`)
        .then((result) => {
          state.indexItems = Array.isArray(result?.Items) ? result.Items : [];
          return state.indexItems;
        })
        .catch((error) => {
          state.indexPromise = null;
          throw error;
        });
    }
    return state.indexPromise;
  }

  async function runSearch(query) {
    const directPromise = fetchDirectSearch(query).catch(() => []);
    const needsIndex = isLikelyPinyinQuery(query);
    const indexPromise = needsIndex ? fetchSearchIndex().catch(() => []) : Promise.resolve([]);
    const [directRows, indexRows] = await Promise.all([directPromise, indexPromise]);
    const scoredRows = dedupeSearchItems([...directRows, ...indexRows])
      .map((item) => ({ item, score: scoreSearchItem(item, query) }))
      .filter((row) => row.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, SEARCH_LIMIT);
    const groups = buildAggregateGroups(scoredRows);
    const displayGroups = groups.length && isStrongSearchMatch(groups[0])
      ? groups.slice(0, 1)
      : groups.slice(0, DISPLAY_LIMIT);
    return Promise.all(displayGroups.map((group) => enrichSearchGroup(group)));
  }

  function renderEmptyState() {
    refs.results.innerHTML = `
      <div class="global-search-empty">
        <div class="global-search-empty-icon">▰</div>
        <p>全局搜索 Emby 媒体库</p>
      </div>
    `;
  }

  function renderLoadingState() {
    refs.results.innerHTML = `
      <div class="global-search-loading">
        <span></span>
        <p>正在搜索媒体库...</p>
      </div>
    `;
  }

  function renderMessage(message) {
    refs.results.innerHTML = `<div class="global-search-message">${escapeHtml(message)}</div>`;
  }

  function buildSearchResultCard(result, index) {
    const tags = result.tags.length
      ? `<div class="global-search-result-tags">${result.tags
          .map((tag) => {
            const label = typeof tag === "string" ? tag : tag?.label;
            const type = typeof tag === "string" ? "" : tag?.type;
            return `<span class="${type ? `tag-${escapeHtml(type)}` : ""}">${escapeHtml(label)}</span>`;
          })
          .join("")}</div>`
      : "";
    const year = result.year ? `<span class="global-search-result-year">${escapeHtml(result.year)}</span>` : "";
    const poster = result.posterUrl
      ? `<img src="${escapeHtml(result.posterUrl)}" alt="${escapeHtml(result.title)}" loading="lazy">`
      : "";
    return `
      <button class="global-search-result-card${index === state.selectedIndex ? " active" : ""}${poster ? "" : " no-poster"}" type="button" data-global-search-index="${index}" aria-selected="${index === state.selectedIndex ? "true" : "false"}">
        <div class="global-search-result-poster">
          ${poster}
          <div class="global-search-poster-fallback">MEDIA</div>
        </div>
        <div class="global-search-result-copy">
          <div class="global-search-result-title-row">
            <strong>${escapeHtml(result.title)}</strong>
            ${year}
          </div>
          ${tags}
          <p>${escapeHtml(result.overview || "暂无简介。")}</p>
        </div>
      </button>
    `;
  }

  function bindPosterFallbacks() {
    refs.results.querySelectorAll(".global-search-result-card img").forEach((img) => {
      if (!(img instanceof HTMLImageElement)) {
        return;
      }
      img.addEventListener("error", () => {
        img.closest(".global-search-result-card")?.classList.add("no-poster");
      }, { once: true });
    });
  }

  function renderResults() {
    if (!state.query) {
      renderEmptyState();
      return;
    }
    if (state.loading) {
      renderLoadingState();
      return;
    }
    if (state.error) {
      renderMessage(state.error);
      return;
    }
    if (!state.results.length) {
      renderMessage("没有找到相关媒体");
      return;
    }
    refs.results.innerHTML = state.results.map((result, index) => buildSearchResultCard(result, index)).join("");
    bindPosterFallbacks();
  }

  function setSelectedIndex(nextIndex) {
    if (!state.results.length) {
      state.selectedIndex = 0;
      return;
    }
    state.selectedIndex = (nextIndex + state.results.length) % state.results.length;
    renderResults();
    refs.results
      .querySelector(`[data-global-search-index="${state.selectedIndex}"]`)
      ?.scrollIntoView({ block: "nearest" });
  }

  async function executeSearch(query) {
    const trimmed = String(query || "").trim();
    state.query = trimmed;
    state.error = "";
    state.results = [];
    state.selectedIndex = 0;
    state.searchSeq += 1;
    const seq = state.searchSeq;
    if (!trimmed) {
      state.loading = false;
      renderResults();
      return;
    }
    if (!appState.config.serverUrl || !appState.config.apiKey) {
      state.loading = false;
      state.error = "请先在系统设置中连接 Emby。";
      renderResults();
      return;
    }
    state.loading = true;
    renderResults();
    try {
      const results = await runSearch(trimmed);
      if (seq !== state.searchSeq) {
        return;
      }
      state.results = results;
      state.error = "";
    } catch (error) {
      if (seq !== state.searchSeq) {
        return;
      }
      state.results = [];
      state.error = error?.message || "搜索失败，请稍后重试。";
    } finally {
      if (seq === state.searchSeq) {
        state.loading = false;
        renderResults();
      }
    }
  }

  function scheduleSearch() {
    window.clearTimeout(state.debounceTimer);
    state.debounceTimer = window.setTimeout(() => {
      executeSearch(refs.input?.value || "");
    }, SEARCH_DEBOUNCE_MS);
  }

  function buildItemDetailUrl(result) {
    const serverUrl = String(appState?.config?.serverUrl || "").trim();
    if (!serverUrl || !result?.id) {
      return "";
    }
    const embyBase = serverUrl.replace(/\/emby$/i, "").replace(/\/$/, "");
    return `${embyBase}/web/index.html#!/item?id=${encodeURIComponent(result.id)}`;
  }

  function openResult(index = state.selectedIndex) {
    const result = state.results[index];
    if (!result) {
      return;
    }
    const detailUrl = buildItemDetailUrl(result);
    close();
    if (detailUrl) {
      window.open(detailUrl, "_blank", "noopener");
    }
  }

  function open() {
    if (!state.mounted) {
      mount();
    }
    state.open = true;
    state.query = "";
    state.results = [];
    state.error = "";
    state.loading = false;
    state.selectedIndex = 0;
    state.searchSeq += 1;
    window.clearTimeout(state.debounceTimer);
    refs.modal.hidden = false;
    document.body.classList.add("global-search-open");
    refs.input.value = "";
    renderResults();
    window.setTimeout(() => refs.input?.focus(), 0);
  }

  function close() {
    if (!state.mounted || !state.open) {
      return;
    }
    state.open = false;
    state.searchSeq += 1;
    window.clearTimeout(state.debounceTimer);
    refs.modal.hidden = true;
    document.body.classList.remove("global-search-open");
    elements.sidebarGlobalSearchInput?.blur();
  }

  function handleKeydown(event) {
    const isShortcut = (event.metaKey || event.ctrlKey) && String(event.key || "").toLowerCase() === "k";
    if (isShortcut) {
      event.preventDefault();
      open();
      return;
    }
    if (!state.open) {
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      close();
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setSelectedIndex(state.selectedIndex + 1);
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setSelectedIndex(state.selectedIndex - 1);
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      openResult();
    }
  }

  function mount() {
    if (state.mounted) {
      return;
    }
    const modal = document.createElement("div");
    modal.id = "global-search-modal";
    modal.className = "global-search-modal";
    modal.hidden = true;
    modal.innerHTML = `
      <div class="global-search-panel" role="dialog" aria-modal="true" aria-label="全局搜索">
        <div class="global-search-input-row">
          <span class="global-search-input-icon">⌕</span>
          <input id="global-search-input" type="search" placeholder="输入影视名称、拼音首字母搜索..." autocomplete="off">
          <button class="global-search-esc" type="button" aria-label="关闭全局搜索">ESC</button>
        </div>
        <div class="global-search-results" role="listbox"></div>
      </div>
    `;
    document.body.appendChild(modal);
    refs.modal = modal;
    refs.panel = modal.querySelector(".global-search-panel");
    refs.input = modal.querySelector("#global-search-input");
    refs.results = modal.querySelector(".global-search-results");

    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        close();
      }
    });
    refs.input?.addEventListener("input", scheduleSearch);
    refs.results?.addEventListener("click", (event) => {
      const button = event.target instanceof Element ? event.target.closest("[data-global-search-index]") : null;
      if (!(button instanceof HTMLButtonElement)) {
        return;
      }
      const index = Number(button.dataset.globalSearchIndex || 0);
      openResult(index);
    });
    modal.querySelector(".global-search-esc")?.addEventListener("click", close);
    document.addEventListener("keydown", handleKeydown);
    state.mounted = true;
  }

  return {
    mount,
    open,
    close
  };
}

const globalSearchModal = GlobalSearchModal();

function initEvents() {
  globalSearchModal.mount();
  elements.sidebarGlobalSearchTrigger?.addEventListener("click", (event) => {
    event.preventDefault();
    globalSearchModal.open();
  });
  elements.sidebarGlobalSearchTrigger?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      globalSearchModal.open();
    }
  });
  elements.sidebarGlobalSearchInput?.addEventListener("focus", () => {
    globalSearchModal.open();
  });

  elements.navItems.forEach((item) => {
    item.addEventListener("click", () => {
      if (!item.dataset.view) {
        return;
      }
      switchView(item.dataset.view);
    });
  });

  elements.userFilterButtons.forEach((button) => {
    button.addEventListener("click", () => {
      appState.userFilter = button.dataset.userFilter;
      elements.userFilterButtons.forEach((chip) => chip.classList.toggle("active", chip === button));
      renderUsers();
    });
  });

  elements.userSearch.addEventListener("input", () => {
    appState.userSearch = elements.userSearch.value;
    renderUsers();
  });

  elements.userCenterSearch?.addEventListener("input", () => {
    appState.userCenterSearch = elements.userCenterSearch.value;
    renderUserCenter();
  });

  elements.userCenterSort?.addEventListener("change", () => {
    appState.userCenterSort = elements.userCenterSort.value;
    renderUserCenter();
  });

  elements.logSearch.addEventListener("input", () => {
    appState.logSearch = elements.logSearch.value;
    renderLogs();
  });

  elements.logUserFilter?.addEventListener("change", () => {
    renderLogs();
  });

  elements.logQueryBtn?.addEventListener("click", () => {
    appState.logSearch = elements.logSearch.value;
    renderLogs();
  });

  elements.connectBtn?.addEventListener("click", async () => {
    applyConfigFromInputs();
    await loadEmbyData();
  });

  elements.diagnoseBtn?.addEventListener("click", async () => {
    await runConnectionDiagnosis();
  });

  elements.disconnectBtn?.addEventListener("click", () => {
    clearConfig();
  });
  elements.tmdbEnabled?.addEventListener("change", () => {
    refreshTmdbUiState();
  });
  elements.tmdbToken?.addEventListener("input", () => {
    refreshTmdbUiState();
  });

  elements.ucInviteManageBtn?.addEventListener("click", () => {
    openUserCenterInviteManageModal();
  });
  elements.ucGenerateInviteBtn?.addEventListener("click", () => {
    openUserCenterInviteModal();
  });
  elements.ucInvitePresetButtons?.forEach((button) => {
    button.addEventListener("click", () => {
      appState.invitePresetDays = button.dataset.days || "30";
      syncInvitePresetButtons();
    });
  });
  elements.ucInviteCustomInc?.addEventListener("click", () => {
    const current = Number(elements.ucInviteCustomDays?.value || 0);
    const next = Math.min(3650, Math.max(1, Math.floor(current || 0) + 1));
    if (elements.ucInviteCustomDays) {
      elements.ucInviteCustomDays.value = String(next);
    }
  });
  elements.ucInviteCustomDec?.addEventListener("click", () => {
    const current = Number(elements.ucInviteCustomDays?.value || 0);
    const next = Math.max(1, Math.floor(current || 1) - 1);
    if (elements.ucInviteCustomDays) {
      elements.ucInviteCustomDays.value = String(next);
    }
  });
  elements.ucCreateUserBtn?.addEventListener("click", async () => {
    await openCreateUserModal();
  });
  elements.ucCreateUserCloseIconBtn?.addEventListener("click", () => {
    closeCreateUserModal();
  });
  elements.ucCreateCancelBtn?.addEventListener("click", () => {
    closeCreateUserModal();
  });
  elements.ucCreateTabs?.forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.dataset.createTab || "basic";
      switchCreateUserTab(tab);
    });
  });
  elements.ucCreateUsername?.addEventListener("input", () => {
    appState.createUserDraft.username = String(elements.ucCreateUsername?.value || "");
  });
  elements.ucCreateTemplate?.addEventListener("change", () => {
    appState.createUserDraft.template = String(elements.ucCreateTemplate?.value || "default");
  });
  elements.ucCreateSourceUser?.addEventListener("change", () => {
    appState.createUserDraft.sourceUserId = String(elements.ucCreateSourceUser?.value || "");
  });
  elements.ucCreateRefreshSourceUsersBtn?.addEventListener("click", async () => {
    if (!appState.users.length && appState.config.serverUrl && appState.config.apiKey) {
      await loadEmbyData();
    }
    populateCreateUserSourceUsers();
    updateCreateUserFeedback("用户列表已刷新。", "success");
  });
  elements.ucCreateApplyPresetBtn?.addEventListener("click", async () => {
    await applyCreateUserPreset();
  });
  elements.ucCreateEnableAllFolders?.addEventListener("change", () => {
    setCreateUserWhitelistDisabled(Boolean(elements.ucCreateEnableAllFolders?.checked));
  });
  elements.ucCreateResetFolders?.addEventListener("click", () => {
    document.querySelectorAll("[data-create-folder-id]").forEach((input) => {
      if (input instanceof HTMLInputElement) {
        input.checked = false;
      }
    });
  });
  elements.ucCreateRatingList?.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement) || !target.hasAttribute("data-create-rating")) {
      return;
    }
    syncCreateUserRatingsFromChecks();
  });
  elements.ucCreateMaxRating?.addEventListener("change", () => {
    syncCreateUserChecksFromMaxRating();
  });
  elements.ucCreateSaveBtn?.addEventListener("click", async () => {
    await createUserFromModal();
  });
  elements.ucCreateUserModal?.addEventListener("click", (event) => {
    if (event.target === elements.ucCreateUserModal) {
      closeCreateUserModal();
    }
  });
  elements.ucInviteForm?.addEventListener("submit", createUserCenterInvite);
  elements.ucInviteCancelBtn?.addEventListener("click", () => {
    closeUserCenterInviteModal();
  });
  elements.ucInviteManageCloseBtn?.addEventListener("click", () => {
    closeUserCenterInviteManageModal();
  });
  elements.ucInviteManageCloseIconBtn?.addEventListener("click", () => {
    closeUserCenterInviteManageModal();
  });
  elements.ucInviteSelectAll?.addEventListener("change", () => {
    const checked = Boolean(elements.ucInviteSelectAll?.checked);
    document.querySelectorAll("[data-select-invite-id]").forEach((input) => {
      if (input instanceof HTMLInputElement) {
        input.checked = checked;
      }
    });
  });
  elements.ucInviteBulkDeleteBtn?.addEventListener("click", async () => {
    await bulkDeleteSelectedInvites();
  });
  elements.ucInviteManageModal?.addEventListener("click", (event) => {
    if (event.target === elements.ucInviteManageModal) {
      closeUserCenterInviteManageModal();
    }
  });
  elements.ucInviteModal?.addEventListener("click", (event) => {
    if (event.target === elements.ucInviteModal) {
      closeUserCenterInviteModal();
    }
  });
  elements.ucInviteCopyAllBtn?.addEventListener("click", async () => {
    if (!appState.generatedInviteLinks.length) {
      showToast("暂无可复制的邀请链接。", 1000);
      return;
    }
    await copyToClipboardWithToast(appState.generatedInviteLinks.join("\n"), "复制成功");
  });
  elements.ucInviteDoneBtn?.addEventListener("click", () => {
    closeUserCenterInviteResultModal();
  });
  elements.ucInviteResultModal?.addEventListener("click", (event) => {
    if (event.target === elements.ucInviteResultModal) {
      closeUserCenterInviteResultModal();
    }
  });
  elements.ucUserConfigCloseIconBtn?.addEventListener("click", () => {
    closeUserConfigModal();
  });
  elements.ucUserConfigCancelBtn?.addEventListener("click", () => {
    closeUserConfigModal();
  });
  elements.ucUserConfigSaveBtn?.addEventListener("click", async () => {
    await saveUserConfig();
  });
  elements.ucConfigTabs?.forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.dataset.configTab || "basic";
      switchUserConfigTab(tab);
    });
  });
  elements.ucConfigEnableAllFolders?.addEventListener("change", () => {
    setUserConfigWhitelistDisabled(Boolean(elements.ucConfigEnableAllFolders?.checked));
  });
  elements.ucConfigResetFolders?.addEventListener("click", () => {
    document.querySelectorAll("[data-config-folder-id]").forEach((input) => {
      if (input instanceof HTMLInputElement) {
        input.checked = false;
      }
    });
  });
  elements.ucUserConfigModal?.addEventListener("click", (event) => {
    if (event.target === elements.ucUserConfigModal) {
      closeUserConfigModal();
    }
  });

  elements.settingsSaveBtn?.addEventListener("click", saveSettingsConfig);
  elements.qualityRescanBtn?.addEventListener("click", runQualityRescan);
  elements.inviteForm?.addEventListener("submit", createInvite);
  elements.renewalForm?.addEventListener("submit", saveRenewal);
  elements.botSaveBtn?.addEventListener("click", saveBotConfig);
  elements.botTelegramTest?.addEventListener("click", () => sendBotTest("telegram"));
  elements.botWechatTest?.addEventListener("click", () => sendBotTest("wechat"));
  elements.botCopyCallbackUrl?.addEventListener("click", async () => {
    const callbackUrl = getWechatCallbackUrlForBot();
    try {
      await navigator.clipboard.writeText(callbackUrl);
      if (elements.botFeedback) {
        elements.botFeedback.textContent = "回调 URL 已复制";
        elements.botFeedback.classList.add("feedback-success");
      }
    } catch {
      if (elements.botFeedback) {
        elements.botFeedback.textContent = "复制失败，请手动复制 URL";
        elements.botFeedback.classList.remove("feedback-success");
      }
    }
  });

  elements.passwordToggles.forEach((button) => {
    button.innerHTML = EYE_CLOSED_SVG;
    button.dataset.open = "false";
    button.classList.remove("is-open");
    button.addEventListener("click", () => togglePasswordVisibility(button));
  });

  elements.profileMenuBtn?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleProfileMenu();
  });

  elements.profileOpenSettings?.addEventListener("click", () => {
    switchView("settings");
  });

  elements.profileOpenSupport?.addEventListener("click", () => {
    switchView("about-support");
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) {
      return;
    }
    if (target.closest(".profile-menu")) {
      return;
    }
    closeProfileMenu();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeUserCenterInviteModal();
      closeUserCenterInviteManageModal();
      closeUserCenterInviteResultModal();
      closeCreateUserModal();
      closeUserConfigModal();
    }
  });
}

function hydrateInputs() {
  elements.serverUrl.value = appState.config.serverUrl.replace(/\/emby$/i, "");
  elements.apiKey.value = appState.config.apiKey;
  if (elements.tmdbEnabled) {
    elements.tmdbEnabled.checked = Boolean(appState.config.tmdbEnabled);
  }
  if (elements.tmdbToken) {
    elements.tmdbToken.value = appState.config.tmdbToken || "";
  }
  if (elements.tmdbLanguage) {
    elements.tmdbLanguage.value = appState.config.tmdbLanguage || "zh-CN";
  }
  if (elements.tmdbRegion) {
    elements.tmdbRegion.value = appState.config.tmdbRegion || "CN";
  }
  refreshTmdbUiState();
  renderEnvControlledState();
}

initEvents();
hydrateInputs();
renderConnectionState(false, "尚未连接 Emby。你可以先填地址和 API Key。");
const initialView = VIEW_META[appState.activeView] ? appState.activeView : "overview";
switchView(initialView);
renderAll();

if (shouldUseLocalProxy()) {
  setTimeout(() => {
    refreshInviteSyncStatus({ silent: true });
    loadBotConfigFromServer({ silent: true });
    refreshBotWebhookInfo({ silent: true });
    ensureBotWebhookStatusPolling();
  }, 80);
}

if (appState.config.serverUrl && appState.config.apiKey) {
  loadEmbyData();
}

/**
 * Adaptive responsive safety enhancer.
 * - Does NOT modify existing HTML structure/IDs/classes.
 * - Does NOT replace existing handlers/logic.
 * - Adds adaptive drawer navigation for compact/mobile widths.
 */
(function initAdaptiveResponsiveSafetyEnhancer() {
  const FORCE_DESKTOP_LAYOUT = true;
  const mobileQuery = window.matchMedia("(max-width: 768px)");
  const compactQuery = window.matchMedia("(min-width: 769px) and (max-width: 1366px)");
  const primaryTabViews = new Set(["overview", "workorders", "user-center"]);
  let mounted = false;
  let drawerHideTimer = null;
  let currentMode = "desktop";

  const ui = {
    bottomBar: null,
    overlay: null,
    drawer: null,
    drawerList: null,
    tabButtons: new Map(),
    topbarMenuBtn: null
  };

  function getResponsiveMode() {
    if (FORCE_DESKTOP_LAYOUT) {
      return "desktop";
    }
    if (mobileQuery.matches) {
      return "mobile";
    }
    if (compactQuery.matches) {
      return "compact";
    }
    return "desktop";
  }

  function isDrawerMode() {
    return currentMode === "mobile" || currentMode === "compact";
  }

  function getSidebarNavRoot() {
    return document.querySelector(".sidebar .nav");
  }

  function getFrozenNavbarActions() {
    return document.querySelector(".frozen-navbar-actions");
  }

  function getNavItemByView(view) {
    if (!view) {
      return null;
    }
    return document.querySelector(`.sidebar .nav-item[data-view="${view}"]`);
  }

  function getActiveNavView() {
    const active = document.querySelector(".sidebar .nav-item.active");
    return active?.dataset?.view || "";
  }

  function activateExistingView(view) {
    const navItem = getNavItemByView(view);
    if (!navItem) {
      return;
    }
    navItem.click();
  }

  function ensureTopbarMenuTrigger() {
    if (ui.topbarMenuBtn && document.body.contains(ui.topbarMenuBtn)) {
      return ui.topbarMenuBtn;
    }
    const host = getFrozenNavbarActions();
    if (!host) {
      return null;
    }
    const button = document.createElement("button");
    button.type = "button";
    button.className = "nav-icon-btn adaptive-menu-trigger";
    button.setAttribute("aria-label", "打开菜单");
    button.setAttribute("aria-expanded", "false");
    button.textContent = "☰";
    button.hidden = true;
    button.addEventListener("click", () => {
      const open = Boolean(ui.drawer && ui.drawer.classList.contains("open"));
      setDrawerOpen(!open);
    });
    host.prepend(button);
    ui.topbarMenuBtn = button;
    return button;
  }

  function updateBottomTabActiveState() {
    if (!mounted) {
      return;
    }
    const activeView = getActiveNavView();
    let activeTabKey = "data";
    if (activeView === "workorders") {
      activeTabKey = "workorders";
    } else if (activeView === "user-center") {
      activeTabKey = "user";
    } else if (activeView && !["overview", "content-ranking", "data-insights", "users", "missing", "dedup", "logs"].includes(activeView)) {
      activeTabKey = "menu";
    }

    ui.tabButtons.forEach((button, key) => {
      button.classList.toggle("active", key === activeTabKey);
      button.setAttribute("aria-selected", key === activeTabKey ? "true" : "false");
    });
  }

  function setDrawerOpen(open) {
    if (!mounted || !ui.overlay || !ui.drawer) {
      return;
    }
    if (!isDrawerMode() && open) {
      return;
    }

    if (drawerHideTimer) {
      clearTimeout(drawerHideTimer);
      drawerHideTimer = null;
    }

    if (open) {
      ui.overlay.hidden = false;
      ui.drawer.hidden = false;
      document.body.classList.add("mobile-drawer-open");
      document.body.classList.add("adaptive-drawer-open");
      if (ui.topbarMenuBtn) {
        ui.topbarMenuBtn.setAttribute("aria-expanded", "true");
      }
      requestAnimationFrame(() => {
        ui.overlay?.classList.add("open");
        ui.drawer?.classList.add("open");
      });
      return;
    }

    ui.overlay.classList.remove("open");
    ui.drawer.classList.remove("open");
    document.body.classList.remove("mobile-drawer-open");
    document.body.classList.remove("adaptive-drawer-open");
    if (ui.topbarMenuBtn) {
      ui.topbarMenuBtn.setAttribute("aria-expanded", "false");
    }
    drawerHideTimer = setTimeout(() => {
      if (ui.overlay) {
        ui.overlay.hidden = true;
      }
      if (ui.drawer) {
        ui.drawer.hidden = true;
      }
    }, 220);
  }

  function createBottomTabs() {
    const bottomBar = document.createElement("nav");
    bottomBar.className = "mobile-bottom-tabbar";
    bottomBar.setAttribute("aria-label", "Adaptive bottom navigation");
    bottomBar.hidden = true;

    const tabs = [
      { key: "data", label: "数据", icon: "📈", view: "overview" },
      { key: "workorders", label: "工单", icon: "🎬", view: "workorders" },
      { key: "user", label: "用户", icon: "👥", view: "user-center" },
      { key: "menu", label: "菜单", icon: "☰", view: "" }
    ];

    tabs.forEach((tab) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "mobile-tab-btn";
      button.setAttribute("data-mobile-tab", tab.key);
      button.setAttribute("aria-selected", "false");
      button.innerHTML = `<span class="mobile-tab-icon">${tab.icon}</span><span class="mobile-tab-label">${tab.label}</span>`;

      button.addEventListener("click", () => {
        if (tab.key === "menu") {
          setDrawerOpen(true);
          return;
        }
        setDrawerOpen(false);
        activateExistingView(tab.view);
        updateBottomTabActiveState();
      });

      bottomBar.appendChild(button);
      ui.tabButtons.set(tab.key, button);
    });

    document.body.appendChild(bottomBar);
    ui.bottomBar = bottomBar;
  }

  function createDrawerShell() {
    const overlay = document.createElement("div");
    overlay.className = "mobile-menu-overlay";
    overlay.hidden = true;
    overlay.addEventListener("click", () => setDrawerOpen(false));

    const drawer = document.createElement("aside");
    drawer.className = "mobile-menu-drawer";
    drawer.hidden = true;

    const sheet = document.createElement("div");
    sheet.className = "mobile-menu-sheet";

    const handle = document.createElement("div");
    handle.className = "mobile-menu-handle";

    const list = document.createElement("div");
    list.className = "mobile-menu-list";

    sheet.appendChild(handle);
    sheet.appendChild(list);
    drawer.appendChild(sheet);

    document.body.appendChild(overlay);
    document.body.appendChild(drawer);

    ui.overlay = overlay;
    ui.drawer = drawer;
    ui.drawerList = list;
  }

  function buildDrawerItems() {
    if (!ui.drawerList) {
      return;
    }

    const navRoot = getSidebarNavRoot();
    if (!navRoot) {
      return;
    }

    const fragment = document.createDocumentFragment();
    let section = null;

    Array.from(navRoot.children).forEach((node) => {
      if (!(node instanceof HTMLElement)) {
        return;
      }

      if (node.classList.contains("nav-group-title")) {
        section = document.createElement("section");
        section.className = "mobile-menu-section";

        const heading = document.createElement("h4");
        heading.className = "mobile-menu-section-title";
        heading.textContent = node.textContent?.trim() || "菜单";
        section.appendChild(heading);
        fragment.appendChild(section);
        return;
      }

      if (!node.classList.contains("nav-item")) {
        return;
      }

      const view = node.dataset.view || "";
      if (!view || primaryTabViews.has(view)) {
        return;
      }

      if (!section) {
        section = document.createElement("section");
        section.className = "mobile-menu-section";
        fragment.appendChild(section);
      }

      const menuButton = document.createElement("button");
      menuButton.type = "button";
      menuButton.className = "mobile-menu-item";
      menuButton.setAttribute("data-mobile-menu-view", view);

      const iconText = node.querySelector(".nav-icon")?.textContent?.trim() || "•";
      const labelText = node.textContent?.replace(iconText, "").trim() || view;
      menuButton.innerHTML = `
        <span class="mobile-menu-item-icon">${iconText}</span>
        <span class="mobile-menu-item-label">${labelText}</span>
        <span class="mobile-menu-item-arrow">›</span>
      `;

      menuButton.addEventListener("click", () => {
        setDrawerOpen(false);
        node.click();
        updateBottomTabActiveState();
      });

      section.appendChild(menuButton);
    });

    const actionSection = document.createElement("section");
    actionSection.className = "mobile-menu-section mobile-menu-actions";

    const closeButton = document.createElement("button");
    closeButton.type = "button";
    closeButton.className = "mobile-menu-secondary-btn";
    closeButton.textContent = "关闭菜单";
    closeButton.addEventListener("click", () => setDrawerOpen(false));
    actionSection.appendChild(closeButton);

    const exitSource = document.querySelector(".sidebar-exit");
    if (exitSource instanceof HTMLButtonElement) {
      const exitButton = document.createElement("button");
      exitButton.type = "button";
      exitButton.className = "mobile-menu-exit-btn";
      exitButton.textContent = exitSource.textContent?.trim() || "退出";
      exitButton.addEventListener("click", () => exitSource.click());
      actionSection.appendChild(exitButton);
    }

    fragment.appendChild(actionSection);

    ui.drawerList.replaceChildren(fragment);
  }

  function bindStateSync() {
    const navRoot = getSidebarNavRoot();
    if (!navRoot) {
      return;
    }

    const observer = new MutationObserver((records) => {
      const hasClassMutation = records.some((record) => record.attributeName === "class");
      if (hasClassMutation) {
        updateBottomTabActiveState();
      }
    });
    observer.observe(navRoot, { subtree: true, attributes: true, attributeFilter: ["class"] });

    navRoot.addEventListener("click", () => {
      if (isDrawerMode()) {
        setDrawerOpen(false);
      }
      setTimeout(updateBottomTabActiveState, 0);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        setDrawerOpen(false);
      }
    });

    document.addEventListener("adaptive:viewchange", () => {
      if (isDrawerMode()) {
        setDrawerOpen(false);
      }
      updateBottomTabActiveState();
    });
  }

  function applyResponsiveMode() {
    currentMode = getResponsiveMode();
    const isMobile = currentMode === "mobile";
    const isCompact = currentMode === "compact";
    document.body.classList.toggle("mobile-app-mode", isMobile);
    document.body.classList.toggle("compact-app-mode", isCompact);
    if (!FORCE_DESKTOP_LAYOUT) {
      ensureTopbarMenuTrigger();
    }

    if (!mounted) {
      return;
    }

    if (ui.topbarMenuBtn) {
      ui.topbarMenuBtn.hidden = FORCE_DESKTOP_LAYOUT || !isCompact;
    }

    if (ui.bottomBar) {
      ui.bottomBar.hidden = FORCE_DESKTOP_LAYOUT || !isMobile;
    }

    if (FORCE_DESKTOP_LAYOUT || !isDrawerMode()) {
      setDrawerOpen(false);
    }

    updateBottomTabActiveState();
  }

  function mount() {
    if (mounted) {
      return;
    }
    createBottomTabs();
    createDrawerShell();
    buildDrawerItems();
    bindStateSync();
    mounted = true;
    applyResponsiveMode();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount, { once: true });
  } else {
    mount();
  }

  if (typeof mobileQuery.addEventListener === "function") {
    mobileQuery.addEventListener("change", applyResponsiveMode);
  } else if (typeof mobileQuery.addListener === "function") {
    mobileQuery.addListener(applyResponsiveMode);
  }

  if (typeof compactQuery.addEventListener === "function") {
    compactQuery.addEventListener("change", applyResponsiveMode);
  } else if (typeof compactQuery.addListener === "function") {
    compactQuery.addListener(applyResponsiveMode);
  }

  window.addEventListener("resize", applyResponsiveMode);
})();

/**
 * Glass overlay state sync (additive only).
 * - Keeps a single body state class for modal/drawer/menu overlay situations.
 */
(function initGlassOverlayStateSync() {
  if (typeof document === "undefined" || !document.body) {
    return;
  }

  const body = document.body;
  const relevantSelector = [
    ".user-center-modal",
    ".global-search-modal",
    ".mobile-menu-overlay",
    ".mobile-menu-drawer"
  ].join(",");

  let rafId = 0;

  function isOpen(el) {
    return Boolean(el && !el.hidden);
  }

  function computeOverlayOpen() {
    const modalOpen = Boolean(document.querySelector(".user-center-modal:not([hidden])"));
    const globalSearchOpen = Boolean(document.querySelector(".global-search-modal:not([hidden])"));
    const drawerOverlayOpen = Boolean(document.querySelector(".mobile-menu-overlay.open:not([hidden])"));
    const drawerPanelOpen = Boolean(document.querySelector(".mobile-menu-drawer.open:not([hidden])"));
    return modalOpen || globalSearchOpen || drawerOverlayOpen || drawerPanelOpen;
  }

  function applyOverlayState() {
    rafId = 0;
    body.classList.toggle("has-glass-overlay", computeOverlayOpen());
  }

  function scheduleApply() {
    if (rafId) {
      return;
    }
    rafId = window.requestAnimationFrame(applyOverlayState);
  }

  const observer = new MutationObserver((records) => {
    let relevant = false;
    for (const record of records) {
      if (record.type === "childList") {
        relevant = true;
        break;
      }
      const target = record.target;
      if (!(target instanceof Element)) {
        continue;
      }
      if (target.matches(relevantSelector) || target.closest(relevantSelector)) {
        relevant = true;
        break;
      }
    }
    if (relevant) {
      scheduleApply();
    }
  });

  observer.observe(body, {
    subtree: true,
    childList: true,
    attributes: true,
    attributeFilter: ["hidden", "class", "style", "aria-expanded"]
  });

  document.addEventListener("click", scheduleApply, true);
  document.addEventListener("keydown", scheduleApply, true);
  document.addEventListener("adaptive:viewchange", scheduleApply);
  window.addEventListener("resize", scheduleApply);
  scheduleApply();
})();

/**
 * Figure-1 skin enhancer (additive only).
 * - No mutation of existing handlers/functions.
 * - Adds visual skin class + realtime playback card.
 */
(function initFigure1SkinEnhancer() {
  if (typeof document === "undefined") {
    return;
  }

  document.body.classList.add("vistamirror-fig1-skin");

  const root = {
    actions: document.querySelector(".frozen-navbar-actions")
  };

  if (!root.actions) {
    return;
  }

  let card = document.getElementById("live-status-card");
  if (!card) {
    card = document.createElement("section");
    card.id = "live-status-card";
    card.className = "live-status-card";
    card.setAttribute("aria-label", "实时播放状态");
    card.innerHTML = `
      <div class="live-status-head">
        <span class="live-status-label"><i class="live-status-dot" aria-hidden="true"></i>实时播放状态</span>
        <span id="live-status-meta" class="live-status-meta">等待数据</span>
      </div>
      <p id="live-status-title" class="live-status-title">当前暂无播放内容</p>
      <p id="live-status-sub" class="live-status-sub">连接 Emby 后自动显示播放器、设备和进度。</p>
      <div class="live-status-progress" role="progressbar" aria-label="播放进度" aria-valuemin="0" aria-valuemax="100">
        <span id="live-status-progress-fill"></span>
      </div>
      <div class="live-status-time">
        <span id="live-status-time-current">00:00</span>
        <span id="live-status-time-total">00:00</span>
      </div>
    `;
    root.actions.prepend(card);
  }

  const els = {
    meta: document.getElementById("live-status-meta"),
    title: document.getElementById("live-status-title"),
    sub: document.getElementById("live-status-sub"),
    fill: document.getElementById("live-status-progress-fill"),
    current: document.getElementById("live-status-time-current"),
    total: document.getElementById("live-status-time-total")
  };

  function toSecondsFromTicks(ticks) {
    const n = Number(ticks);
    if (!Number.isFinite(n) || n <= 0) {
      return 0;
    }
    return Math.floor(n / 10000000);
  }

  function toTimeLabel(seconds) {
    const sec = Math.max(0, Math.floor(Number(seconds) || 0));
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (h > 0) {
      return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
    }
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  }

  function getActiveSession() {
    if (!Array.isArray(appState?.sessions) || appState.sessions.length === 0) {
      return null;
    }

    const playing = appState.sessions.find((session) => {
      const item = session?.NowPlayingItem;
      return Boolean(item && (item.Name || item.Id));
    });

    return playing || null;
  }

  function renderIdle() {
    if (!els.title || !els.sub || !els.meta || !els.fill || !els.current || !els.total) {
      return;
    }
    els.meta.textContent = "等待数据";
    els.title.textContent = "当前暂无播放内容";
    els.sub.textContent = "连接 Emby 后自动显示播放器、设备和进度。";
    els.fill.style.width = "0%";
    els.current.textContent = "00:00";
    els.total.textContent = "00:00";
  }

  function renderLiveStatus() {
    const session = getActiveSession();
    if (!session) {
      renderIdle();
      return;
    }

    const userName = String(session.UserName || session.User?.Name || "未知用户");
    const deviceName = String(session.DeviceName || session.DeviceId || session.Client || "未知设备");
    const clientName = String(session.Client || session.ApplicationVersion || "未知客户端");
    const item = session.NowPlayingItem || {};
    const itemTitle = String(item.Name || item.SeriesName || "未命名媒体");

    const runTimeSeconds = toSecondsFromTicks(item.RunTimeTicks);
    const positionSeconds = toSecondsFromTicks(session.PlayState?.PositionTicks);
    const safeTotal = Math.max(runTimeSeconds, 0);
    const safeCurrent = Math.max(Math.min(positionSeconds, safeTotal || positionSeconds), 0);
    const progress = safeTotal > 0 ? Math.max(0, Math.min(100, (safeCurrent / safeTotal) * 100)) : 0;

    if (els.meta) {
      els.meta.textContent = `${userName} · 在线`;
    }
    if (els.title) {
      els.title.textContent = itemTitle;
    }
    if (els.sub) {
      els.sub.textContent = `${deviceName} · ${clientName}`;
    }
    if (els.fill) {
      els.fill.style.width = `${progress.toFixed(2)}%`;
      const progressWrap = els.fill.parentElement;
      if (progressWrap) {
        progressWrap.setAttribute("aria-valuenow", progress.toFixed(0));
      }
    }
    if (els.current) {
      els.current.textContent = toTimeLabel(safeCurrent);
    }
    if (els.total) {
      els.total.textContent = toTimeLabel(safeTotal);
    }
  }

  renderLiveStatus();
  setInterval(renderLiveStatus, 1000);
})();

/**
 * Figure-2 composite stats module (additive only)
 * - Keep original stats DOM untouched for data pipeline compatibility
 * - Build a visual replica container with same source data
 */
(function initFigure2CompositeStats() {
  if (typeof document === "undefined") {
    return;
  }

  const host = document.getElementById("overview-stats-grid");
  if (!host) {
    return;
  }

  let wrap = document.getElementById("fig2-stats-wrap");
  if (!wrap) {
    wrap = document.createElement("div");
    wrap.id = "fig2-stats-wrap";
    wrap.className = "fig2-stats-wrap";
    wrap.innerHTML = `
      <article class="fig2-stats-card" aria-label="媒体库储量">
        <h3 class="fig2-stats-title">媒体库储量</h3>
        <div class="fig2-stats-row">
          <section class="fig2-metric fig2-metric-movies">
            <span class="fig2-metric-label">电影</span>
            <strong id="fig2-movies-value" class="fig2-metric-value">0</strong>
            <span class="fig2-metric-sub">电影总数</span>
            <i class="fig2-metric-icon" aria-hidden="true">🎞️</i>
          </section>
          <section class="fig2-metric fig2-metric-series">
            <span class="fig2-metric-label">剧集</span>
            <strong id="fig2-series-value" class="fig2-metric-value">0</strong>
            <span class="fig2-metric-sub">电视剧总数</span>
            <i class="fig2-metric-icon" aria-hidden="true">📁</i>
          </section>
          <section class="fig2-metric fig2-metric-episodes">
            <span class="fig2-metric-label">总集数</span>
            <strong id="fig2-episodes-value" class="fig2-metric-value">0</strong>
            <span class="fig2-metric-sub">剧集总数</span>
            <i class="fig2-metric-icon" aria-hidden="true">📚</i>
          </section>
        </div>
      </article>
      <article class="fig2-stats-card" aria-label="核心运营指标">
        <h3 class="fig2-stats-title">核心运营指标</h3>
        <div class="fig2-stats-row">
          <section class="fig2-metric fig2-metric-plays">
            <span class="fig2-metric-label">播放次数</span>
            <strong id="fig2-plays-value" class="fig2-metric-value">0</strong>
            <span class="fig2-metric-sub">日志聚合</span>
            <i class="fig2-metric-icon" aria-hidden="true">▶️</i>
          </section>
          <section class="fig2-metric fig2-metric-viewers">
            <span class="fig2-metric-label">活跃观众</span>
            <strong id="fig2-viewers-value" class="fig2-metric-value">0</strong>
            <span class="fig2-metric-sub">在线会话用户</span>
            <i class="fig2-metric-icon" aria-hidden="true">👥</i>
          </section>
          <section class="fig2-metric fig2-metric-hours">
            <span class="fig2-metric-label">时长（小时）</span>
            <strong id="fig2-hours-value" class="fig2-metric-value">0</strong>
            <span class="fig2-metric-sub">观影累计时长</span>
            <i class="fig2-metric-icon" aria-hidden="true">🕒</i>
          </section>
        </div>
      </article>
    `;
    host.appendChild(wrap);
  }

  const nodes = {
    movies: document.getElementById("fig2-movies-value"),
    series: document.getElementById("fig2-series-value"),
    episodes: document.getElementById("fig2-episodes-value"),
    plays: document.getElementById("fig2-plays-value"),
    viewers: document.getElementById("fig2-viewers-value"),
    hours: document.getElementById("fig2-hours-value"),
    srcMovies: document.getElementById("stat-movies"),
    srcSeries: document.getElementById("stat-series"),
    srcEpisodes: document.getElementById("stat-episodes")
  };

  function parseNumberText(raw) {
    const num = Number(String(raw || "").replace(/,/g, "").trim());
    return Number.isFinite(num) ? num : 0;
  }

  function formatInt(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) {
      return "0";
    }
    return Math.max(0, Math.round(n)).toLocaleString("zh-CN");
  }

  function readPlaybackRowsSafe() {
    try {
      if (typeof buildPlaybackHistoryRows === "function") {
        const rows = buildPlaybackHistoryRows();
        return Array.isArray(rows) ? rows : [];
      }
    } catch {
      // keep fallback below
    }

    const logs = Array.isArray(appState?.logs) ? appState.logs : [];
    return logs.map((log) => ({
      durationMin: 0,
      userName: String(log?.UserName || log?.ByUserName || "")
    }));
  }

  function renderCompositeStats() {
    const movieCount = parseNumberText(nodes.srcMovies?.textContent);
    const seriesCount = parseNumberText(nodes.srcSeries?.textContent);
    const episodeCount = parseNumberText(nodes.srcEpisodes?.textContent);

    const playbackRows = readPlaybackRowsSafe();
    const playCount = playbackRows.length;

    const sessionUsers = new Set(
      (Array.isArray(appState?.sessions) ? appState.sessions : [])
        .map((session) => String(session?.UserName || session?.User?.Name || "").trim())
        .filter(Boolean)
    );
    const activeViewers = sessionUsers.size;

    const totalMinutes = playbackRows.reduce((sum, row) => {
      const minutes = Number(row?.durationMin || 0);
      return sum + (Number.isFinite(minutes) ? Math.max(0, minutes) : 0);
    }, 0);
    const totalHours = totalMinutes / 60;

    if (nodes.movies) {
      nodes.movies.textContent = formatInt(movieCount);
    }
    if (nodes.series) {
      nodes.series.textContent = formatInt(seriesCount);
    }
    if (nodes.episodes) {
      nodes.episodes.textContent = formatInt(episodeCount);
    }
    if (nodes.plays) {
      nodes.plays.textContent = formatInt(playCount);
    }
    if (nodes.viewers) {
      nodes.viewers.textContent = formatInt(activeViewers);
    }
    if (nodes.hours) {
      nodes.hours.textContent = formatInt(totalHours);
    }
  }

  renderCompositeStats();
  setInterval(renderCompositeStats, 1200);
})();

/**
 * Figure-2 dashboard date line enhancer (additive only)
 * - Injects date row under #topbar-title
 * - Keeps existing title/subtitle text unchanged
 */
(function initFigure2TopbarDateLine() {
  if (typeof document === "undefined") {
    return;
  }

  const titleEl = document.getElementById("topbar-title");
  const subtitleEl = document.getElementById("topbar-subtitle");
  if (!titleEl || !subtitleEl || !subtitleEl.parentElement) {
    return;
  }

  const textWrap = subtitleEl.parentElement;
  let dateEl = document.getElementById("fig2-topbar-date");
  if (!dateEl) {
    dateEl = document.createElement("p");
    dateEl.id = "fig2-topbar-date";
    dateEl.className = "fig2-topbar-date";
    textWrap.insertBefore(dateEl, subtitleEl);
  }

  const weekDays = ["星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"];

  function formatNowLine(now) {
    const y = now.getFullYear();
    const m = now.getMonth() + 1;
    const d = now.getDate();
    const w = weekDays[now.getDay()] || "星期";
    return `📅 ${y}年${m}月${d}日 ${w}`;
  }

  function updateDateLine() {
    const now = new Date();
    dateEl.textContent = formatNowLine(now);
  }

  function applyVisibilityByView() {
    const activeView = document.querySelector(".main-content")?.dataset?.activeView || "";
    dateEl.hidden = activeView !== "overview";
  }

  updateDateLine();
  applyVisibilityByView();

  setInterval(updateDateLine, 30 * 1000);

  const mainContent = document.querySelector(".main-content");
  if (mainContent) {
    const observer = new MutationObserver(() => {
      applyVisibilityByView();
    });
    observer.observe(mainContent, {
      attributes: true,
      attributeFilter: ["data-active-view"]
    });
  }
})();

/**
 * Add-on module: My Media Library (additive only)
 * - Mirror Emby UserViews directly (no local category inference)
 * - Keep cover style/content in sync with Emby source
 * - Keep all existing dashboard logic untouched
 */
(function initMyMediaLibraryAddon() {
  if (typeof document === "undefined") {
    return;
  }

  const STATE = {
    mounted: false,
    loading: false,
    lastSyncAt: 0,
    views: []
  };
  const LIBRARY_SYNC_INTERVAL_MS = 60 * 1000;
  function normalizeCollectionType(type) {
    return String(type || "").trim().toLowerCase();
  }

  function collectionTypeLabel(type) {
    switch (normalizeCollectionType(type)) {
      case "movies":
        return "Movies";
      case "tvshows":
      case "series":
        return "Series";
      case "music":
        return "Music";
      case "musicvideos":
        return "Music Videos";
      case "boxsets":
        return "Collections";
      case "books":
        return "Books";
      case "livetv":
        return "Live TV";
      case "playlists":
        return "Playlists";
      case "homevideos":
        return "Home Videos";
      default:
        return "Library";
    }
  }

  function collectionCountLabel(view) {
    const raw = [view?.recursiveItemCount, view?.childCount, view?.itemCount].find((value) => Number.isFinite(Number(value)));
    if (!Number.isFinite(Number(raw))) {
      return "";
    }
    const count = Math.max(0, Number(raw));
    const type = normalizeCollectionType(view?.collectionType);
    const unit = type === "music" ? "首" : type === "books" ? "本" : "部";
    return `${count.toLocaleString("zh-CN")} ${unit}`;
  }

  function buildEmbyViewImageUrl(viewId, imageType, imageTag = "", maxWidth = 600) {
    if (!viewId || !appState?.config?.serverUrl || !appState?.config?.apiKey) {
      return "";
    }
    const safeType = String(imageType || "").trim();
    if (!safeType) {
      return "";
    }
    const safeTag = String(imageTag || "").trim();
    const safeWidth = Math.max(140, Number(maxWidth) || 600);
    const tagQuery = safeTag ? `&tag=${encodeURIComponent(safeTag)}` : "";
    return `${appState.config.serverUrl}/Items/${encodeURIComponent(viewId)}/Images/${safeType}?maxWidth=${safeWidth}&quality=90&api_key=${encodeURIComponent(appState.config.apiKey)}${tagQuery}`;
  }

  function normalizeUserViews(source) {
    const rows = Array.isArray(source?.Items) ? source.Items : Array.isArray(source) ? source : [];
    return rows
      .filter((item) => item?.Id && item?.Name)
      .map((item) => ({
        id: String(item.Id),
        name: String(item.Name || "未命名媒体库"),
        collectionType: normalizeCollectionType(item.CollectionType),
        type: String(item.Type || ""),
        imageTags: item.ImageTags && typeof item.ImageTags === "object" ? item.ImageTags : {},
        backdropImageTags: Array.isArray(item.BackdropImageTags) ? item.BackdropImageTags : [],
        recursiveItemCount: Number(item.RecursiveItemCount),
        childCount: Number(item.ChildCount),
        itemCount: Number(item.ItemCount)
      }));
  }

  function normalizeVirtualFolderViews(source) {
    const rows = Array.isArray(source?.Items) ? source.Items : Array.isArray(source) ? source : [];
    return rows
      .map((item) => ({
        id: String(item?.ItemId || item?.Id || "").trim(),
        name: String(item?.Name || "").trim(),
        collectionType: normalizeCollectionType(item?.CollectionType),
        type: String(item?.Type || ""),
        imageTags: item?.ImageTags && typeof item.ImageTags === "object" ? item.ImageTags : {},
        backdropImageTags: Array.isArray(item?.BackdropImageTags) ? item.BackdropImageTags : [],
        recursiveItemCount: Number(item?.RecursiveItemCount),
        childCount: Number(item?.ChildCount),
        itemCount: Number(item?.ItemCount)
      }))
      .filter((item) => item.id && item.name);
  }

  function buildCoverCandidates(view) {
    const imageTags = view?.imageTags || {};
    const backdropTag = view?.backdropImageTags?.[0] || imageTags.Backdrop || "";
    const candidates = [
      buildEmbyViewImageUrl(view.id, "Primary", imageTags.Primary || "", 640),
      buildEmbyViewImageUrl(view.id, "Thumb", imageTags.Thumb || "", 640),
      buildEmbyViewImageUrl(view.id, "Backdrop", backdropTag, 960),
      buildEmbyViewImageUrl(view.id, "Primary", "", 640),
      buildEmbyViewImageUrl(view.id, "Thumb", "", 640),
      buildEmbyViewImageUrl(view.id, "Backdrop", "", 960)
    ];
    return Array.from(new Set(candidates.filter(Boolean)));
  }

  function buildUserViewPath(view) {
    const id = encodeURIComponent(view.id);
    const collectionType = normalizeCollectionType(view.collectionType);
    if (collectionType === "movies" || collectionType === "boxsets") {
      return `movies.html?topParentId=${id}`;
    }
    if (collectionType === "tvshows" || collectionType === "series") {
      return `tv.html?topParentId=${id}`;
    }
    if (collectionType === "music") {
      return `music.html?topParentId=${id}`;
    }
    if (collectionType === "musicvideos") {
      return `musicvideos.html?topParentId=${id}`;
    }
    if (collectionType === "livetv") {
      return "livetv.html";
    }
    return `item?id=${id}`;
  }

  function buildCardHtml(view) {
    const coverList = buildCoverCandidates(view);
    const hasCover = coverList.length > 0;
    const coverData = coverList.map((url) => encodeURIComponent(url)).join("|");
    const countText = collectionCountLabel(view);
    return `
      <button class="my-library-card${hasCover ? "" : " is-no-image"}" type="button" data-user-view-id="${escapeHtml(view.id)}" aria-label="打开${escapeHtml(view.name)}媒体库">
        ${hasCover ? `<img class="my-library-cover" src="${escapeHtml(coverList[0])}" data-cover-list="${coverData}" data-cover-index="0" alt="${escapeHtml(view.name)}" loading="lazy">` : ""}
        <div class="my-library-copy">
          <strong class="my-library-cn">${escapeHtml(view.name)}</strong>
          <span class="my-library-en">${escapeHtml(collectionTypeLabel(view.collectionType))}</span>
          ${countText ? `<span class="my-library-count">${escapeHtml(countText)}</span>` : ""}
        </div>
      </button>
    `;
  }

  function bindCoverFallbacks(root) {
    root.querySelectorAll(".my-library-cover[data-cover-list]").forEach((img) => {
      if (!(img instanceof HTMLImageElement)) {
        return;
      }
      if (img.dataset.coverBound === "1") {
        return;
      }
      img.dataset.coverBound = "1";
      img.addEventListener("error", () => {
        const list = String(img.dataset.coverList || "")
          .split("|")
          .map((seg) => {
            try {
              return decodeURIComponent(seg);
            } catch {
              return "";
            }
          })
          .filter(Boolean);
        const nextIndex = Number(img.dataset.coverIndex || "0") + 1;
        img.dataset.coverIndex = String(nextIndex);
        if (nextIndex < list.length) {
          img.src = list[nextIndex];
          return;
        }
        img.closest(".my-library-card")?.classList.add("is-no-image");
        img.remove();
      });
    });
  }

  function renderUserViews(views) {
    const grid = document.getElementById("my-library-grid");
    if (!grid) {
      return;
    }
    if (!Array.isArray(views) || !views.length) {
      grid.innerHTML = `<div class="my-library-empty">未读取到可展示的 Emby 媒体库视图。</div>`;
      return;
    }
    grid.innerHTML = views.map((view) => buildCardHtml(view)).join("");
    bindCoverFallbacks(grid);
  }

  function renderUserViewsLoading() {
    const grid = document.getElementById("my-library-grid");
    if (!grid) {
      return;
    }
    grid.innerHTML = `<div class="my-library-empty">正在同步 Emby 媒体库...</div>`;
  }

  function renderUserViewsError(errorMessage = "") {
    const grid = document.getElementById("my-library-grid");
    if (!grid) {
      return;
    }
    const detail = String(errorMessage || "").trim();
    const brief = detail ? `（${escapeHtml(detail).slice(0, 120)}）` : "";
    grid.innerHTML = `<div class="my-library-empty">媒体库同步失败${brief}</div>`;
  }

  async function fetchUserViews() {
    let lastError = null;

    try {
      const result = await embyFetch("/UserViews");
      const normalized = normalizeUserViews(result);
      if (normalized.length > 0) {
        return normalized;
      }
    } catch (error) {
      lastError = error;
    }

    try {
      const fallback = await embyFetch("/Library/VirtualFolders");
      const normalizedFallback = normalizeVirtualFolderViews(fallback);
      if (normalizedFallback.length > 0) {
        return normalizedFallback;
      }
    } catch (error) {
      if (!lastError) {
        lastError = error;
      }
    }

    if (lastError) {
      throw lastError;
    }
    return [];
  }

  function mountModule() {
    if (STATE.mounted) {
      return;
    }

    const overview = document.getElementById("view-overview");
    const firstGrid = overview?.querySelector(":scope > .dashboard-grid");
    if (!overview || !firstGrid) {
      return;
    }

    const panel = document.createElement("section");
    panel.id = "my-media-library-panel";
    panel.className = "my-library-panel";
    panel.innerHTML = `
      <h3 class="my-library-title">我的媒体库</h3>
      <div id="my-library-grid" class="my-library-grid"></div>
    `;

    firstGrid.insertAdjacentElement("afterend", panel);

    panel.addEventListener("click", (event) => {
      const button = event.target instanceof Element ? event.target.closest("[data-user-view-id]") : null;
      if (!(button instanceof HTMLButtonElement)) {
        return;
      }
      const viewId = String(button.getAttribute("data-user-view-id") || "").trim();
      const target = STATE.views.find((item) => item.id === viewId);
      if (!target) {
        return;
      }
      const serverUrl = String(appState?.config?.serverUrl || "").trim();
      if (!serverUrl) {
        showToast?.("请先连接 Emby", 1200);
        return;
      }
      const embyBase = serverUrl.replace(/\/emby$/i, "").replace(/\/$/, "");
      const path = buildUserViewPath(target);
      window.open(`${embyBase}/web/index.html#!/${path}`, "_blank", "noopener");
    });

    STATE.mounted = true;
    renderUserViewsLoading();
  }

  async function refreshLibraryData(force = false) {
    const now = Date.now();
    if (STATE.loading) {
      return;
    }
    if (!force && now - STATE.lastSyncAt < LIBRARY_SYNC_INTERVAL_MS) {
      return;
    }
    if (!appState?.config?.serverUrl || !appState?.config?.apiKey) {
      STATE.views = [];
      renderUserViews([]);
      return;
    }

    STATE.loading = true;
    try {
      const views = await fetchUserViews();
      STATE.views = views;
      renderUserViews(views);
      STATE.lastSyncAt = now;
    } catch (error) {
      renderUserViewsError(error?.message || "未知错误");
    } finally {
      STATE.loading = false;
    }
  }

  function ensureAndRefresh(force = false) {
    mountModule();
    if (!STATE.mounted) {
      return;
    }
    refreshLibraryData(force);
  }

  ensureAndRefresh(true);

  const mainContent = document.querySelector(".main-content");
  if (mainContent) {
    const viewObserver = new MutationObserver(() => {
      const view = mainContent.dataset.activeView || "";
      if (view === "overview") {
        ensureAndRefresh(false);
      }
    });
    viewObserver.observe(mainContent, {
      attributes: true,
      attributeFilter: ["data-active-view"]
    });
  }

  setInterval(() => {
    const activeView = document.querySelector(".main-content")?.dataset?.activeView || "";
    if (activeView === "overview") {
      ensureAndRefresh(false);
    }
  }, LIBRARY_SYNC_INTERVAL_MS);
})();

/**
 * Add-on: Radar live hotspot on top-right actions (additive only)
 * - Re-skins existing notice button to radar style
 * - Popover toggles on click and refreshes /Sessions every second
 */
(function initRadarLiveHotspotAddon() {
  if (typeof document === "undefined") {
    return;
  }

  const actions = document.querySelector(".frozen-navbar-actions");
  if (!actions) {
    return;
  }

  const noticeBtn = actions.querySelector('.nav-icon-btn[aria-label="通知"]') || actions.querySelector('.nav-icon-btn');
  if (!(noticeBtn instanceof HTMLButtonElement)) {
    return;
  }

  noticeBtn.classList.add("radar-alert-btn");
  noticeBtn.setAttribute("aria-label", "正在热播雷达");
  noticeBtn.setAttribute("aria-expanded", "false");
  noticeBtn.setAttribute("aria-haspopup", "dialog");
  noticeBtn.innerHTML = `
    <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false">
      <circle cx="12" cy="12" r="2.2" fill="#ffffff" />
      <path d="M12 5.2a6.8 6.8 0 0 1 6.8 6.8" stroke="#ffffff" stroke-width="1.8" stroke-linecap="round"/>
      <path d="M12 2.6a9.4 9.4 0 0 1 9.4 9.4" stroke="#ffffff" stroke-width="1.8" stroke-linecap="round" opacity="0.9"/>
      <path d="M12 8.2a3.8 3.8 0 0 1 3.8 3.8" stroke="#ffffff" stroke-width="1.8" stroke-linecap="round" opacity="0.95"/>
    </svg>
    <span id="radar-live-dot" class="radar-live-dot" hidden></span>
  `;

  let backdrop = document.getElementById("radar-live-backdrop");
  if (!backdrop) {
    backdrop = document.createElement("div");
    backdrop.id = "radar-live-backdrop";
    backdrop.hidden = true;
    document.body.appendChild(backdrop);
  }

  let popover = document.getElementById("radar-live-popover");
  if (!popover) {
    popover = document.createElement("section");
    popover.id = "radar-live-popover";
    popover.className = "radar-live-popover";
    popover.hidden = true;
    popover.setAttribute("role", "dialog");
    popover.setAttribute("aria-label", "正在热播设备状态");
    popover.innerHTML = `
      <header class="radar-live-head">
        <strong class="radar-live-title">正在热播</strong>
        <span id="radar-live-meta" class="radar-live-meta">0 设备在线</span>
      </header>
      <div id="radar-live-list" class="radar-live-list">
        <div class="radar-live-empty"><div><strong>当前暂无播放</strong><span>连接 Emby 后自动显示播放设备与进度。</span></div></div>
      </div>
    `;
    document.body.appendChild(popover);
  }

  const els = {
    dot: document.getElementById("radar-live-dot"),
    meta: document.getElementById("radar-live-meta"),
    list: document.getElementById("radar-live-list")
  };

  const state = {
    open: false,
    sessions: [],
    timerId: null
  };
  let suppressOutsideClickUntil = 0;

  function setBackdropOpen(nextOpen) {
    if (!backdrop) {
      return;
    }
    const open = Boolean(nextOpen);
    backdrop.hidden = !open;
    backdrop.classList.toggle("open", open);
  }

  function toSecondsFromTicks(ticks) {
    const num = Number(ticks);
    if (!Number.isFinite(num) || num <= 0) {
      return 0;
    }
    return Math.floor(num / 10000000);
  }

  function formatMmss(seconds) {
    const safe = Math.max(0, Math.floor(Number(seconds) || 0));
    const m = Math.floor(safe / 60);
    const s = safe % 60;
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  }

  function escapeRadarHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function buildRadarPosterUrl(itemId) {
    return buildEmbyPrimaryPosterUrl(itemId, { maxWidth: 220, quality: 88 });
  }

  function getPosterUrl(session) {
    const item = session?.NowPlayingItem;
    const itemId = item?.SeriesId || item?.ParentId || item?.Id;
    if (!itemId) {
      return "";
    }
    return buildRadarPosterUrl(itemId) || "";
  }

  function normalizeSessions(rawSessions) {
    const rows = Array.isArray(rawSessions) ? rawSessions : [];
    return rows.filter((session) => {
      const item = session?.NowPlayingItem;
      return Boolean(item && (item.Name || item.Id));
    });
  }

  async function fetchSessionsFromServer() {
    if (!appState?.config?.serverUrl || !appState?.config?.apiKey) {
      return [];
    }
    try {
      const result = await embyFetch("/Sessions");
      return Array.isArray(result) ? result : [];
    } catch {
      return Array.isArray(appState?.sessions) ? appState.sessions : [];
    }
  }

  function renderEmpty() {
    if (!els.list) {
      return;
    }
    els.list.innerHTML = `<div class="radar-live-empty"><div><strong>当前暂无播放</strong><span>连接 Emby 后自动显示播放设备、内容和进度。</span></div></div>`;
  }

  function renderPopover() {
    const sessions = state.sessions;
    const onlineCount = sessions.length;

    if (els.meta) {
      els.meta.textContent = `${onlineCount} 设备在线`;
    }
    if (els.dot) {
      els.dot.hidden = onlineCount <= 0;
    }

    if (!els.list) {
      return;
    }
    if (!sessions.length) {
      renderEmpty();
      return;
    }

    els.list.innerHTML = sessions.map((session) => {
      const item = session?.NowPlayingItem || {};
      const username = String(session?.UserName || session?.User?.Name || "未知用户");
      const seriesTitle = String(item?.SeriesName || item?.Album || "").trim();
      const episodeTitle = String(item?.Name || "未命名条目").trim();
      const displayTitle = seriesTitle ? `${seriesTitle} - ${episodeTitle}` : episodeTitle;

      const currentSec = toSecondsFromTicks(session?.PlayState?.PositionTicks);
      const totalSec = toSecondsFromTicks(item?.RunTimeTicks);
      const hasTotal = totalSec > 0;
      const progress = hasTotal ? Math.max(0, Math.min(100, (currentSec / totalSec) * 100)) : 0;
      const timeText = hasTotal ? `${formatMmss(currentSec)} / ${formatMmss(totalSec)}` : "--:-- / --:--";

      const poster = getPosterUrl(session);
      const cover = poster
        ? `<img class="radar-live-cover" src="${escapeRadarHtml(poster)}" alt="${escapeRadarHtml(displayTitle)}" loading="lazy">`
        : `<div class="radar-live-cover radar-live-cover-fallback">No</div>`;

      return `
        <article class="radar-live-item">
          <div class="radar-live-row">
            ${cover}
            <div class="radar-live-main">
              <p class="radar-live-user">${escapeRadarHtml(username)}</p>
              <p class="radar-live-name">${escapeRadarHtml(displayTitle)}</p>
            </div>
            <span class="radar-live-time-pill">${escapeRadarHtml(timeText)}</span>
          </div>
          <div class="radar-live-progress"><span style="width:${progress.toFixed(2)}%"></span></div>
        </article>
      `;
    }).join("");
  }

  function placePopover() {
    if (!state.open || !popover || window.matchMedia("(max-width: 768px)").matches) {
      if (popover) {
        popover.style.top = `${(noticeBtn.getBoundingClientRect().bottom || 0) + 10}px`;
        popover.style.left = "";
        popover.style.right = "";
      }
      return;
    }

    const viewportPadding = 12;
    const desktopOffset = 48;
    const rect = noticeBtn.getBoundingClientRect();
    const measuredWidth = Math.max(220, Math.round(popover.getBoundingClientRect().width || 0)) || Math.min(760, window.innerWidth - 24);
    const width = Math.min(measuredWidth, Math.max(220, window.innerWidth - viewportPadding * 2));
    const minLeft = viewportPadding;
    const maxLeft = Math.max(minLeft, window.innerWidth - width - viewportPadding);
    const preferredLeft = rect.right - width - desktopOffset;
    const left = Math.max(minLeft, Math.min(preferredLeft, maxLeft));
    const top = Math.max(8, rect.bottom + 10);

    popover.style.left = `${left}px`;
    popover.style.right = "auto";
    popover.style.top = `${top}px`;
  }

  function setOpen(nextOpen) {
    state.open = Boolean(nextOpen);
    popover.hidden = !state.open;
    setBackdropOpen(state.open);
    noticeBtn.setAttribute("aria-expanded", state.open ? "true" : "false");
    if (state.open) {
      placePopover();
      renderPopover();
    }
  }

  async function tick() {
    const rawSessions = await fetchSessionsFromServer();
    state.sessions = normalizeSessions(rawSessions);
    renderPopover();
    if (state.open) {
      placePopover();
    }
  }

  noticeBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    setOpen(!state.open);
  });

  function isRadarInteractiveTarget(target) {
    return target instanceof Element
      && (target.closest("#radar-live-popover") || target.closest(".radar-alert-btn"));
  }

  document.addEventListener("pointerdown", (event) => {
    if (!state.open || event.pointerType === "touch") {
      return;
    }
    const target = event.target;
    if (isRadarInteractiveTarget(target)) {
      return;
    }
    setOpen(false);
    suppressOutsideClickUntil = Date.now() + 320;
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation?.();
  }, true);

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (Date.now() < suppressOutsideClickUntil) {
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation?.();
      return;
    }
    if (!state.open || isRadarInteractiveTarget(target)) {
      return;
    }
    setOpen(false);
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation?.();
  }, true);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setOpen(false);
    }
  });

  window.addEventListener("resize", () => {
    if (state.open) {
      placePopover();
    }
  });

  tick();
  state.timerId = setInterval(tick, 1000);
})();

/**
 * Add-on patch: mount profile-menu exit button (additive only)
 * - Keep existing menu entries/handlers unchanged
 * - Delegate to original sidebar exit button logic
 */
(function mountProfileMenuExitButtonAddon() {
  if (typeof document === "undefined") {
    return;
  }

  const menuPanel = document.getElementById("profile-menu-panel");
  const exitSource = document.querySelector(".sidebar-exit");
  if (!(menuPanel instanceof HTMLElement) || !(exitSource instanceof HTMLButtonElement)) {
    return;
  }

  if (menuPanel.querySelector("[data-profile-exit-addon='true']")) {
    return;
  }

  const exitButton = document.createElement("button");
  exitButton.type = "button";
  exitButton.className = "profile-menu-item profile-menu-exit-item";
  exitButton.setAttribute("role", "menuitem");
  exitButton.setAttribute("data-profile-exit-addon", "true");
  exitButton.textContent = exitSource.textContent?.trim() || "退出管理系统";
  exitButton.addEventListener("click", () => {
    exitSource.click();
  });

  menuPanel.appendChild(exitButton);
})();

/**
 * Add-on patch: user-center avatar uses first character of username
 * - Letter usernames: lowercased first letter (a / l)
 * - Chinese usernames: first Han character
 * - Pure UI sync, no data/business mutation
 */
(function mountUserCenterAvatarFirstCharAddon() {
  if (typeof document === "undefined") {
    return;
  }

  const tbody = document.getElementById("user-center-body");
  if (!(tbody instanceof HTMLElement)) {
    return;
  }

  function resolveAvatarChar(rawName) {
    const name = String(rawName || "").trim();
    if (!name) {
      return "u";
    }
    const first = Array.from(name)[0] || "u";
    return /[A-Za-z]/.test(first) ? first.toLowerCase() : first;
  }

  function syncUserCenterAvatarChar() {
    const rows = tbody.querySelectorAll(".user-cell");
    rows.forEach((cell) => {
      const avatar = cell.querySelector(".user-avatar");
      const nameEl = cell.querySelector(".user-meta strong");
      if (!(avatar instanceof HTMLElement) || !(nameEl instanceof HTMLElement)) {
        return;
      }
      const expected = resolveAvatarChar(nameEl.textContent);
      if (avatar.textContent !== expected) {
        avatar.textContent = expected;
      }
    });
  }

  syncUserCenterAvatarChar();
  const observer = new MutationObserver(() => {
    syncUserCenterAvatarChar();
  });
  /* Only watch row add/remove on tbody itself; avoid self-trigger loops. */
  observer.observe(tbody, { childList: true });
})();

/**
 * Add-on: Emby ScheduledTasks visual module for Task Center
 * - Additive-only mount: keep existing Task Center DOM/features untouched
 * - Reuse existing embyFetch/auth/config pipeline
 */
(function mountTaskCenterScheduledTasksAddon() {
  if (typeof document === "undefined") {
    return;
  }

  const STORAGE_KEY = "embyPulseTaskCenterNotifyEnabled";
  const POLL_INTERVAL_MS = 20000;

  const state = {
    mounted: false,
    loading: false,
    lastUpdatedAt: "",
    lastError: "",
    tasks: [],
    grouped: [],
    timerId: null,
    activeView: "",
    notifyEnabled: localStorage.getItem(STORAGE_KEY) !== "0",
    seenSnapshot: new Map()
  };

  const refs = {
    topbarActionGroup: null,
    notifyToggle: null,
    refreshBtn: null,
    moduleRoot: null,
    groupsHost: null,
    statusText: null
  };

  function addonEscapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function getActiveView() {
    const fromDataset = String(elements.mainContent?.dataset?.activeView || "").trim();
    if (fromDataset) {
      return fromDataset;
    }
    return String(appState.activeView || "").trim();
  }

  function isEmbyConnected() {
    return Boolean(appState?.config?.serverUrl && appState?.config?.apiKey);
  }

  function ensureTopbarActionGroup() {
    if (refs.topbarActionGroup || !elements.topbarActions) {
      return;
    }
    const group = document.createElement("div");
    group.id = "task-center-topbar-actions";
    group.className = "topbar-actions-group task-center-topbar-actions";
    group.hidden = true;
    group.innerHTML = `
      <label class="task-center-notify-toggle" aria-label="任务通知">
        <span>任务通知</span>
        <input id="task-center-notify-switch" type="checkbox">
        <em aria-hidden="true"></em>
      </label>
      <button id="task-center-refresh-btn" class="ghost-btn task-center-refresh-btn" type="button">刷新状态</button>
    `;
    elements.topbarActions.appendChild(group);

    refs.topbarActionGroup = group;
    refs.notifyToggle = group.querySelector("#task-center-notify-switch");
    refs.refreshBtn = group.querySelector("#task-center-refresh-btn");

    if (refs.notifyToggle instanceof HTMLInputElement) {
      refs.notifyToggle.checked = state.notifyEnabled;
      refs.notifyToggle.addEventListener("change", () => {
        state.notifyEnabled = Boolean(refs.notifyToggle?.checked);
        localStorage.setItem(STORAGE_KEY, state.notifyEnabled ? "1" : "0");
        showToast(state.notifyEnabled ? "任务通知已开启" : "任务通知已关闭", 1000);
      });
    }

    refs.refreshBtn?.addEventListener("click", async () => {
      await refreshScheduledTasks({ manual: true });
    });
  }

  function ensureModuleMounted() {
    if (refs.moduleRoot) {
      return true;
    }
    const taskCenterView = document.getElementById("view-task-center");
    const taskCenterPanel = taskCenterView?.querySelector(".panel");
    if (!taskCenterView || !taskCenterPanel) {
      return false;
    }

    const moduleRoot = document.createElement("section");
    moduleRoot.id = "task-center-emby-module";
    moduleRoot.className = "task-center-emby-module";
    moduleRoot.hidden = false;
    moduleRoot.innerHTML = `
      <div class="task-center-emby-head">
        <div class="task-center-emby-head-main">
          <p class="section-label">Emby 后台任务</p>
          <h3>计划任务可视化</h3>
        </div>
        <p id="task-center-emby-status" class="task-center-emby-status">等待同步...</p>
      </div>
      <div id="task-center-emby-groups" class="task-center-emby-groups"></div>
    `;

    taskCenterPanel.appendChild(moduleRoot);
    refs.moduleRoot = moduleRoot;
    refs.groupsHost = moduleRoot.querySelector("#task-center-emby-groups");
    refs.statusText = moduleRoot.querySelector("#task-center-emby-status");
    return true;
  }

  function inferTaskCategory(name, category) {
    const rawCategory = String(category || "").trim();
    if (rawCategory) {
      return rawCategory;
    }
    const text = String(name || "").toLowerCase();
    if (/danmu|弹幕/.test(text)) {
      return "Danmu";
    }
    if (/神医|douban|tmdb|medialnfo|mediainfo|extract|thumbnail|fingerprint|merge/.test(text)) {
      return "神医助手";
    }
    return "系统任务";
  }

  function localizeTaskCategory(rawCategory) {
    const text = String(rawCategory || "").trim();
    if (!text) {
      return "系统任务";
    }
    const key = text.toLowerCase();
    const map = {
      "downloads & conversions": "下载与转码",
      "playback reporting": "播放统计",
      "danmu": "弹幕任务",
      "神医助手": "神医助手",
      "系统任务": "系统任务"
    };
    return map[key] || text;
  }

  function localizeTaskName(rawName) {
    const text = String(rawName || "").trim();
    if (!text) {
      return "未命名任务";
    }
    const key = text.toLowerCase();
    const map = {
      "scan media library": "扫描媒体库",
      "refresh people": "刷新人物数据",
      "clean cache directory": "清理缓存目录",
      "clean transcode directory": "清理转码目录",
      "chapter image extraction": "章节图提取",
      "extract chapter images": "提取章节图片",
      "refresh guide": "刷新节目指南",
      "recording post processing": "录制后处理",
      "merge versions": "合并多版本",
      "merge multi versions": "合并多版本",
      "delete trashes": "清理回收站",
      "delete persons": "清理人物数据",
      "download missing subtitles": "下载缺失字幕",
      "find missing subtitles": "查找缺失字幕",
      "refresh metadata": "刷新元数据",
      "refresh chinese actor": "刷新中文演员信息",
      "refresh episode": "刷新剧集信息",
      "extract intro fingerprint": "提取片头指纹",
      "extract mediainfo": "提取媒体信息",
      "extract video thumbnail": "提取视频缩略图",
      "build douban cache": "构建豆瓣缓存",
      "refresh chinese actor info": "刷新中文演员信息",
      "refresh episodes": "刷新剧集信息",
      "update subtitle files": "更新字幕文件",
      "scan and match subtitles": "扫描匹配字幕",
      "update danmu files": "更新弹幕文件",
      "scan danmu in library": "扫描媒体库匹配弹幕"
    };
    return map[key] || text;
  }

  function localizeTaskDescription(rawDescription, localizedName) {
    const text = String(rawDescription || "").trim();
    if (!text) {
      const fallbackByName = {
        "扫描媒体库": "扫描媒体库中的新增与变更内容，并更新到数据库。",
        "清理缓存目录": "清理系统缓存文件，释放磁盘空间。",
        "清理转码目录": "清理历史转码临时文件，释放空间。",
        "提取章节图片": "提取媒体章节缩略图，优化播放预览体验。",
        "刷新元数据": "重新抓取并更新媒体条目的元数据。",
        "合并多版本": "按规则自动合并同名多版本影片与剧集条目。"
      };
      return fallbackByName[localizedName] || "执行后台计划任务并更新系统状态。";
    }

    const exactMap = {
      "Build Douban cache, assists with scraping acceleration.": "构建豆瓣元数据缓存，用于辅助刮削加速。",
      "Extract media info and keyframes from video files.": "提取视频与音频媒体信息，并生成关键帧预览。",
      "Extract preview thumbnails and chapter images.": "提取视频预览缩略图与章节图。",
      "Merge duplicate versions automatically after scan.": "按偏好自动合并库内重复多版本资源，扫描后自动执行。"
    };
    if (exactMap[text]) {
      return exactMap[text];
    }

    const replacements = [
      [/scan/gi, "扫描"],
      [/library/gi, "媒体库"],
      [/metadata/gi, "元数据"],
      [/subtitle|subtitles/gi, "字幕"],
      [/danmu/gi, "弹幕"],
      [/refresh/gi, "刷新"],
      [/extract/gi, "提取"],
      [/cache/gi, "缓存"],
      [/thumbnail|thumbnails/gi, "缩略图"],
      [/fingerprint/gi, "指纹"],
      [/chapter/gi, "章节"],
      [/video/gi, "视频"],
      [/audio/gi, "音频"],
      [/persons|people/gi, "人物"],
      [/delete/gi, "清理"],
      [/merge/gi, "合并"],
      [/versions/gi, "多版本"],
      [/playback/gi, "播放"],
      [/reporting/gi, "统计"],
      [/task/gi, "任务"]
    ];

    let localized = text;
    replacements.forEach(([pattern, target]) => {
      localized = localized.replace(pattern, target);
    });
    return localized;
  }

  function formatTaskDate(value) {
    if (!value) {
      return "从未执行过";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const month = date.getMonth() + 1;
    const day = date.getDate();
    const hh = String(date.getHours()).padStart(2, "0");
    const mm = String(date.getMinutes()).padStart(2, "0");
    return `${month}/${day} ${hh}:${mm}`;
  }

  function mapTaskStatus(task) {
    const stateValue = String(task?.State || task?.state || "").toLowerCase();
    const resultStatus = String(
      task?.LastExecutionResult?.Status ||
      task?.LastExecutionResult?.status ||
      task?.LastExecutionStatus ||
      ""
    ).toLowerCase();
    const hasLastRun = Boolean(
      task?.LastExecutionResult?.EndTimeUtc ||
      task?.LastExecutionResult?.EndTime ||
      task?.LastExecutionTimeUtc ||
      task?.LastExecutionTime
    );

    if (stateValue === "running") {
      return {
        key: "running",
        label: "执行中",
        className: "is-running"
      };
    }

    if (
      /fail|error|aborted|cancel|interrupted|stopped/.test(resultStatus) ||
      /fail|error|aborted|cancel/.test(stateValue)
    ) {
      return {
        key: "failed",
        label: "执行失败",
        className: "is-failed"
      };
    }

    if (
      /success|completed|ok/.test(resultStatus) ||
      stateValue === "completed"
    ) {
      return {
        key: "success",
        label: "执行成功",
        className: "is-success"
      };
    }

    if (!hasLastRun) {
      return {
        key: "idle",
        label: "无记录",
        className: "is-idle"
      };
    }

    return {
      key: "idle",
      label: "无记录",
      className: "is-idle"
    };
  }

  function normalizeScheduledTask(raw) {
    const id = String(raw?.Id || raw?.id || raw?.Key || "").trim();
    const originName = String(raw?.Name || raw?.name || "未命名任务").trim();
    const rawCategory = inferTaskCategory(originName, raw?.Category || raw?.category);
    const category = localizeTaskCategory(rawCategory);
    const name = localizeTaskName(originName);
    const description = localizeTaskDescription(String(raw?.Description || raw?.description || "").trim(), name);
    const status = mapTaskStatus(raw);
    const lastRunAt =
      raw?.LastExecutionResult?.EndTimeUtc ||
      raw?.LastExecutionResult?.EndTime ||
      raw?.LastExecutionResult?.StartTimeUtc ||
      raw?.LastExecutionResult?.StartTime ||
      raw?.LastExecutionTimeUtc ||
      raw?.LastExecutionTime ||
      "";

    return {
      id,
      name,
      description,
      category,
      status,
      lastRunAt,
      lastRunText: formatTaskDate(lastRunAt)
    };
  }

  function buildSnapshotKey(task) {
    return `${task.status.key}|${task.lastRunText}`;
  }

  function emitTaskChangeNotifications(nextTasks) {
    if (!state.notifyEnabled || !Array.isArray(nextTasks) || nextTasks.length === 0) {
      state.seenSnapshot = new Map(nextTasks.map((task) => [task.id, buildSnapshotKey(task)]));
      return;
    }

    const nextSnapshot = new Map();
    const changed = [];
    nextTasks.forEach((task) => {
      const key = buildSnapshotKey(task);
      nextSnapshot.set(task.id, key);
      const prev = state.seenSnapshot.get(task.id);
      if (prev && prev !== key) {
        changed.push(task);
      }
    });

    if (changed.length > 0) {
      const top = changed[0];
      showToast(`任务状态更新：${top.name}（${top.status.label}）`, 1300);
      addSyncEvent("任务状态更新", `${top.name}：${top.status.label}`, top.status.key === "failed" ? "danger" : "success");
    }
    state.seenSnapshot = nextSnapshot;
  }

  function groupTasksByCategory(tasks) {
    const map = new Map();
    tasks.forEach((task) => {
      const key = task.category || "系统任务";
      const list = map.get(key) || [];
      list.push(task);
      map.set(key, list);
    });
    return Array.from(map.entries()).map(([category, items]) => ({ category, items }));
  }

  async function fetchScheduledTasksFromServer() {
    const result = await embyFetch("/ScheduledTasks");
    if (Array.isArray(result)) {
      return result;
    }
    if (Array.isArray(result?.Items)) {
      return result.Items;
    }
    return [];
  }

  async function runScheduledTask(taskId, taskName) {
    try {
      await embyFetch(`/ScheduledTasks/Running/${encodeURIComponent(taskId)}`, {
        method: "POST"
      });
      showToast(`已触发任务：${taskName}`, 1100);
      addSyncEvent("手动执行任务", `${taskName} 已提交执行。`, "success");
      window.setTimeout(() => {
        refreshScheduledTasks({ manual: false });
      }, 700);
    } catch (error) {
      showToast("任务触发失败", 1200);
      addSyncEvent("手动执行失败", `${taskName}：${error.message || "未知错误"}`, "danger");
    }
  }

  function bindRunButtonEvents() {
    if (!refs.groupsHost) {
      return;
    }
    refs.groupsHost.querySelectorAll("[data-run-task-id]").forEach((button) => {
      button.addEventListener("click", async () => {
        const target = button;
        if (!(target instanceof HTMLButtonElement)) {
          return;
        }
        const taskId = String(target.dataset.runTaskId || "");
        const taskName = String(target.dataset.runTaskName || "");
        if (!taskId) {
          return;
        }
        target.disabled = true;
        await runScheduledTask(taskId, taskName || taskId);
        target.disabled = false;
      });
    });
  }

  function renderTaskGroups() {
    if (!refs.groupsHost || !refs.statusText) {
      return;
    }

    if (!isEmbyConnected()) {
      refs.statusText.textContent = "未连接 Emby，无法读取任务状态";
      refs.groupsHost.innerHTML = `<div class="task-center-emby-empty">请先在“系统设置”中连接 Emby 服务器。</div>`;
      return;
    }

    if (state.loading) {
      refs.statusText.textContent = "正在同步任务状态...";
    } else if (state.lastError) {
      refs.statusText.textContent = `同步失败：${state.lastError}`;
    } else if (state.lastUpdatedAt) {
      refs.statusText.textContent = `最近同步：${formatTaskDate(state.lastUpdatedAt)}`;
    } else {
      refs.statusText.textContent = "等待同步...";
    }

    if (!state.grouped.length) {
      refs.groupsHost.innerHTML = `<div class="task-center-emby-empty">${state.loading ? "正在加载任务..." : "暂无可展示任务"}</div>`;
      return;
    }

    refs.groupsHost.innerHTML = state.grouped
      .map((group) => {
        const cards = group.items
          .map((task) => `
            <article class="task-center-emby-card">
              <div class="task-center-emby-card-head">
                <div class="task-center-emby-card-title-wrap">
                  <h4>${addonEscapeHtml(task.name)}</h4>
                  <p>${addonEscapeHtml(task.description || "暂无任务描述")}</p>
                </div>
                <button class="task-center-emby-run-btn" type="button" data-run-task-id="${addonEscapeHtml(task.id)}" data-run-task-name="${addonEscapeHtml(task.name)}" aria-label="手动执行任务">▶</button>
              </div>
              <div class="task-center-emby-card-foot">
                <span class="task-center-emby-status ${task.status.className}">
                  <i></i>${addonEscapeHtml(task.status.label)}
                </span>
                <span class="task-center-emby-time">${addonEscapeHtml(task.lastRunText)}</span>
              </div>
            </article>
          `)
          .join("");

        return `
          <section class="task-center-emby-group">
            <div class="task-center-emby-group-head">
              <span class="task-center-emby-group-mark"></span>
              <h3>${addonEscapeHtml(group.category)}</h3>
              <em>${group.items.length}</em>
            </div>
            <div class="task-center-emby-grid">${cards}</div>
          </section>
        `;
      })
      .join("");

    bindRunButtonEvents();
  }

  async function refreshScheduledTasks(options = {}) {
    const { manual = false } = options;
    if (state.loading) {
      return;
    }
    if (!ensureModuleMounted()) {
      return;
    }
    if (!isEmbyConnected()) {
      state.tasks = [];
      state.grouped = [];
      state.lastError = "";
      renderTaskGroups();
      return;
    }

    state.loading = true;
    state.lastError = "";
    renderTaskGroups();
    try {
      const rawTasks = await fetchScheduledTasksFromServer();
      const normalized = rawTasks
        .map((row) => normalizeScheduledTask(row))
        .filter((row) => row.id && row.name)
        .sort((a, b) => a.name.localeCompare(b.name, "zh-CN"));

      if (state.seenSnapshot.size > 0) {
        emitTaskChangeNotifications(normalized);
      } else {
        state.seenSnapshot = new Map(normalized.map((task) => [task.id, buildSnapshotKey(task)]));
      }

      state.tasks = normalized;
      state.grouped = groupTasksByCategory(normalized);
      state.lastUpdatedAt = new Date().toISOString();
      state.lastError = "";
      if (manual) {
        showToast("任务状态已刷新", 1000);
      }
    } catch (error) {
      state.lastError = error?.message || "未知错误";
      if (manual) {
        showToast("任务状态刷新失败", 1200);
      }
    } finally {
      state.loading = false;
      renderTaskGroups();
    }
  }

  function startPolling() {
    if (state.timerId) {
      return;
    }
    state.timerId = window.setInterval(() => {
      refreshScheduledTasks({ manual: false });
    }, POLL_INTERVAL_MS);
  }

  function stopPolling() {
    if (!state.timerId) {
      return;
    }
    window.clearInterval(state.timerId);
    state.timerId = null;
  }

  function onViewChanged() {
    const currentView = getActiveView();
    if (state.activeView === currentView && state.mounted) {
      return;
    }
    state.activeView = currentView;

    ensureTopbarActionGroup();
    if (refs.topbarActionGroup) {
      refs.topbarActionGroup.hidden = currentView !== "task-center";
    }

    if (currentView === "task-center") {
      ensureModuleMounted();
      renderTaskGroups();
      refreshScheduledTasks({ manual: false });
      startPolling();
    } else {
      stopPolling();
    }
  }

  function observeViewState() {
    const target = elements.mainContent;
    if (!target) {
      return;
    }
    const observer = new MutationObserver(() => {
      onViewChanged();
    });
    observer.observe(target, { attributes: true, attributeFilter: ["data-active-view"] });
  }

  function bindConnectivityRefresh() {
    const watchInputs = [elements.serverUrl, elements.apiKey];
    watchInputs.forEach((input) => {
      input?.addEventListener("change", () => {
        if (getActiveView() === "task-center") {
          refreshScheduledTasks({ manual: false });
        }
      });
    });
    elements.connectBtn?.addEventListener("click", () => {
      window.setTimeout(() => {
        if (getActiveView() === "task-center") {
          refreshScheduledTasks({ manual: false });
        }
      }, 300);
    });
  }

  function init() {
    if (state.mounted) {
      return;
    }
    state.mounted = true;
    ensureTopbarActionGroup();
    ensureModuleMounted();
    renderTaskGroups();
    observeViewState();
    bindConnectivityRefresh();
    onViewChanged();
  }

  init();
})();

/**
 * Add-on: Annual Ranking module for Content Ranking view
 * - Additive-only mount, keeps existing table/ranking module untouched
 * - Reuses existing embyFetch/auth/config pipeline
 */
(function mountAnnualRankingAddon() {
  if (typeof document === "undefined") {
    return;
  }

  const POLL_INTERVAL_MS = 60000;
  const MAX_RANKING_ITEMS = 50;
  const ANNUAL_RANKING_CACHE_KEY = "vistamirrorAnnualRankingCacheV4";
  const ANNUAL_RANKING_CACHE_TTL_KEY = "vistamirrorAnnualRankingCacheTtlMs";
  const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;
  const MAX_STALE_CACHE_MS = 24 * 60 * 60 * 1000;

  const state = {
    mounted: false,
    loading: false,
    refreshPromise: null,
    coverRefreshPromise: null,
    error: "",
    lastUpdatedAt: "",
    cacheHydrated: false,
    cacheExpired: false,
    cacheLoadedAt: 0,
    viewCount: 0,
    rawLogCount: 0,
    matchedEventCount: 0,
    source: "none",
    events: [],
    scopeOptions: [{ value: "all", label: "全服" }],
    annualCoverCache: {},
    annualItemDetailCache: {},
    annualMediaIdentityCache: {},
    items: [],
    filteredItems: [],
    category: "global",
    sortBy: "playCount",
    scope: "all",
    activeView: "",
    timerId: null,
    preloadedImages: new Set()
  };

  const refs = {
    root: null,
    controls: null,
    top3Host: null,
    listHost: null,
    status: null
  };

  function escapeAnnualHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function getCurrentView() {
    return String(elements.mainContent?.dataset?.activeView || appState.activeView || "").trim();
  }

  function isConnected() {
    return Boolean(appState?.config?.serverUrl && appState?.config?.apiKey);
  }

  function getAnnualCacheTtlMs() {
    try {
      const raw = Number(window.localStorage?.getItem(ANNUAL_RANKING_CACHE_TTL_KEY) || "");
      if (Number.isFinite(raw) && raw >= 60000) {
        return Math.min(raw, 30 * 60 * 1000);
      }
    } catch {
      // ignore unavailable storage
    }
    return DEFAULT_CACHE_TTL_MS;
  }

  function getAnnualCacheConfigKey() {
    return String(appState?.config?.serverUrl || "").trim().replace(/\/$/, "").toLowerCase();
  }

  function cloneAnnualRankingItem(item) {
    return {
      id: String(item?.id || ""),
      coverCacheKey: String(item?.coverCacheKey || ""),
      itemId: String(item?.itemId || ""),
      name: String(item?.name || item?.title || ""),
      title: String(item?.title || ""),
      type: String(item?.type || "other"),
      year: item?.year ?? null,
      overview: String(item?.overview || ""),
      imageTag: String(item?.imageTag || ""),
      imageUrl: String(item?.imageUrl || ""),
      playCount: Math.max(0, Number(item?.playCount) || 0),
      duration: Math.max(0, Number(item?.duration ?? item?.totalDurationMinutes) || 0),
      totalDurationMinutes: Math.max(0, Number(item?.totalDurationMinutes ?? item?.duration) || 0),
      lastPlayed: String(item?.lastPlayed || ""),
      posterUrl: String(item?.posterUrl || item?.imageUrl || ""),
      coverSearchTitles: Array.isArray(item?.coverSearchTitles) ? item.coverSearchTitles.filter(Boolean) : [],
      coverResolveSource: String(item?.coverResolveSource || "cache"),
      coverCandidateCount: Math.max(0, Number(item?.coverCandidateCount) || 0),
      coverCandidatesTop: Array.isArray(item?.coverCandidatesTop) ? item.coverCandidatesTop.filter(Boolean) : [],
      coverCandidatesList: Array.isArray(item?.coverCandidatesList) ? item.coverCandidatesList.filter(Boolean) : []
    };
  }

  function cloneAnnualEvent(event) {
    return {
      title: String(event?.title || ""),
      userName: String(event?.userName || ""),
      itemId: String(event?.itemId || ""),
      playedAt: String(event?.playedAt || ""),
      durationMin: Math.max(0, Number(event?.durationMin) || 0),
      type: String(event?.type || "other"),
      posterUrl: String(event?.posterUrl || ""),
      aggregateKey: String(event?.aggregateKey || ""),
      aggregateId: String(event?.aggregateId || ""),
      aggregateType: String(event?.aggregateType || ""),
      aggregateKind: String(event?.aggregateKind || ""),
      aggregateTitle: String(event?.aggregateTitle || ""),
      coverCacheKey: String(event?.coverCacheKey || ""),
      coverItemId: String(event?.coverItemId || ""),
      coverSearchTitles: Array.isArray(event?.coverSearchTitles) ? event.coverSearchTitles.filter(Boolean) : []
    };
  }

  function cloneAnnualCoverCache(cache) {
    const out = {};
    Object.entries(cache || {}).forEach(([key, value]) => {
      if (!key || !hasAnnualCoverCandidates(value)) {
        return;
      }
      out[key] = {
        top: Array.isArray(value?.top) ? value.top.filter(Boolean) : [],
        list: Array.isArray(value?.list) ? value.list.filter(Boolean) : [],
        source: String(value?.source || "cache"),
        candidateCount: Math.max(0, Number(value?.candidateCount) || 0)
      };
    });
    return out;
  }

  function restoreAnnualRankingCache() {
    try {
      const raw = window.localStorage?.getItem(ANNUAL_RANKING_CACHE_KEY);
      if (!raw) {
        return false;
      }
      const payload = JSON.parse(raw);
      if (!payload || payload.version !== 4 || payload.configKey !== getAnnualCacheConfigKey()) {
        return false;
      }
      const cachedAt = Number(payload.cachedAt || 0);
      const age = Date.now() - cachedAt;
      if (!Number.isFinite(age) || age < 0 || age > MAX_STALE_CACHE_MS) {
        return false;
      }

      state.source = String(payload.source || "cache");
      state.lastUpdatedAt = String(payload.lastUpdatedAt || new Date(cachedAt).toISOString());
      state.rawLogCount = Math.max(0, Number(payload.rawLogCount) || 0);
      state.matchedEventCount = Math.max(0, Number(payload.matchedEventCount) || 0);
      state.viewCount = Math.max(0, Number(payload.viewCount) || 0);
      state.category = String(payload.category || state.category || "global");
      state.sortBy = String(payload.sortBy || state.sortBy || "playCount");
      state.scope = String(payload.scope || state.scope || "all");
      state.scopeOptions = Array.isArray(payload.scopeOptions) && payload.scopeOptions.length ? payload.scopeOptions : [{ value: "all", label: "全服" }];
      state.events = Array.isArray(payload.events) ? payload.events.map(cloneAnnualEvent) : [];
      state.items = Array.isArray(payload.items) ? payload.items.map(cloneAnnualRankingItem) : [];
      state.annualCoverCache = cloneAnnualCoverCache(payload.annualCoverCache || {});
      applyCoverCandidatesToItems(state.items);
      applyFilterAndSort();
      state.cacheHydrated = state.filteredItems.length > 0;
      state.cacheLoadedAt = cachedAt;
      state.cacheExpired = age > getAnnualCacheTtlMs();
      return state.cacheHydrated;
    } catch {
      return false;
    }
  }

  function persistAnnualRankingCache() {
    if (!state.items.length) {
      return;
    }
    try {
      const payload = {
        version: 4,
        cachedAt: Date.now(),
        cacheTtlMs: getAnnualCacheTtlMs(),
        configKey: getAnnualCacheConfigKey(),
        source: state.source,
        lastUpdatedAt: state.lastUpdatedAt || new Date().toISOString(),
        rawLogCount: state.rawLogCount,
        matchedEventCount: state.matchedEventCount,
        viewCount: state.viewCount,
        category: state.category,
        sortBy: state.sortBy,
        scope: state.scope,
        scopeOptions: state.scopeOptions || [{ value: "all", label: "全服" }],
        events: (state.events || []).slice(0, 4000).map(cloneAnnualEvent),
        items: (state.items || []).slice(0, MAX_RANKING_ITEMS).map(cloneAnnualRankingItem),
        annualCoverCache: cloneAnnualCoverCache(state.annualCoverCache)
      };
      window.localStorage?.setItem(ANNUAL_RANKING_CACHE_KEY, JSON.stringify(payload));
    } catch {
      // Quota/storage errors should never block the ranking UI.
    }
  }

  function formatMinutesLabel(minutes) {
    const m = Math.max(0, Math.round(Number(minutes) || 0));
    return `${m.toLocaleString("zh-CN")} 分钟`;
  }

  function formatPlayCountLabel(count) {
    const n = Math.max(0, Math.round(Number(count) || 0));
    return `${n.toLocaleString("zh-CN")} 次`;
  }

  function formatShortDateTime(value) {
    if (!value) {
      return "无记录";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const month = date.getMonth() + 1;
    const day = date.getDate();
    const hh = String(date.getHours()).padStart(2, "0");
    const mm = String(date.getMinutes()).padStart(2, "0");
    return `${month}/${day} ${hh}:${mm}`;
  }

  function buildAnnualImageUrl(itemId, imageType, imageTag = "", maxWidth = 420) {
    if (!itemId || !appState?.config?.serverUrl || !appState?.config?.apiKey) {
      return "";
    }
    const type = String(imageType || "").trim();
    if (!type) {
      return "";
    }
    const tag = String(imageTag || "").trim();
    const width = Math.max(160, Number(maxWidth) || 420);
    const tagQuery = tag ? `&tag=${encodeURIComponent(tag)}` : "";
    return `${appState.config.serverUrl}/Items/${encodeURIComponent(itemId)}/Images/${type}?maxWidth=${width}&quality=90&api_key=${encodeURIComponent(
      appState.config.apiKey
    )}${tagQuery}`;
  }

  function dedupeStringList(rows) {
    return Array.from(new Set((rows || []).filter(Boolean)));
  }

  function hasAnnualCoverCandidates(entry) {
    if (!entry || typeof entry !== "object") {
      return false;
    }
    const top = Array.isArray(entry.top) ? entry.top : [];
    const list = Array.isArray(entry.list) ? entry.list : [];
    return top.length > 0 || list.length > 0;
  }

  function purgeAnnualEmptyCoverCache() {
    Object.keys(state.annualCoverCache || {}).forEach((key) => {
      if (!hasAnnualCoverCandidates(state.annualCoverCache[key])) {
        delete state.annualCoverCache[key];
      }
    });
  }

  async function getAnnualItemDetail(itemId) {
    const id = String(itemId || "").trim();
    if (!id) {
      return null;
    }
    if (Object.prototype.hasOwnProperty.call(state.annualItemDetailCache, id)) {
      return state.annualItemDetailCache[id];
    }
    try {
      const detail = await embyFetch(
        `/Items/${id}?Fields=Name,SeriesName,SeriesId,SeasonId,ParentId,PrimaryImageItemId,Type,RunTimeTicks,IndexNumber,ParentIndexNumber,ImageTags,BackdropImageTags`
      );
      state.annualItemDetailCache[id] = detail || null;
      return state.annualItemDetailCache[id];
    } catch {
      state.annualItemDetailCache[id] = null;
      return null;
    }
  }

  async function buildAnnualCoverIdChain(itemId) {
    const id = String(itemId || "").trim();
    if (!id) {
      return [];
    }
    const detail = await getAnnualItemDetail(id);
    const type = String(detail?.Type || "").toLowerCase();
    const primaryImageItemId = String(detail?.PrimaryImageItemId || "").trim();
    if (type === "episode") {
      const seasonId = String(detail?.SeasonId || detail?.ParentId || "").trim();
      return dedupeStringList([detail?.SeriesId, primaryImageItemId, seasonId, detail?.ParentId, id]);
    }
    if (type === "season") {
      return dedupeStringList([detail?.SeriesId, primaryImageItemId, detail?.ParentId, id]);
    }
    return dedupeStringList([primaryImageItemId, id, detail?.SeriesId, detail?.ParentId]);
  }

  async function buildAnnualCoverCandidatesFromSearch(titles, maxWidth) {
    const titleList = dedupeStringList((titles || []).map((title) => String(title || "").trim()).filter(Boolean)).slice(0, 4);
    if (!titleList.length) {
      return [];
    }

    const matchedIds = [];
    for (const title of titleList) {
      try {
        const result = await embyFetch(
          `/Items?Recursive=true&SearchTerm=${escapeQueryValue(title)}&IncludeItemTypes=Season,Series,Movie,Episode&Fields=SeriesId,SeasonId,ParentId,Type,ImageTags,BackdropImageTags&Limit=12`
        );
        const items = Array.isArray(result?.Items) ? result.Items : [];
        const best = selectBestCoverItem(items, title);
        if (best?.Id) {
          matchedIds.push(String(best.Id));
        }
      } catch {
        // ignore title search failures, keep current fallback chain
      }
    }

    const idChain = [];
    for (const matchedId of dedupeStringList(matchedIds)) {
      const chain = await buildAnnualCoverIdChain(matchedId);
      idChain.push(...chain);
    }

    const finalIds = dedupeStringList(idChain);
    if (!finalIds.length) {
      return [];
    }
    return buildAnnualCoverCandidatesForWidth(finalIds, maxWidth);
  }

  function stripAnnualSeasonSuffix(title) {
    return String(title || "")
      .trim()
      .replace(/\s*[-–—]\s*第\s*\d+\s*季\s*$/i, "")
      .replace(/\s*[-–—]\s*season\s*\d+\s*$/i, "")
      .replace(/\s*[-–—]\s*s\s*\d+\s*$/i, "")
      .replace(/\s*第\s*\d+\s*季\s*$/i, "")
      .trim();
  }

  async function buildAnnualCoverCandidatesForWidth(itemIds, maxWidth) {
    const candidates = [];
    for (const id of itemIds) {
      const detail = await getAnnualItemDetail(id);
      const imageTags = detail?.ImageTags && typeof detail.ImageTags === "object" ? detail.ImageTags : {};
      candidates.push(
        buildAnnualImageUrl(id, "Primary", imageTags.Primary || "", maxWidth),
        buildAnnualImageUrl(id, "Primary", "", maxWidth)
      );
    }
    return dedupeStringList(candidates);
  }

  async function resolveAnnualCoverCandidates(item) {
    const itemId = String(item?.itemId || "").trim();
    const coverKey = String(item?.coverCacheKey || item?.id || itemId).trim();
    const searchTitles = Array.isArray(item?.coverSearchTitles) ? item.coverSearchTitles : [item?.title || ""];
    const primaryTitle = searchTitles[0] || item?.title || "";
    if (!coverKey && !itemId) {
      return { top: [], list: [] };
    }
    const cacheKey = coverKey || itemId;
    const existing = state.annualCoverCache[cacheKey];
    if (existing && hasAnnualCoverCandidates(existing)) {
      return state.annualCoverCache[cacheKey];
    }
    if (existing && !hasAnnualCoverCandidates(existing)) {
      delete state.annualCoverCache[cacheKey];
    }

    const idChain = itemId ? await buildAnnualCoverIdChain(itemId) : [];
    const [idTopCandidates, idListCandidates, searchTopCandidates, searchListCandidates] = await Promise.all([
      idChain.length ? buildAnnualCoverCandidatesForWidth(idChain, 900) : Promise.resolve([]),
      idChain.length ? buildAnnualCoverCandidatesForWidth(idChain, 420) : Promise.resolve([]),
      buildAnnualCoverCandidatesFromSearch(searchTitles, 900),
      buildAnnualCoverCandidatesFromSearch(searchTitles, 420)
    ]);

    const embyTop = dedupeStringList([...(searchTopCandidates || []), ...(idTopCandidates || [])]);
    const embyList = dedupeStringList([...(searchListCandidates || []), ...(idListCandidates || [])]);
    const merged = await resolvePosterCandidates({
      title: primaryTitle,
      type: item?.type || "other",
      year: extractYearFromTitle(primaryTitle, 0),
      seasonNo: extractSeasonNoFromTitle(primaryTitle, 0),
      embyTopCandidates: embyTop,
      embyListCandidates: embyList,
      topSize: "w780",
      listSize: "w500",
      includeTmdbBackup: true
    });
    const top = dedupeStringList(merged.top || []);
    const list = dedupeStringList(merged.list || []);
    const source = merged.source || (embyTop.length || embyList.length ? "emby" : "fallback");
    state.annualCoverCache[cacheKey] = {
      top,
      list,
      source,
      candidateCount: dedupeStringList([...(top || []), ...(list || [])]).length
    };
    return state.annualCoverCache[cacheKey];
  }

  function encodeCoverList(urls) {
    return (urls || []).map((url) => encodeURIComponent(url)).join("|");
  }

  function getAnnualPosterFirstCandidates(item) {
    return dedupeStringList([
      item?.posterUrl || item?.imageUrl || "",
      ...(item?.coverCandidatesList || []),
      ...(item?.coverCandidatesTop || [])
    ]);
  }

  function buildAnnualCoverImageHtml(item, className, coverType) {
    const list = getAnnualPosterFirstCandidates(item);
    const firstUrl = String(list[0] || "").trim();
    if (!firstUrl) {
      return "";
    }
    const encodedList = encodeCoverList(list.length ? list : [firstUrl]);
    const fallbackClass =
      coverType === "top"
        ? "annual-ranking-top-cover annual-ranking-top-cover-fallback"
        : "annual-ranking-row-cover annual-ranking-row-cover-fallback";
    const fallbackText = coverType === "top" ? "No Poster" : "No";
    return `<img class="${className}" src="${escapeAnnualHtml(firstUrl)}" alt="${escapeAnnualHtml(item.title)}" loading="lazy" decoding="async" data-cover-list="${encodedList}" data-cover-index="0" data-fallback-class="${fallbackClass}" data-fallback-text="${fallbackText}">`;
  }

  function bindAnnualCoverFallbacks(host) {
    if (!host) {
      return;
    }
    host.querySelectorAll("img[data-cover-list]").forEach((node) => {
      if (!(node instanceof HTMLImageElement)) {
        return;
      }
      if (node.dataset.coverBound === "1") {
        return;
      }
      node.dataset.coverBound = "1";
      node.addEventListener("error", () => {
        const list = String(node.dataset.coverList || "")
          .split("|")
          .map((value) => {
            try {
              return decodeURIComponent(value);
            } catch {
              return "";
            }
          })
          .filter(Boolean);
        const nextIndex = Number(node.dataset.coverIndex || "0") + 1;
        node.dataset.coverIndex = String(nextIndex);
        if (nextIndex < list.length) {
          node.src = list[nextIndex];
          return;
        }

        const wrapper = node.parentElement;
        if (!wrapper) {
          return;
        }
        const fallback = document.createElement("div");
        fallback.className = node.dataset.fallbackClass || "";
        fallback.textContent = node.dataset.fallbackText || "No";
        wrapper.replaceChildren(fallback);
      });
    });
  }

  function applyCoverCandidatesToItems(items) {
    (items || []).forEach((item) => {
      const coverKey = String(item?.coverCacheKey || item?.id || item?.itemId || "").trim();
      if (!coverKey) {
        return;
      }
      const fallbackItemKey = String(item?.itemId || "").trim();
      const cached = state.annualCoverCache[coverKey] || (fallbackItemKey ? state.annualCoverCache[fallbackItemKey] : null);
      if (!cached) {
        return;
      }
      item.coverCandidatesTop = cached.top || [];
      item.coverCandidatesList = cached.list || [];
      item.coverResolveSource = cached.source || (hasAnnualCoverCandidates(cached) ? "id-chain" : "fallback");
      item.coverCandidateCount = Math.max(0, Number(cached.candidateCount) || 0);
      if (!item.posterUrl) {
        item.posterUrl = getAnnualPosterFirstCandidates(item)[0] || "";
      }
    });
  }

  async function hydrateAnnualCoverCacheFromItems(items) {
    purgeAnnualEmptyCoverCache();
    const queue = (items || [])
      .map((item) => ({
        id: String(item?.id || "").trim(),
        itemId: String(item?.itemId || "").trim(),
        coverCacheKey: String(item?.coverCacheKey || item?.id || item?.itemId || "").trim(),
        title: String(item?.title || "").trim(),
        coverSearchTitles: Array.isArray(item?.coverSearchTitles) ? item.coverSearchTitles : []
      }))
      .filter((item) => item.coverCacheKey || item.itemId)
      .slice(0, MAX_RANKING_ITEMS);

    const seen = new Set();
    for (const item of queue) {
      const key = item.coverCacheKey || item.itemId;
      if (!key || seen.has(key)) {
        continue;
      }
      seen.add(key);
      await resolveAnnualCoverCandidates(item);
    }
  }

  function getTypeFromItem(item) {
    const type = String(item?.Type || "").toLowerCase();
    if (type === "movie") {
      return "movie";
    }
    if (type === "series" || type === "episode" || type === "season") {
      return "series";
    }
    return "other";
  }

  function getTypeFromLog(log, title) {
    const rawType = String(log?.ItemType || log?.Item?.Type || log?.Type || "").toLowerCase();
    if (rawType.includes("movie")) {
      return "movie";
    }
    if (rawType.includes("series") || rawType.includes("episode") || rawType.includes("season")) {
      return "series";
    }
    const inferred = inferMediaTypeLabel(title);
    if (inferred === "Series" || inferred === "Episode") {
      return "series";
    }
    return "other";
  }

  function normalizeRankingItem(rawItem) {
    const id = String(rawItem?.Id || "").trim();
    const rawType = String(rawItem?.Type || "").toLowerCase();
    const baseTitle = String(rawItem?.Name || "未命名内容").trim();
    const title =
      rawType === "season"
        ? buildAnnualSeasonTitle(String(rawItem?.SeriesName || "").trim(), rawItem, baseTitle)
        : normalizeAnnualMediaTitle(baseTitle);
    const type = getTypeFromItem(rawItem);
    const aggregateKind = rawType === "season" ? "season" : type;
    const playCountRaw = rawItem?.PlayCount ?? rawItem?.UserData?.PlayCount ?? 0;
    const playCount = Math.max(0, Number(playCountRaw) || 0);
    const runTicks = Number(rawItem?.RunTimeTicks || 0);
    const runtimeSeconds = runTicks > 0 ? Math.floor(runTicks / 10000000) : 0;
    const totalDurationMinutes = runtimeSeconds > 0 ? Math.max(0, Math.round((runtimeSeconds * playCount) / 60)) : 0;

    return {
      id,
      coverCacheKey: buildAnnualAggregateKey(type, id, title, aggregateKind),
      itemId: id,
      title,
      type,
      playCount,
      totalDurationMinutes,
      lastPlayed: rawItem?.DateLastPlayed || rawItem?.DateLastContentAdded || rawItem?.DateCreated || "",
      posterUrl: "",
      coverSearchTitles: buildAnnualCoverSearchTitles(title),
      coverResolveSource: "fallback",
      coverCandidateCount: 0,
      coverCandidatesTop: [],
      coverCandidatesList: []
    };
  }

  function normalizeAnnualBackendItem(rawItem) {
    const id = String(rawItem?.id || rawItem?.itemId || rawItem?.Id || "").trim();
    const title = normalizeAnnualMediaTitle(String(rawItem?.name || rawItem?.title || rawItem?.Name || "未命名内容").trim());
    const imageUrl = String(rawItem?.imageUrl || rawItem?.posterUrl || "").trim();
    const type = normalizeAnnualMediaType(rawItem?.type || rawItem?.Type || "other");
    return {
      id: id || buildAnnualAggregateKey(type, "", title, "title"),
      coverCacheKey: buildAnnualAggregateKey(type, id, title, type),
      itemId: String(rawItem?.itemId || rawItem?.id || rawItem?.Id || "").trim(),
      name: title,
      title,
      type,
      year: rawItem?.year ?? rawItem?.ProductionYear ?? null,
      overview: String(rawItem?.overview || rawItem?.Overview || ""),
      imageTag: String(rawItem?.imageTag || ""),
      imageUrl,
      playCount: Math.max(0, Number(rawItem?.playCount || rawItem?.PlayCount) || 0),
      duration: Math.max(0, Number(rawItem?.duration ?? rawItem?.totalDurationMinutes) || 0),
      totalDurationMinutes: Math.max(0, Number(rawItem?.totalDurationMinutes ?? rawItem?.duration) || 0),
      lastPlayed: String(rawItem?.lastPlayed || rawItem?.DateLastPlayed || ""),
      posterUrl: imageUrl,
      coverSearchTitles: buildAnnualCoverSearchTitles(title),
      coverResolveSource: imageUrl ? "backend" : "none",
      coverCandidateCount: imageUrl ? 1 : 0,
      coverCandidatesTop: [],
      coverCandidatesList: []
    };
  }

  function dedupeById(items) {
    const map = new Map();
    items.forEach((item) => {
      if (!item?.id) {
        return;
      }
      if (!map.has(item.id)) {
        map.set(item.id, item);
      }
    });
    return Array.from(map.values());
  }

  function stripEpisodeHintsFromTitle(title) {
    let next = String(title || "").trim();
    if (!next) {
      return "";
    }

    const rules = [
      /\s*[-–—]\s*S\d+\s*,?\s*E(?:P)?\d+\b.*$/i,
      /\s*[-–—]\s*S\d+\s*E\d+\b.*$/i,
      /\s*[-–—]\s*第\s*\d+\s*季\s*第\s*\d+\s*集.*$/i,
      /\s*[-–—]\s*第\s*\d+\s*集.*$/i
    ];
    rules.forEach((pattern) => {
      next = next.replace(pattern, "").trim();
    });
    return next;
  }

  function normalizeAnnualMediaTitle(title) {
    const stripped = stripEpisodeHintsFromTitle(title);
    return stripped || String(title || "").trim() || "未命名内容";
  }

  function normalizeAnnualMediaType(value, fallbackType = "other") {
    const type = String(value || fallbackType || "").toLowerCase();
    if (type.includes("movie")) {
      return "movie";
    }
    if (type.includes("series") || type.includes("episode") || type.includes("season")) {
      return "series";
    }
    if (type === "movie" || type === "series" || type === "other") {
      return type;
    }
    return String(fallbackType || "other").toLowerCase();
  }

  function normalizeAnnualAggregateKind(value, fallbackKind = "title") {
    const kind = String(value || fallbackKind || "").toLowerCase();
    if (kind.includes("season")) {
      return "season";
    }
    if (kind.includes("series")) {
      return "series";
    }
    if (kind.includes("movie")) {
      return "movie";
    }
    return "title";
  }

  function buildAnnualAggregateKey(type, aggregateId, aggregateTitle, aggregateKind = "") {
    const normalizedType = normalizeAnnualMediaType(type || "other");
    const normalizedKind = normalizeAnnualAggregateKind(aggregateKind || normalizedType);
    const id = String(aggregateId || "").trim();
    const titleKey = normalizeMediaLookupKey(normalizeAnnualMediaTitle(aggregateTitle || ""));
    if (normalizedKind === "season") {
      return id ? `season:${id}` : `season-title:${titleKey || "unknown"}`;
    }
    if (normalizedKind === "series") {
      return id ? `series:${id}` : `series-title:${titleKey || "unknown"}`;
    }
    if (normalizedKind === "movie") {
      return id ? `movie:${id}` : `movie-title:${titleKey || "unknown"}`;
    }
    return `title:${normalizedType}:${titleKey || "unknown"}`;
  }

  function formatAnnualSeasonLabel(seasonDetail, fallbackTitle = "") {
    const index = Number(seasonDetail?.IndexNumber ?? seasonDetail?.ParentIndexNumber ?? 0);
    if (Number.isFinite(index) && index > 0) {
      return `第${index}季`;
    }
    const rawName = String(seasonDetail?.Name || "").trim();
    if (/第\s*\d+\s*季/.test(rawName)) {
      const match = rawName.match(/第\s*\d+\s*季/);
      return match?.[0] || rawName;
    }
    const seasonMatch = rawName.match(/season\s*(\d+)/i);
    if (seasonMatch?.[1]) {
      return `第${seasonMatch[1]}季`;
    }
    const fallback = String(fallbackTitle || "");
    const fallbackMatch = fallback.match(/第\s*\d+\s*季/);
    if (fallbackMatch?.[0]) {
      return fallbackMatch[0];
    }
    return rawName || "";
  }

  function buildAnnualSeasonTitle(seriesName, seasonDetail, fallbackTitle = "") {
    const cleanSeries = normalizeAnnualMediaTitle(seriesName || "");
    const seasonLabel = formatAnnualSeasonLabel(seasonDetail, fallbackTitle);
    if (cleanSeries && seasonLabel) {
      return `${cleanSeries} - ${seasonLabel}`;
    }
    return normalizeAnnualMediaTitle(cleanSeries || seasonLabel || fallbackTitle);
  }

  function buildAnnualCoverSearchTitles(...titles) {
    const out = [];
    const seen = new Set();
    const push = (value) => {
      const text = String(value || "").trim();
      if (!text) {
        return;
      }
      const key = normalizeMediaLookupKey(text);
      if (!key || seen.has(key)) {
        return;
      }
      seen.add(key);
      out.push(text);
    };

    titles
      .flat()
      .forEach((raw) => {
        const direct = String(raw || "").trim();
        const normalized = normalizeAnnualMediaTitle(raw);
        const seasonStripped = stripAnnualSeasonSuffix(direct);
        const normalizedSeasonStripped = stripAnnualSeasonSuffix(normalized);
        const beforeDash = direct.split("-")[0]?.trim() || "";
        const beforeLongDash = direct.split("—")[0]?.trim() || "";
        [direct, normalized, seasonStripped, normalizedSeasonStripped, beforeDash, beforeLongDash].forEach(push);
      });

    return out;
  }

  function getAnnualCoverHitStats(items) {
    const rows = Array.isArray(items) ? items : [];
    const total = rows.length;
    const hits = rows.reduce((count, item) => {
      const hasDirect = Boolean(String(item?.posterUrl || "").trim());
      const hasCandidates = (Array.isArray(item?.coverCandidatesTop) && item.coverCandidatesTop.length > 0) || (Array.isArray(item?.coverCandidatesList) && item.coverCandidatesList.length > 0);
      return count + (hasDirect || hasCandidates ? 1 : 0);
    }, 0);
    return { hits, total };
  }

  async function resolveAnnualMediaIdentityFromEvent(event) {
    const fallbackTitle = normalizeAnnualMediaTitle(event?.title || "");
    const fallbackType = normalizeAnnualMediaType(event?.type || "other");
    const itemId = String(event?.itemId || "").trim();
    if (!itemId) {
      return {
        aggregateKey: buildAnnualAggregateKey(fallbackType, "", fallbackTitle, "title"),
        aggregateId: "",
        aggregateType: fallbackType,
        aggregateKind: "title",
        aggregateTitle: fallbackTitle,
        coverCacheKey: buildAnnualAggregateKey(fallbackType, "", fallbackTitle, "title"),
        coverItemId: "",
        coverSearchTitles: buildAnnualCoverSearchTitles(fallbackTitle),
        detailType: fallbackType,
        fallbackDurationMin: 0
      };
    }

    if (Object.prototype.hasOwnProperty.call(state.annualMediaIdentityCache, itemId)) {
      return state.annualMediaIdentityCache[itemId];
    }

    const detail = await getAnnualItemDetail(itemId);
    const detailType = normalizeAnnualMediaType(detail?.Type || fallbackType, fallbackType);
    const rawDetailType = String(detail?.Type || "").toLowerCase();
    const detailRuntimeTicks = Number(detail?.RunTimeTicks || 0);
    const fallbackDurationMin = detailRuntimeTicks > 0 ? Math.max(1, Math.round(detailRuntimeTicks / 10000000 / 60)) : 0;

    let aggregateType = detailType;
    let aggregateKind = detailType === "movie" ? "movie" : detailType === "series" ? "series" : "title";
    let aggregateId = "";
    let aggregateTitle = fallbackTitle;
    let coverItemId = itemId;
    let coverSearchTitles = buildAnnualCoverSearchTitles(fallbackTitle);

    if (detailType === "series" && rawDetailType === "episode") {
      const seasonId = String(detail?.SeasonId || detail?.ParentId || "").trim();
      const seriesId = String(detail?.SeriesId || "").trim();

      const seasonDetail = seasonId ? await getAnnualItemDetail(seasonId) : null;
      const resolvedSeriesId = String(seasonDetail?.SeriesId || seriesId || "").trim();
      const seriesDetail = resolvedSeriesId ? await getAnnualItemDetail(resolvedSeriesId) : null;
      const seriesTitle = String(seriesDetail?.Name || detail?.SeriesName || seasonDetail?.SeriesName || "").trim();

      if (seasonId) {
        aggregateKind = "season";
        aggregateId = seasonId;
        coverItemId = seasonId;
        aggregateTitle = buildAnnualSeasonTitle(seriesTitle, seasonDetail || detail, fallbackTitle);
        coverSearchTitles = buildAnnualCoverSearchTitles(aggregateTitle, seriesTitle, seasonDetail?.Name, fallbackTitle);
      } else if (resolvedSeriesId) {
        aggregateKind = "series";
        aggregateId = resolvedSeriesId;
        coverItemId = resolvedSeriesId;
        aggregateTitle = normalizeAnnualMediaTitle(seriesTitle || fallbackTitle);
        coverSearchTitles = buildAnnualCoverSearchTitles(aggregateTitle, fallbackTitle);
      }
    } else if (detailType === "series" && rawDetailType === "season") {
      const seriesId = String(detail?.SeriesId || detail?.ParentId || "").trim();
      const seriesDetail = seriesId ? await getAnnualItemDetail(seriesId) : null;
      const seriesTitle = String(seriesDetail?.Name || detail?.SeriesName || "").trim();
      aggregateKind = "season";
      aggregateId = itemId;
      coverItemId = itemId;
      aggregateTitle = buildAnnualSeasonTitle(seriesTitle, detail, fallbackTitle);
      coverSearchTitles = buildAnnualCoverSearchTitles(aggregateTitle, seriesTitle, detail?.Name, fallbackTitle);
    } else if (detailType === "series") {
      aggregateId = detail ? itemId : "";
      coverItemId = itemId;
      aggregateTitle = normalizeAnnualMediaTitle(String(detail?.Name || detail?.SeriesName || fallbackTitle).trim());
      aggregateKind = "series";
      coverSearchTitles = buildAnnualCoverSearchTitles(aggregateTitle, fallbackTitle);
    } else if (detailType === "movie") {
      aggregateId = itemId;
      coverItemId = itemId;
      aggregateTitle = normalizeAnnualMediaTitle(String(detail?.Name || fallbackTitle).trim());
      aggregateKind = "movie";
      coverSearchTitles = buildAnnualCoverSearchTitles(aggregateTitle, fallbackTitle);
    } else {
      aggregateId = itemId;
      coverItemId = itemId;
      aggregateTitle = fallbackTitle;
      aggregateType = fallbackType;
      aggregateKind = "title";
      coverSearchTitles = buildAnnualCoverSearchTitles(aggregateTitle);
    }

    const aggregateKey = buildAnnualAggregateKey(aggregateType, aggregateId, aggregateTitle, aggregateKind);

    const identity = {
      aggregateKey,
      aggregateId,
      aggregateType,
      aggregateKind,
      aggregateTitle,
      coverCacheKey: aggregateKey,
      coverItemId,
      coverSearchTitles,
      detailType,
      fallbackDurationMin
    };
    state.annualMediaIdentityCache[itemId] = identity;
    return identity;
  }

  async function enrichAnnualEventsWithMediaIdentity(events) {
    const list = events || [];
    const identityPromises = new Map();
    const getIdentityPromise = (event) => {
      const itemId = String(event?.itemId || "").trim();
      const fallbackTitle = normalizeAnnualMediaTitle(event?.title || "");
      const key = itemId ? `id:${itemId}` : `title:${normalizeMediaLookupKey(fallbackTitle)}`;
      if (!identityPromises.has(key)) {
        identityPromises.set(key, resolveAnnualMediaIdentityFromEvent(event));
      }
      return identityPromises.get(key);
    };
    const resolved = await Promise.all(
      list.map(async (event) => {
        const identity = await getIdentityPromise(event);
        return {
          ...event,
          durationMin: Math.max(0, Number(event?.durationMin) || 0) || Math.max(0, Number(identity?.fallbackDurationMin) || 0),
          aggregateKey: identity.aggregateKey,
          aggregateId: identity.aggregateId,
          aggregateType: identity.aggregateType,
          aggregateKind: identity.aggregateKind,
          aggregateTitle: identity.aggregateTitle,
          coverCacheKey: identity.coverCacheKey,
          coverItemId: identity.coverItemId,
          coverSearchTitles: identity.coverSearchTitles
        };
      })
    );
    return resolved;
  }

  function parseScopeUserName(scopeValue) {
    const value = String(scopeValue || "");
    if (!value.startsWith("user:")) {
      return "";
    }
    try {
      return decodeURIComponent(value.slice(5)).trim();
    } catch {
      return "";
    }
  }

  function buildScopeOptionsFromEvents(events) {
    const names = [];
    const seen = new Set();
    const pushName = (name) => {
      const normalized = String(name || "").trim();
      if (!normalized) {
        return;
      }
      const key = normalized.toLowerCase();
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      names.push(normalized);
    };

    (events || []).forEach((event) => pushName(event.userName));
    (appState.users || []).forEach((user) => pushName(user?.Name));

    return [
      { value: "all", label: "全服" },
      ...names.map((name) => ({ value: `user:${encodeURIComponent(name)}`, label: name }))
    ];
  }

  function filterEventsByScope(events) {
    const scopeUserName = parseScopeUserName(state.scope);
    if (!scopeUserName) {
      return events;
    }
    return (events || []).filter((event) => String(event.userName || "").toLowerCase() === scopeUserName.toLowerCase());
  }

  function aggregateEventsToRanking(events) {
    const map = new Map();
    (events || []).forEach((event) => {
      const normalizedType = normalizeAnnualMediaType(event.aggregateType || event.type || "other");
      const normalizedTitle = normalizeAnnualMediaTitle(event.aggregateTitle || event.title || "");
      const key =
        String(event.aggregateKey || "").trim() ||
        buildAnnualAggregateKey(normalizedType, event.aggregateId || "", normalizedTitle, event.aggregateKind || "");
      const current = map.get(key) || {
        id: key,
        coverCacheKey: String(event.coverCacheKey || key),
        itemId: event.coverItemId || event.aggregateId || event.itemId || "",
        title: normalizedTitle,
        type: normalizedType,
        playCount: 0,
        totalDurationMinutes: 0,
        lastPlayed: event.playedAt || "",
        posterUrl: event.posterUrl || "",
        coverSearchTitles: Array.isArray(event.coverSearchTitles) ? event.coverSearchTitles : [],
        coverResolveSource: "fallback",
        coverCandidateCount: 0,
        coverCandidatesTop: [],
        coverCandidatesList: []
      };

      current.playCount += 1;
      current.totalDurationMinutes += Math.max(0, Number(event.durationMin) || 0);
      if (!current.itemId && (event.coverItemId || event.aggregateId || event.itemId)) {
        current.itemId = event.coverItemId || event.aggregateId || event.itemId;
      }
      if (!current.posterUrl && event.posterUrl) {
        current.posterUrl = event.posterUrl;
      }
      const mergedSearchTitles = buildAnnualCoverSearchTitles(current.coverSearchTitles, event.coverSearchTitles, normalizedTitle);
      current.coverSearchTitles = mergedSearchTitles;
      if (current.type === "other" && normalizedType && normalizedType !== "other") {
        current.type = normalizedType;
      }
      if (!current.title || current.title === "未命名内容") {
        current.title = normalizedTitle;
      }
      const currentTime = new Date(current.lastPlayed || 0).getTime();
      const nextTime = new Date(event.playedAt || 0).getTime();
      if (Number.isFinite(nextTime) && nextTime > currentTime) {
        current.lastPlayed = event.playedAt || current.lastPlayed;
      }

      map.set(key, current);
    });

    return Array.from(map.values());
  }

  async function fetchActivityLogsPaged(maxCount = 2000, chunk = 200) {
    const firstResult = await embyFetch(`/System/ActivityLog/Entries?Limit=${chunk}&StartIndex=0`);
    const firstItems = Array.isArray(firstResult?.Items) ? firstResult.Items : Array.isArray(firstResult) ? firstResult : [];
    const total = Math.min(Math.max(0, Number(firstResult?.TotalRecordCount || firstItems.length || 0)), maxCount);
    if (!firstItems.length || firstItems.length >= total) {
      return firstItems.slice(0, maxCount);
    }

    const starts = [];
    for (let start = firstItems.length; start < total; start += chunk) {
      starts.push(start);
    }
    const pages = await Promise.all(
      starts.map((startIndex) =>
        embyFetch(`/System/ActivityLog/Entries?Limit=${chunk}&StartIndex=${startIndex}`)
          .then((result) => (Array.isArray(result?.Items) ? result.Items : Array.isArray(result) ? result : []))
          .catch(() => [])
      )
    );
    return [...firstItems, ...pages.flat()].slice(0, maxCount);
  }

  function normalizePlaybackEventsFromLogs(logs) {
    const keywords = ["播放", "观看", "play", "playback", "playing", "watched", "stream"];
    const events = [];
    const dedupe = new Set();

    (logs || []).forEach((log) => {
      const text = [log?.Name, log?.ShortOverview, log?.Overview, log?.Message, log?.Description].filter(Boolean).join(" ");
      const lower = text.toLowerCase();
      const matchesPlayback = keywords.some((keyword) => lower.includes(keyword));
      if (!matchesPlayback) {
        return;
      }

      const action = inferPlaybackAction(text);
      if (action === "pause") {
        return;
      }

      const title = extractMediaTitleCandidate(log) || extractTitleFromLog(log) || "";
      if (!title) {
        return;
      }

      const parsed = parseUserAndPlayerFromText(log);
      const userName = String(log?.UserName || log?.ByUserName || log?.User?.Name || parsed?.userName || "未知用户").trim();
      const playedAt = log?.Date || log?.StartDate || log?.DateCreated || "";
      const playedTime = new Date(playedAt).getTime();
      if (!Number.isFinite(playedTime) || playedTime <= 0) {
        return;
      }

      const itemId = String(log?.ItemId || log?.Item?.Id || "").trim();
      const duration = parsePlaybackDuration(log, text);
      const dedupeKey = `${itemId || normalizeMediaLookupKey(title)}|${userName.toLowerCase()}|${Math.floor(playedTime / 60000)}`;
      if (dedupe.has(dedupeKey)) {
        return;
      }
      dedupe.add(dedupeKey);

      events.push({
        title: title.trim(),
        userName,
        itemId,
        playedAt,
        durationMin: duration.minutes || 0,
        type: getTypeFromLog(log, title),
        posterUrl: ""
      });
    });

    return events;
  }

  async function fetchFallbackRankingItems() {
    const [movieResult, seriesResult] = await Promise.all([
      embyFetch(
        "/Items?Recursive=true&IncludeItemTypes=Movie&Fields=PlayCount,RunTimeTicks,DateLastPlayed,DateLastContentAdded,DateCreated,Type&SortBy=PlayCount&SortOrder=Descending&Limit=600"
      ).catch(() => ({ Items: [] })),
      embyFetch(
        "/Items?Recursive=true&IncludeItemTypes=Season,Series&Fields=PlayCount,RunTimeTicks,DateLastPlayed,DateLastContentAdded,DateCreated,Type,SeriesName,ParentIndexNumber,IndexNumber&SortBy=PlayCount&SortOrder=Descending&Limit=600"
      ).catch(() => ({ Items: [] }))
    ]);

    const movieItems = Array.isArray(movieResult?.Items) ? movieResult.Items : Array.isArray(movieResult) ? movieResult : [];
    const seriesItems = Array.isArray(seriesResult?.Items) ? seriesResult.Items : Array.isArray(seriesResult) ? seriesResult : [];

    return dedupeById([...movieItems, ...seriesItems].map(normalizeRankingItem)).filter(
      (item) => item.playCount > 0 || item.totalDurationMinutes > 0
    );
  }

  function rebuildItemsFromCurrentScope() {
    const scopedEvents = filterEventsByScope(state.events || []);
    state.items = aggregateEventsToRanking(scopedEvents);
    applyFilterAndSort();
  }

  function applyFilterAndSort() {
    const next = state.items
      .filter((item) => {
        if (state.category === "movie") {
          return item.type === "movie";
        }
        if (state.category === "series") {
          return item.type === "series";
        }
        return true;
      })
      .sort((a, b) => {
        if (state.sortBy === "duration") {
          return b.totalDurationMinutes - a.totalDurationMinutes || b.playCount - a.playCount;
        }
        return b.playCount - a.playCount || b.totalDurationMinutes - a.totalDurationMinutes;
      })
      .slice(0, MAX_RANKING_ITEMS);
    state.filteredItems = next;
  }

  async function fetchAnnualRankingData() {
    const params = new URLSearchParams({
      scope: state.scope || "all",
      category: state.category || "global",
      sortBy: state.sortBy || "playCount",
      limit: String(MAX_RANKING_ITEMS),
      ttlSeconds: String(Math.round(getAnnualCacheTtlMs() / 1000))
    });
    const response = await fetch(`/api/ranking/annual?${params.toString()}`, {
      method: "GET",
      cache: "no-store",
      headers: {
        Accept: "application/json"
      }
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    if (!response.ok || !payload?.ok) {
      throw new Error(payload?.error || `排行榜接口请求失败 (${response.status})`);
    }

    state.source = payload.cached ? "backend-cache" : "backend";
    state.events = [];
    state.rawLogCount = Math.max(0, Number(payload.rawLogCount) || 0);
    state.matchedEventCount = Math.max(0, Number(payload.matchedEventCount) || 0);
    state.viewCount = Math.max(0, Number(payload.viewCount) || 0);
    state.scopeOptions =
      Array.isArray(payload.scopeOptions) && payload.scopeOptions.length ? payload.scopeOptions : [{ value: "all", label: "全服" }];
    state.scope = String(payload.scope || state.scope || "all");
    if (!state.scopeOptions.some((option) => option.value === state.scope)) {
      state.scope = "all";
    }
    state.items = Array.isArray(payload.items) ? payload.items.map(normalizeAnnualBackendItem) : [];
    state.lastUpdatedAt = String(payload.generatedAt || new Date().toISOString());
    applyFilterAndSort();
    persistAnnualRankingCache();
  }

  function buildTopCard(item, rankIndex) {
    if (!item) {
      return `
        <article class="annual-ranking-top-card is-empty rank-${rankIndex}">
          <div class="annual-ranking-empty-card">暂无数据</div>
        </article>
      `;
    }

    const rankNo = rankIndex + 1;
    const coverMarkup = buildAnnualCoverImageHtml(item, "annual-ranking-top-cover", "top");
    return `
      <article class="annual-ranking-top-card rank-${rankNo}">
        <span class="annual-ranking-top-rank-label">TOP ${rankNo}</span>
        <div class="annual-ranking-top-cover-wrap">
          ${coverMarkup || `<div class="annual-ranking-top-cover annual-ranking-top-cover-fallback">No Poster</div>`}
        </div>
        <div class="annual-ranking-top-overlay">
          <h4>${escapeAnnualHtml(item.title)}</h4>
          <p>
            <span>▶ ${escapeAnnualHtml(formatPlayCountLabel(item.playCount))}</span>
            <span>⏱ ${escapeAnnualHtml(formatMinutesLabel(item.totalDurationMinutes))}</span>
          </p>
        </div>
      </article>
    `;
  }

  function renderTop3() {
    if (!refs.top3Host) {
      return;
    }
    const topItems = state.filteredItems.slice(0, 3);
    const ordered = [topItems[1] || null, topItems[0] || null, topItems[2] || null];
    refs.top3Host.innerHTML = ordered
      .map((item, index) => {
        const sourceRank = index === 1 ? 0 : index === 0 ? 1 : 2;
        return buildTopCard(item, sourceRank);
      })
      .join("");
    bindAnnualCoverFallbacks(refs.top3Host);
  }

  function renderAnnualSkeleton() {
    if (refs.top3Host) {
      refs.top3Host.innerHTML = [0, 1, 2]
        .map(
          (index) => `
            <article class="annual-ranking-top-card annual-ranking-skeleton-card rank-${index + 1}">
              <div class="annual-ranking-skeleton-block annual-ranking-skeleton-cover"></div>
              <div class="annual-ranking-top-overlay">
                <span class="annual-ranking-skeleton-line short"></span>
                <span class="annual-ranking-skeleton-line title"></span>
                <span class="annual-ranking-skeleton-line meta"></span>
              </div>
            </article>
          `
        )
        .join("");
    }
    if (refs.listHost) {
      refs.listHost.innerHTML = [0, 1, 2, 3]
        .map(
          () => `
            <article class="annual-ranking-row annual-ranking-row-skeleton">
              <div class="annual-ranking-skeleton-line rank"></div>
              <div class="annual-ranking-row-main">
                <div class="annual-ranking-skeleton-block annual-ranking-row-cover"></div>
                <div class="annual-ranking-row-text">
                  <span class="annual-ranking-skeleton-line title"></span>
                  <span class="annual-ranking-skeleton-line meta"></span>
                </div>
              </div>
              <div class="annual-ranking-skeleton-line date"></div>
            </article>
          `
        )
        .join("");
    }
  }

  function renderList() {
    if (!refs.listHost) {
      return;
    }
    const listItems = state.filteredItems.slice(3, MAX_RANKING_ITEMS);
    if (!listItems.length) {
      if (state.source === "logs") {
        refs.listHost.innerHTML = `<div class="annual-ranking-empty">暂无入围数据，请调整用户筛选或等待新的播放日志。</div>`;
      } else if (state.source === "items") {
        refs.listHost.innerHTML = `<div class="annual-ranking-empty">暂无入围数据，播放计数尚未形成有效排名。</div>`;
      } else {
        refs.listHost.innerHTML = `<div class="annual-ranking-empty">暂无播放事件，连接 Emby 后会自动生成榜单。</div>`;
      }
      return;
    }
    refs.listHost.innerHTML = listItems
      .map((item, idx) => {
        const rank = idx + 1;
        const coverMarkup = buildAnnualCoverImageHtml(item, "annual-ranking-row-cover", "list");
        return `
          <article class="annual-ranking-row">
            <div class="annual-ranking-rank">${rank}</div>
            <div class="annual-ranking-row-main">
              <div class="annual-ranking-row-cover-wrap">
                ${coverMarkup || `<div class="annual-ranking-row-cover annual-ranking-row-cover-fallback">No</div>`}
              </div>
              <div class="annual-ranking-row-text">
                <h5>${escapeAnnualHtml(item.title)}</h5>
                <p>
                  <span>▶ ${escapeAnnualHtml(formatPlayCountLabel(item.playCount))}</span>
                  <span>⏱ ${escapeAnnualHtml(formatMinutesLabel(item.totalDurationMinutes))}</span>
                </p>
              </div>
            </div>
            <div class="annual-ranking-row-meta">${escapeAnnualHtml(formatShortDateTime(item.lastPlayed))}</div>
          </article>
        `;
      })
      .join("");
    bindAnnualCoverFallbacks(refs.listHost);
  }

  function syncActiveButtons() {
    if (!refs.controls) {
      return;
    }
    refs.controls.querySelectorAll("[data-annual-category]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-annual-category") === state.category);
    });
    refs.controls.querySelectorAll("[data-annual-sort]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-annual-sort") === state.sortBy);
    });
    const scope = refs.controls.querySelector("#annual-ranking-scope");
    if (scope instanceof HTMLSelectElement) {
      scope.innerHTML = (state.scopeOptions || [{ value: "all", label: "全服" }])
        .map((option) => `<option value="${escapeAnnualHtml(option.value)}">${escapeAnnualHtml(option.label)}</option>`)
        .join("");
      if (!state.scopeOptions.some((option) => option.value === state.scope)) {
        state.scope = "all";
      }
      scope.value = state.scope;
    }
  }

  function renderAnnualRanking() {
    if (!refs.root || !refs.status) {
      return;
    }
    refs.root.hidden = false;
    syncActiveButtons();

    if (!isConnected()) {
      refs.status.textContent = "未连接 Emby，等待同步";
      if (refs.top3Host) {
        refs.top3Host.innerHTML = `<div class="annual-ranking-empty">请先在系统设置中连接 Emby。</div>`;
      }
      if (refs.listHost) {
        refs.listHost.innerHTML = "";
      }
      return;
    }

    if (state.loading) {
      refs.status.textContent = state.filteredItems.length ? "正在后台更新榜单数据..." : "正在同步榜单数据...";
      if (!state.filteredItems.length) {
        renderAnnualSkeleton();
        return;
      }
    }

    if (state.error) {
      refs.status.textContent = `同步失败：${state.error}`;
    } else if (state.lastUpdatedAt) {
      const coverStats = getAnnualCoverHitStats(state.filteredItems);
      const coverText = coverStats.total > 0 ? ` · 封面 ${coverStats.hits}/${coverStats.total}` : "";
      if (state.source === "backend" || state.source === "backend-cache") {
        const cacheText = state.source === "backend-cache" ? " · 命中服务端缓存" : "";
        refs.status.textContent = `最近更新：${formatShortDateTime(state.lastUpdatedAt)} · 日志 ${state.rawLogCount} · 命中 ${state.matchedEventCount}${coverText}${cacheText}`;
      } else if (state.source === "logs") {
        refs.status.textContent = `最近更新：${formatShortDateTime(state.lastUpdatedAt)} · 日志 ${state.rawLogCount} · 命中 ${state.matchedEventCount}${coverText}`;
      } else if (state.source === "items") {
        refs.status.textContent = `最近更新：${formatShortDateTime(state.lastUpdatedAt)} · 已使用播放计数兜底${coverText}`;
      } else {
        refs.status.textContent = `最近更新：${formatShortDateTime(state.lastUpdatedAt)} · 暂无播放事件${coverText}`;
      }
    } else {
      refs.status.textContent = "等待同步";
    }

    renderTop3();
    renderList();
  }

  function bindControlEvents() {
    if (!refs.controls) {
      return;
    }
    refs.controls.querySelectorAll("[data-annual-category]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.category = btn.getAttribute("data-annual-category") || "global";
        applyFilterAndSort();
        renderAnnualRanking();
        refreshAnnualRanking({ manual: false });
      });
    });
    refs.controls.querySelectorAll("[data-annual-sort]").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.sortBy = btn.getAttribute("data-annual-sort") || "playCount";
        applyFilterAndSort();
        renderAnnualRanking();
        refreshAnnualRanking({ manual: false });
      });
    });
    const scope = refs.controls.querySelector("#annual-ranking-scope");
    if (scope instanceof HTMLSelectElement) {
      scope.addEventListener("change", () => {
        state.scope = scope.value || "all";
        applyFilterAndSort();
        renderAnnualRanking();
        refreshAnnualRanking({ manual: false });
      });
    }
    const refreshBtn = refs.controls.querySelector("#annual-ranking-refresh-btn");
    refreshBtn?.addEventListener("click", async () => {
      await refreshAnnualRanking({ manual: true });
    });
  }

  function ensureMounted() {
    if (refs.root) {
      return true;
    }
    const panel = document.querySelector("#view-content-ranking .panel");
    if (!(panel instanceof HTMLElement)) {
      return false;
    }
    const tableWrap = panel.querySelector(".user-table-wrap");
    if (!(tableWrap instanceof HTMLElement)) {
      return false;
    }

    const root = document.createElement("section");
    root.id = "annual-ranking-module";
    root.className = "annual-ranking-module";
    root.innerHTML = `
      <div class="annual-ranking-head">
        <div class="annual-ranking-head-left">
          <h3>🏆 年度风云榜</h3>
          <p>全服播放热度与时长综合排行</p>
        </div>
        <div class="annual-ranking-controls">
          <div class="annual-ranking-tabs">
            <button type="button" data-annual-category="global" class="active">全局</button>
            <button type="button" data-annual-category="movie">电影</button>
            <button type="button" data-annual-category="series">剧集</button>
          </div>
          <div class="annual-ranking-tabs">
            <button type="button" data-annual-sort="playCount" class="active">🔥 播放量</button>
            <button type="button" data-annual-sort="duration">🕒 总时长</button>
          </div>
          <label class="annual-ranking-scope-wrap">
            <span>🌏</span>
            <select id="annual-ranking-scope">
              <option value="all">全服</option>
            </select>
          </label>
          <button id="annual-ranking-refresh-btn" type="button" class="annual-ranking-refresh-btn">刷新</button>
        </div>
      </div>
      <p id="annual-ranking-status" class="annual-ranking-status">等待同步...</p>
      <div id="annual-ranking-top3" class="annual-ranking-top3"></div>
      <div class="annual-ranking-list-head">
        <h4>📋 影片入围名单</h4>
      </div>
      <div id="annual-ranking-list" class="annual-ranking-list"></div>
    `;

    panel.insertBefore(root, tableWrap);
    tableWrap.hidden = true;
    tableWrap.style.display = "none";
    refs.root = root;
    refs.controls = root.querySelector(".annual-ranking-controls");
    refs.top3Host = root.querySelector("#annual-ranking-top3");
    refs.listHost = root.querySelector("#annual-ranking-list");
    refs.status = root.querySelector("#annual-ranking-status");
    bindControlEvents();
    return true;
  }

  async function refreshAnnualRanking(options = {}) {
    const { manual = false } = options;
    if (state.refreshPromise) {
      return state.refreshPromise;
    }
    if (!ensureMounted()) {
      return;
    }
    if (!isConnected()) {
      state.error = "";
      state.source = "none";
      state.events = [];
      state.scopeOptions = [{ value: "all", label: "全服" }];
      state.rawLogCount = 0;
      state.matchedEventCount = 0;
      state.items = [];
      state.filteredItems = [];
      renderAnnualRanking();
      return;
    }

    state.loading = true;
    state.error = "";
    renderAnnualRanking();
    state.refreshPromise = (async () => {
      try {
        await fetchAnnualRankingData();
        state.error = "";
        if (manual) {
          showToast("年度风云榜已刷新", 1000);
        }
      } catch (error) {
        state.error = error?.message || "未知错误";
        if (manual) {
          showToast("年度风云榜刷新失败", 1200);
        }
      } finally {
        state.loading = false;
        state.refreshPromise = null;
        renderAnnualRanking();
      }
    })();
    return state.refreshPromise;
  }

  function startPolling() {
    if (state.timerId) {
      return;
    }
    state.timerId = window.setInterval(() => {
      refreshAnnualRanking({ manual: false });
    }, POLL_INTERVAL_MS);
  }

  function stopPolling() {
    if (!state.timerId) {
      return;
    }
    window.clearInterval(state.timerId);
    state.timerId = null;
  }

  function onViewChange() {
    const view = getCurrentView();
    if (state.activeView === view && state.mounted) {
      return;
    }
    state.activeView = view;
    if (view === "content-ranking") {
      ensureMounted();
      renderAnnualRanking();
      refreshAnnualRanking({ manual: false });
      startPolling();
    } else {
      stopPolling();
    }
  }

  function observeViewChange() {
    if (!elements.mainContent) {
      return;
    }
    const observer = new MutationObserver(() => {
      onViewChange();
    });
    observer.observe(elements.mainContent, { attributes: true, attributeFilter: ["data-active-view"] });
  }

  function bindExternalRefresh() {
    elements.connectBtn?.addEventListener("click", () => {
      window.setTimeout(() => {
        if (getCurrentView() === "content-ranking") {
          refreshAnnualRanking({ manual: false });
        }
      }, 300);
    });
  }

  function init() {
    if (state.mounted) {
      return;
    }
    state.mounted = true;
    ensureMounted();
    restoreAnnualRankingCache();
    renderAnnualRanking();
    observeViewChange();
    bindExternalRefresh();
    refreshAnnualRanking({ manual: false });
    onViewChange();
  }

  init();
})();
