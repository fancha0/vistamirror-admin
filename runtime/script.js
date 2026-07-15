const STORAGE_KEYS = {
  config: "embyPulseConfig",
  invites: "embyPulseInvites",
  renewals: "embyPulseRenewals",
  botConfig: "embyPulseBotConfig",
  notificationConfig: "vistamirrorNotificationConfig",
  aiConfig: "vistamirrorAiConfig",
  coverStudioConfig: "vistamirrorCoverStudioConfig",
  libraryDirectoryConfig: "vistamirrorLibraryDirectoryConfig",
  drive115Config: "vistamirrorDrive115Config",
  hdhiveConfig: "vistamirrorHDHiveConfig",
  activeView: "embyPulseActiveView",
  coverStudioMode: "vistamirrorCoverStudioMode",
  inviteSyncEndpoint: "embyPulseInviteSyncEndpoint",
  qualityLastScanAt: "embyPulseQualityLastScanAt",
  sidebarCollapsed: "vistamirrorSidebarCollapsed"
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
  wechatCallbackAes: "",
  libraryTemplates: {
    single:
      "🎬 【{{entry_label}}】｜ {{title}}{{year_suffix}}\n\n{{summary_line}}\n{{tagline_line}}\n\n= 📦基础参数 =\n{{content_type_line}}\n{{library_scope_line}}\n{{library_time_line}}\n\n= 💽资源详情 =\n{{quality_line}}\n{{actors_line}}\n\n= 📖内容简介 =\n{{overview}}",
    grouped:
      "📺 【{{entry_label}}】｜ {{title}}{{year_suffix}}\n\n{{summary_line}}\n{{tagline_line}}\n\n= 📦基础参数 =\n{{content_type_line}}\n{{library_scope_line}}\n{{library_time_line}}\n\n= 💽资源详情 =\n{{quality_line}}\n{{actors_line}}\n\n= 📖内容简介 =\n{{overview}}"
  }
};

const DEFAULT_NOTIFICATION_CONFIG = {
  enabled: true,
  channels: {
    telegram: {
      enabled: true,
      botToken: "",
      chatId: "",
      enableCommands: true,
      proxyUrl: ""
    },
    wecom: {
      enabled: false,
      corpId: "",
      agentId: "",
      secret: "",
      toUser: "@all",
      callbackToken: "",
      callbackAes: "",
      callbackUrl: "",
      proxyUrl: ""
    }
  },
  routes: {
    telegram: {
      "playback.start": true,
      "playback.pause": true,
      "playback.resume": true,
      "playback.stop": true,
      "library.single": true,
      "library.grouped": true
    },
    wecom: {
      "playback.start": false,
      "playback.pause": false,
      "playback.resume": false,
      "playback.stop": false,
      "library.single": false,
      "library.grouped": false
    }
  },
  templates: {
    telegram: {
      "playback.start":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "playback.pause":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "playback.resume":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "playback.stop":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "library.single": DEFAULT_BOT_CONFIG.libraryTemplates.single,
      "library.grouped": DEFAULT_BOT_CONFIG.libraryTemplates.grouped
    },
    wecom: {
      "playback.start":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "playback.pause":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "playback.resume":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "playback.stop":
        "{{title_line}}\n\n{{user_line}}\n{{playback_method_line}}\n{{media_spec_line}}\n━━━━━━━━━━━━━━━━━━━━\n📋 播放数据\n{{rating_line}}\n{{progress_line}}\n\n🛋️ 终端状态\n{{device_line}}\n{{ip_line}}\n{{time_line}}\n\n━━━━━━━━━━━━━━━━━━━━\n{{overview_line}}",
      "library.single": DEFAULT_BOT_CONFIG.libraryTemplates.single,
      "library.grouped": DEFAULT_BOT_CONFIG.libraryTemplates.grouped
    }
  },
  display: {
    telegram: {
      "playback.start": { label: "开始播放", description: "用户开始播放媒体时发送通知。" },
      "playback.pause": { label: "暂停播放", description: "用户暂停播放时发送通知。" },
      "playback.resume": { label: "恢复播放", description: "用户恢复播放时发送通知。" },
      "playback.stop": { label: "停止播放", description: "用户停止播放时发送通知。" },
      "library.single": { label: "入库通知", description: "单个电影或单集入库时发送通知。" },
      "library.grouped": { label: "剧集聚合入库", description: "同一剧集短时间多集入库时合并发送。" }
    },
    wecom: {
      "playback.start": { label: "开始播放", description: "用户开始播放媒体时发送通知。" },
      "playback.pause": { label: "暂停播放", description: "用户暂停播放时发送通知。" },
      "playback.resume": { label: "恢复播放", description: "用户恢复播放时发送通知。" },
      "playback.stop": { label: "停止播放", description: "用户停止播放时发送通知。" },
      "library.single": { label: "入库通知", description: "单个电影或单集入库时发送通知。" },
      "library.grouped": { label: "剧集聚合入库", description: "同一剧集短时间多集入库时合并发送。" }
    }
  },
  runtime: {
    dedupeSeconds: 10,
    playback: {
      showIp: true,
      showIpGeo: true,
      showOverview: true,
      userScope: {
        mode: "all",
        selectedUserNames: [],
        selectedUsersMeta: []
      }
    }
  }
};

const DEFAULT_NOTIFICATION_CAPABILITIES = {
  channels: {
    telegram: { label: "Telegram", supportsCommands: true },
    wecom: { label: "企业微信", supportsCommands: false }
  },
  events: [],
  upcomingEvents: []
};

const TOPBAR_VISIBLE_VIEWS = new Set(["logs", "user-center", "task-center", "media-config", "ai-config"]);

const DEFAULT_AI_CONFIG = {
  enabled: false,
  baseUrl: "https://api.openai.com/v1",
  apiKey: "",
  model: "gpt-4o-mini",
  temperature: 0.4,
  maxTokens: 800,
  contextTokensK: 64
};

const DEFAULT_DRIVE115_CONFIG = {
  enabled: false,
  cookie: "",
  defaultCid: "0",
  hasCookie: false,
  cookieMasked: ""
};

const DEFAULT_COVER_STUDIO_FONT_KEY = "heiti";
const REMOVED_COVER_STUDIO_FONT_KEYS = new Set([
  "hiragino",
  "noteworthy",
  "avenir",
  "fu_lu_da_mao_bi_ti",
  "the_mordeus"
]);

function normalizeCoverStudioFontKey(value) {
  const key = String(value || "").trim();
  return !key || REMOVED_COVER_STUDIO_FONT_KEYS.has(key) ? DEFAULT_COVER_STUDIO_FONT_KEY : key;
}

const DEFAULT_COVER_STUDIO_CONFIG = {
  currentPresetId: "default",
  lastViewId: "",
  draft: {
    viewId: "",
    viewIds: [],
    templateKey: "fan_spread",
    pickMode: "random",
    titleText: "",
    subtitleText: "",
    fontKey: DEFAULT_COVER_STUDIO_FONT_KEY,
    titleFontSize: 108,
    subtitleFontSize: 44,
    presetName: "默认封面",
    titleAlign: "left",
    overlayStrength: 0,
    posterCount: 5,
    accentTone: "blue",
    posterRotation: 42,
    titleYOffset: 0,
    lockedItemIds: []
  },
  scheduleDraft: {
    templateKey: "fan_spread",
    pickMode: "random",
    titleText: "",
    subtitleText: "",
    fontKey: DEFAULT_COVER_STUDIO_FONT_KEY,
    titleFontSize: 108,
    subtitleFontSize: 44,
    titleAlign: "left",
    posterCount: 5,
    accentTone: "blue",
    posterRotation: 42,
    titleYOffset: 0
  },
  presets: [
    {
      id: "default",
      name: "默认封面",
      templateKey: "fan_spread",
      pickMode: "random",
      titleText: "",
      subtitleText: "",
      fontKey: DEFAULT_COVER_STUDIO_FONT_KEY,
      titleFontSize: 108,
      subtitleFontSize: 44,
      titleAlign: "left",
      overlayStrength: 0,
      posterCount: 5,
      accentTone: "blue",
      posterRotation: 42,
      titleYOffset: 0,
      lockedItemIds: []
    }
  ],
  backups: {},
  schedule: {
    enabled: false,
    cron: "0 */6 * * *",
    lastRunAt: "",
    lastStatus: "idle",
    lastMessage: "未启用自动更新。",
    lastResultCount: 0
  },
  schedules: []
};

// Keep cover titles in sync with the selected Emby library. The Chinese title
// always comes from Emby; the subtitle is only a lightweight display hint.
const COVER_STUDIO_LIBRARY_SUBTITLES = [
  ["国产动漫", "Chinese Animation"],
  ["动漫电影", "Anime Movies"],
  ["动漫剧集", "Anime Series"],
  ["华语电影", "Chinese Movies"],
  ["华语剧集", "Chinese Series"],
  ["欧美电影", "Western Movies"],
  ["欧美剧集", "Western Series"],
  ["亚洲电影", "Asian Movies"],
  ["亚洲剧集", "Asian Series"],
  ["纪录片电影", "Documentary Movies"],
  ["纪录片剧集", "Documentary Series"],
  ["综艺", "Variety Shows"],
  ["合集", "Collections"],
  ["动画电影", "Animation Movies"],
  ["动画剧集", "Animation Series"],
  ["动画", "Animation"],
  ["动漫", "Animation"]
];

const DEFAULT_COVER_STUDIO_MODES = [
  {
    key: "fan_spread",
    label: "扇形展开",
    description: "多张海报像扇面一样展开，适合动画和剧集类视图。",
    supports: ["titleAlign", "posterCount", "accentTone", "posterRotation", "titleYOffset"],
    maxPosterCount: 8,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 6, accentTone: "gold", posterRotation: 68, titleYOffset: -12 }
  },
  {
    key: "banner_showcase",
    label: "横幅橱窗",
    description: "大背景主视觉加底部海报陈列，适合做流媒体风格分类封面。",
    supports: ["titleAlign", "posterCount", "accentTone", "titleYOffset"],
    maxPosterCount: 5,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 5, accentTone: "gold", posterRotation: 0, titleYOffset: 0 }
  },
  {
    key: "hero_showcase",
    label: "主视觉橱窗",
    description: "强化主视觉人物与灯光层次，适合剧集与国漫的首页封面。",
    supports: ["titleAlign", "posterCount", "accentTone", "posterRotation", "titleYOffset"],
    maxPosterCount: 5,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 5, accentTone: "blue", posterRotation: 12, titleYOffset: 0 }
  },
  {
    key: "gallery_wall_showcase",
    label: "海报陈列墙",
    description: "用更规整的海报橱窗形成分类感，适合电影库与动漫库封面。",
    supports: ["titleAlign", "posterCount", "accentTone", "posterRotation", "titleYOffset"],
    maxPosterCount: 6,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 6, accentTone: "emerald", posterRotation: 8, titleYOffset: 0 }
  },
  {
    key: "immersive_stage",
    label: "沉浸展映台",
    description: "深色影院舞台感与倒影灯光更强，适合突出沉浸式流媒体封面。",
    supports: ["titleAlign", "posterCount", "accentTone", "posterRotation", "titleYOffset"],
    maxPosterCount: 5,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 5, accentTone: "rose", posterRotation: 16, titleYOffset: 0 }
  },
  {
    key: "bookshelf_gallery",
    label: "书架陈列",
    description: "暖色书架与直立海报陈列，适合纪录片、电影精选与剧集分类。",
    supports: ["titleAlign", "posterCount", "accentTone", "titleYOffset"],
    maxPosterCount: 7,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 7, accentTone: "gold", posterRotation: 0, titleYOffset: 0 }
  },
  {
    key: "honeycomb_hex",
    label: "蜂巢六边形",
    description: "以六边形影像网格聚焦内容，适合动画、动作和科幻类媒体库。",
    supports: ["titleAlign", "posterCount", "accentTone", "titleYOffset"],
    maxPosterCount: 7,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 7, accentTone: "blue", posterRotation: 0, titleYOffset: 0 }
  },
  {
    key: "panorama_gallery",
    label: "全景画廊",
    description: "弧形展墙配合地面倒影，适合做沉浸式电影与剧集分类封面。",
    supports: ["titleAlign", "posterCount", "accentTone", "titleYOffset"],
    maxPosterCount: 7,
    defaults: { titleAlign: "left", overlayStrength: 0, posterCount: 7, accentTone: "gold", posterRotation: 0, titleYOffset: 0 }
  }
];

const DEFAULT_COVER_STUDIO_ACCENT_TONES = [
  { key: "blue", label: "海蓝" },
  { key: "gold", label: "鎏金" },
  { key: "emerald", label: "翡翠" },
  { key: "rose", label: "玫瑰" },
  { key: "neutral", label: "冷灰" }
];

const DEFAULT_COVER_STUDIO_TITLE_ALIGN_OPTIONS = [
  { key: "left", label: "左对齐" },
  { key: "center", label: "居中" },
  { key: "right", label: "右对齐" }
];

const DEFAULT_LIBRARY_DIRECTORY_CONFIG = {
  roots: []
};

const DEFAULT_HDHIVE_CONFIG = {
  enabled: false,
  authMode: "broker",
  brokerUrl: "",
  installationId: "",
  registered: false,
  oauthSessionId: "",
  oauthSessionExpiresAt: 0,
  autoCheckin: true,
  timezone: "Asia/Shanghai",
  lastCheckin: {},
  lastCheckinDate: "",
  clientId: "",
  redirectUri: "",
  hasAppSecret: false,
  appSecretMasked: "",
  authorized: false,
  accessExpiresAt: 0,
  scopes: "meta query unlock write",
  user: {}
};

const DEFAULT_EMBY_CLIENT_NAME = "VistaMirror User Console";
const MEDIA_SERVER_TYPES = ["emby", "jellyfin"];
const MEDIA_SERVER_META = {
  emby: {
    label: "Emby",
    apiName: "Emby",
    placeholder: "例如 http://192.168.1.10:8096 或 https://demo.example.com/emby",
    keyPlaceholder: "输入 Emby 后台生成的 API Key"
  },
  jellyfin: {
    label: "Jellyfin",
    apiName: "Jellyfin",
    placeholder: "例如 http://192.168.1.10:8096 或 https://jellyfin.example.com",
    keyPlaceholder: "输入 Jellyfin 后台生成的 API Key"
  }
};
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
  notificationConfig: loadJson(STORAGE_KEYS.notificationConfig, DEFAULT_NOTIFICATION_CONFIG),
  aiConfig: loadJson(STORAGE_KEYS.aiConfig, DEFAULT_AI_CONFIG),
  coverStudioConfig: loadJson(STORAGE_KEYS.coverStudioConfig, DEFAULT_COVER_STUDIO_CONFIG),
  coverStudioMode: localStorage.getItem(STORAGE_KEYS.coverStudioMode) === "auto" ? "auto" : "manual",
  coverStudioScheduleDraft: null,
  coverStudioSchedulePreviewDataUrl: "",
  coverStudioSchedulePreviewLoading: false,
  coverStudioEditingScheduleId: "",
  libraryDirectoryConfig: loadJson(STORAGE_KEYS.libraryDirectoryConfig, DEFAULT_LIBRARY_DIRECTORY_CONFIG),
  drive115Config: loadJson(STORAGE_KEYS.drive115Config, DEFAULT_DRIVE115_CONFIG),
  hdhiveConfig: loadJson(STORAGE_KEYS.hdhiveConfig, DEFAULT_HDHIVE_CONFIG),
  hdhiveResources: [],
  hdhiveIdentity: null,
  hdhiveRecords: [],
  drive115Records: [],
  drive115LastParse: null,
  drive115QrSessionId: "",
  drive115QrTimer: null,
  drive115QrStartedAt: 0,
  drive115QrPolling: false,
  drive115QrFailureCount: 0,
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
  playbackHistoryRows: [],
  playbackHistoryDebug: {},
  playbackHistoryWarning: "",
  playbackHistoryLoading: false,
  coverStudioFonts: [],
  coverStudioModes: [],
  coverStudioAccentTones: [],
  coverStudioTitleAlignOptions: [],
  coverStudioViews: [],
  coverStudioPreviewToken: "",
  coverStudioPreviewDataUrl: "",
  coverStudioPreviews: [],
  coverStudioSelectedItems: [],
  coverStudioLoading: false,
  missingRows: [],
  missingSummary: {
    scannedSeries: 0,
    matchedTmdbSeries: 0,
    missingSeries: 0,
    missingEpisodeCount: 0,
    unknownMatchCount: 0,
    scannedAt: ""
  },
  missingWarnings: [],
  missingLoading: false,
  missingScannedOnce: false,
  projectLogs: [],
  projectLogTotal: 0,
  projectLogFilters: {
    level: "",
    module: "",
    keyword: ""
  },
  projectLogSearchTimer: null,
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
  sidebarCollapsed: localStorage.getItem(STORAGE_KEYS.sidebarCollapsed) === "1",
  inviteSyncEndpoint: localStorage.getItem(STORAGE_KEYS.inviteSyncEndpoint) || "",
  botWebhookUrl: "",
  botWebhookState: null,
  botWebhookWarning: "",
  envControlledFields: {
    embyConfig: [],
    botConfig: [],
    aiConfig: [],
    adminAuth: []
  },
  adminCredentialMeta: {
    username: "",
    managedByEnv: false,
    allowUpdate: false,
    source: "missing"
  },
  adminCredentialLoading: false,
  botWebhookRefreshPromise: null,
  botWebhookStatusTimer: null,
  notificationCapabilities: DEFAULT_NOTIFICATION_CAPABILITIES,
  notificationChannelMenuOpen: false,
  notificationChannelModalChannel: "",
  notificationChannelDraftChannel: "",
  notificationWorkspaceChannel: "telegram",
  notificationWorkspaceEvent: "",
  notificationModalSection: "channel",
  qualityRescanPromise: null,
  qualityLastScanAt: Number(localStorage.getItem(STORAGE_KEYS.qualityLastScanAt) || 0) || 0,
  liveSessionsRefreshPromise: null,
  liveSessionsTimer: null,
  authEnabled: false,
  authenticated: false,
  authUser: "",
  authBootstrapped: false
};

let postAuthBootstrapPromise = null;

if (appState.activeView === "project-logs" || appState.activeView === "settings") {
  appState.activeView = "media-config";
  localStorage.setItem(STORAGE_KEYS.activeView, "media-config");
}

function normalizeAppConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const activeServerType = MEDIA_SERVER_TYPES.includes(config.activeServerType) ? config.activeServerType : "emby";
  const rawServers = config.mediaServers && typeof config.mediaServers === "object" ? config.mediaServers : {};
  const embyConfig = rawServers.emby && typeof rawServers.emby === "object" ? rawServers.emby : config;
  const jellyfinConfig = rawServers.jellyfin && typeof rawServers.jellyfin === "object" ? rawServers.jellyfin : {};
  const mediaServers = {
    emby: normalizeMediaServerConfig(embyConfig),
    jellyfin: normalizeMediaServerConfig(jellyfinConfig)
  };
  const activeConfig = mediaServers[activeServerType] || mediaServers.emby;
  return {
    activeServerType,
    mediaServers,
    serverUrl: activeConfig.serverUrl,
    apiKey: activeConfig.apiKey,
    clientName: DEFAULT_EMBY_CLIENT_NAME,
    tmdbEnabled: Boolean(config.tmdbEnabled),
    tmdbToken: String(config.tmdbToken || "").trim(),
    tmdbLanguage: String(config.tmdbLanguage || "zh-CN").trim() || "zh-CN",
    tmdbRegion: String(config.tmdbRegion || "CN").trim().toUpperCase() || "CN"
  };
}

function normalizeMediaServerConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  return {
    serverUrl: String(config.serverUrl || "").trim(),
    apiKey: String(config.apiKey || "").trim(),
    clientName: DEFAULT_EMBY_CLIENT_NAME
  };
}

function normalizeBotConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const defaults = DEFAULT_BOT_CONFIG;
  const notifyEventsSource =
    config.notifyEvents && typeof config.notifyEvents === "object" ? config.notifyEvents : defaults.notifyEvents;
  const templateSource =
    config.libraryTemplates && typeof config.libraryTemplates === "object" ? config.libraryTemplates : defaults.libraryTemplates;
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
    wechatCallbackAes: String(config.wechatCallbackAes || defaults.wechatCallbackAes).trim(),
    libraryTemplates: {
      single: String(templateSource.single || defaults.libraryTemplates.single).replace(/\r\n/g, "\n").replace(/\r/g, "\n"),
      grouped: String(templateSource.grouped || defaults.libraryTemplates.grouped).replace(/\r\n/g, "\n").replace(/\r/g, "\n")
    }
  };
}

function normalizePlaybackUserScope(rawScope, defaultScope = DEFAULT_NOTIFICATION_CONFIG.runtime.playback.userScope) {
  const source = rawScope && typeof rawScope === "object" ? rawScope : {};
  const fallback = defaultScope && typeof defaultScope === "object" ? defaultScope : { mode: "all", selectedUserNames: [], selectedUsersMeta: [] };
  const mode = String(source.mode ?? fallback.mode ?? "all").trim().toLowerCase();
  const nextMode = mode === "selected" ? "selected" : "all";
  const selectedUserNames = [];
  const seenNames = new Set();
  const rawNames = Array.isArray(source.selectedUserNames) ? source.selectedUserNames : Array.isArray(fallback.selectedUserNames) ? fallback.selectedUserNames : [];
  rawNames.forEach((value) => {
    const safeValue = String(value || "").trim();
    const safeKey = safeValue.toLowerCase();
    if (!safeValue || seenNames.has(safeKey)) {
      return;
    }
    seenNames.add(safeKey);
    selectedUserNames.push(safeValue);
  });
  const selectedUsersMeta = [];
  const seenMeta = new Set();
  const rawMeta = Array.isArray(source.selectedUsersMeta) ? source.selectedUsersMeta : Array.isArray(fallback.selectedUsersMeta) ? fallback.selectedUsersMeta : [];
  rawMeta.forEach((row) => {
    if (!row || typeof row !== "object") {
      return;
    }
    const id = String(row.id || row.userId || "").trim();
    const name = String(row.name || row.userName || "").trim();
    if (!id && !name) {
      return;
    }
    const metaKey = `${name.toLowerCase()}::${id.toLowerCase()}`;
    if (seenMeta.has(metaKey)) {
      return;
    }
    seenMeta.add(metaKey);
    if (name && !seenNames.has(name.toLowerCase())) {
      seenNames.add(name.toLowerCase());
      selectedUserNames.push(name);
    }
    selectedUsersMeta.push({ id, name });
  });
  return {
    mode: nextMode,
    selectedUserNames,
    selectedUsersMeta
  };
}

function normalizeNotificationConfig(rawConfig, legacyBotConfig = DEFAULT_BOT_CONFIG) {
  const legacy = normalizeBotConfig(rawConfig?.botConfig || legacyBotConfig || DEFAULT_BOT_CONFIG);
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const channelSource = config.channels && typeof config.channels === "object" ? config.channels : {};
  const routeSource = config.routes && typeof config.routes === "object" ? config.routes : {};
  const templateSource = config.templates && typeof config.templates === "object" ? config.templates : {};
  const displaySource = config.display && typeof config.display === "object" ? config.display : {};
  const runtimeSource = config.runtime && typeof config.runtime === "object" ? config.runtime : {};
  const playbackRuntimeSource =
    runtimeSource.playback && typeof runtimeSource.playback === "object" ? runtimeSource.playback : {};
  const legacyDerived = {
    enabled: Boolean(legacy.enableCore),
    channels: {
      telegram: {
        enabled: Boolean(legacy.enableCore),
        botToken: String(legacy.telegramToken || "").trim(),
        chatId: String(legacy.telegramChatId || "").trim(),
        enableCommands: Boolean(legacy.enableCommands),
        proxyUrl: ""
      },
      wecom: {
        enabled: false,
        corpId: String(legacy.wechatCorpId || "").trim(),
        agentId: String(legacy.wechatAgentId || "").trim(),
        secret: String(legacy.wechatSecret || "").trim(),
        toUser: String(legacy.wechatToUser || "@all").trim() || "@all",
        callbackToken: String(legacy.wechatCallbackToken || "").trim(),
        callbackAes: String(legacy.wechatCallbackAes || "").trim(),
        callbackUrl: "",
        proxyUrl: ""
      }
    },
    routes: {
      telegram: {
        "playback.start": Boolean(legacy.enablePlayback && legacy.notifyEvents?.start),
        "playback.pause": Boolean(legacy.enablePlayback && legacy.notifyEvents?.pause),
        "playback.resume": Boolean(legacy.enablePlayback && legacy.notifyEvents?.resume),
        "playback.stop": Boolean(legacy.enablePlayback && legacy.notifyEvents?.stop),
        "library.single": Boolean(legacy.enableLibrary),
        "library.grouped": Boolean(legacy.enableLibrary)
      },
      wecom: { ...DEFAULT_NOTIFICATION_CONFIG.routes.wecom }
    },
    templates: {
      telegram: {
        ...DEFAULT_NOTIFICATION_CONFIG.templates.telegram,
        "library.single": String(legacy.libraryTemplates?.single || DEFAULT_NOTIFICATION_CONFIG.templates.telegram["library.single"]),
        "library.grouped": String(legacy.libraryTemplates?.grouped || DEFAULT_NOTIFICATION_CONFIG.templates.telegram["library.grouped"])
      },
      wecom: {
        ...DEFAULT_NOTIFICATION_CONFIG.templates.wecom,
        "library.single": String(legacy.libraryTemplates?.single || DEFAULT_NOTIFICATION_CONFIG.templates.wecom["library.single"]),
        "library.grouped": String(legacy.libraryTemplates?.grouped || DEFAULT_NOTIFICATION_CONFIG.templates.wecom["library.grouped"])
      }
    },
    runtime: {
      dedupeSeconds: Number(legacy.eventDedupSeconds || 10) || 10,
      playback: {
        showIp: Boolean(legacy.showIp),
        showIpGeo: Boolean(legacy.showIpGeo),
        showOverview: Boolean(legacy.showOverview),
        userScope: normalizePlaybackUserScope(null)
      }
    }
  };
  let dedupeSeconds = Number.parseInt(
    String(runtimeSource.dedupeSeconds ?? legacyDerived.runtime.dedupeSeconds ?? DEFAULT_NOTIFICATION_CONFIG.runtime.dedupeSeconds),
    10
  );
  if (!Number.isFinite(dedupeSeconds)) {
    dedupeSeconds = DEFAULT_NOTIFICATION_CONFIG.runtime.dedupeSeconds;
  }
  dedupeSeconds = Math.max(1, Math.min(120, dedupeSeconds));
  const normalizeTemplates = (channelKey) => {
    const defaults = DEFAULT_NOTIFICATION_CONFIG.templates[channelKey];
    const source = templateSource[channelKey] && typeof templateSource[channelKey] === "object" ? templateSource[channelKey] : {};
    const legacyTemplates = legacyDerived.templates[channelKey];
    const next = {};
    Object.keys(defaults).forEach((eventKey) => {
      const value = source[eventKey] ?? legacyTemplates[eventKey] ?? defaults[eventKey];
      next[eventKey] = String(value || defaults[eventKey]).replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    });
    return next;
  };
  const normalizeRoutes = (channelKey) => {
    const defaults = DEFAULT_NOTIFICATION_CONFIG.routes[channelKey];
    const source = routeSource[channelKey] && typeof routeSource[channelKey] === "object" ? routeSource[channelKey] : {};
    const legacyRoutes = legacyDerived.routes[channelKey];
    const next = {};
    Object.keys(defaults).forEach((eventKey) => {
      next[eventKey] = Boolean(source[eventKey] ?? legacyRoutes[eventKey] ?? defaults[eventKey]);
    });
    return next;
  };
  const normalizeDisplay = (channelKey) => {
    const defaults = DEFAULT_NOTIFICATION_CONFIG.display[channelKey];
    const source = displaySource[channelKey] && typeof displaySource[channelKey] === "object" ? displaySource[channelKey] : {};
    const next = {};
    Object.keys(defaults).forEach((eventKey) => {
      const value = source[eventKey] && typeof source[eventKey] === "object" ? source[eventKey] : {};
      const fallback = defaults[eventKey] || { label: eventKey, description: "" };
      next[eventKey] = {
        label: String(value.label ?? fallback.label ?? eventKey).trim() || String(fallback.label || eventKey),
        description: String(value.description ?? fallback.description ?? "").trim()
      };
    });
    return next;
  };
  return {
    enabled: Boolean(config.enabled ?? legacyDerived.enabled ?? DEFAULT_NOTIFICATION_CONFIG.enabled),
    channels: {
      telegram: {
        enabled: Boolean(channelSource.telegram?.enabled ?? legacyDerived.channels.telegram.enabled),
        botToken: String(channelSource.telegram?.botToken ?? legacyDerived.channels.telegram.botToken ?? "").trim(),
        chatId: String(channelSource.telegram?.chatId ?? legacyDerived.channels.telegram.chatId ?? "").trim(),
        enableCommands: Boolean(channelSource.telegram?.enableCommands ?? legacyDerived.channels.telegram.enableCommands),
        proxyUrl: String(channelSource.telegram?.proxyUrl ?? legacyDerived.channels.telegram.proxyUrl ?? "").trim()
      },
      wecom: {
        enabled: Boolean(channelSource.wecom?.enabled ?? legacyDerived.channels.wecom.enabled),
        corpId: String(channelSource.wecom?.corpId ?? legacyDerived.channels.wecom.corpId ?? "").trim(),
        agentId: String(channelSource.wecom?.agentId ?? legacyDerived.channels.wecom.agentId ?? "").trim(),
        secret: String(channelSource.wecom?.secret ?? legacyDerived.channels.wecom.secret ?? "").trim(),
        toUser: String(channelSource.wecom?.toUser ?? legacyDerived.channels.wecom.toUser ?? "@all").trim() || "@all",
        callbackToken: String(channelSource.wecom?.callbackToken ?? legacyDerived.channels.wecom.callbackToken ?? "").trim(),
        callbackAes: String(channelSource.wecom?.callbackAes ?? legacyDerived.channels.wecom.callbackAes ?? "").trim(),
        callbackUrl: String(channelSource.wecom?.callbackUrl ?? legacyDerived.channels.wecom.callbackUrl ?? "").trim(),
        proxyUrl: String(channelSource.wecom?.proxyUrl ?? legacyDerived.channels.wecom.proxyUrl ?? "").trim()
      }
    },
    routes: {
      telegram: normalizeRoutes("telegram"),
      wecom: normalizeRoutes("wecom")
    },
    templates: {
      telegram: normalizeTemplates("telegram"),
      wecom: normalizeTemplates("wecom")
    },
    display: {
      telegram: normalizeDisplay("telegram"),
      wecom: normalizeDisplay("wecom")
    },
    runtime: {
      dedupeSeconds,
      playback: {
        showIp: Boolean(playbackRuntimeSource.showIp ?? legacyDerived.runtime.playback.showIp),
        showIpGeo: Boolean(playbackRuntimeSource.showIpGeo ?? legacyDerived.runtime.playback.showIpGeo),
        showOverview: Boolean(playbackRuntimeSource.showOverview ?? legacyDerived.runtime.playback.showOverview),
        userScope: normalizePlaybackUserScope(playbackRuntimeSource.userScope, legacyDerived.runtime.playback.userScope)
      }
    }
  };
}

function getNotificationEventPresentation(channel, eventDef, config = appState.notificationConfig) {
  const safeConfig = normalizeNotificationConfig(config, appState.botConfig);
  const fallbackLabel = String(eventDef?.label || eventDef?.key || "").trim();
  const fallbackDescription = String(eventDef?.description || "").trim();
  const display = safeConfig.display?.[channel]?.[eventDef?.key] || {};
  return {
    label: String(display.label || fallbackLabel || eventDef?.key || "").trim(),
    description: String(display.description || fallbackDescription || "").trim()
  };
}

function normalizeNotificationCapabilities(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const events = Array.isArray(source.events) ? source.events.filter((row) => row && typeof row === "object") : [];
  const upcomingEvents =
    Array.isArray(source.upcomingEvents) ? source.upcomingEvents.filter((row) => row && typeof row === "object") : [];
  const channels = source.channels && typeof source.channels === "object" ? source.channels : DEFAULT_NOTIFICATION_CAPABILITIES.channels;
  return {
    channels,
    events,
    upcomingEvents
  };
}

const notificationPreviewTimers = new Map();

function normalizeAiConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  let temperature = Number.parseFloat(String(config.temperature ?? DEFAULT_AI_CONFIG.temperature));
  if (!Number.isFinite(temperature)) {
    temperature = DEFAULT_AI_CONFIG.temperature;
  }
  temperature = Math.max(0, Math.min(2, temperature));
  let maxTokens = Number.parseInt(String(config.maxTokens ?? DEFAULT_AI_CONFIG.maxTokens), 10);
  if (!Number.isFinite(maxTokens)) {
    maxTokens = DEFAULT_AI_CONFIG.maxTokens;
  }
  maxTokens = Math.max(128, Math.min(4000, maxTokens));
  let contextTokensK = Number.parseInt(String(config.contextTokensK ?? DEFAULT_AI_CONFIG.contextTokensK), 10);
  if (!Number.isFinite(contextTokensK)) {
    contextTokensK = DEFAULT_AI_CONFIG.contextTokensK;
  }
  contextTokensK = Math.max(4, Math.min(1024, contextTokensK));
  return {
    enabled: Boolean(config.enabled ?? DEFAULT_AI_CONFIG.enabled),
    baseUrl: String(config.baseUrl || DEFAULT_AI_CONFIG.baseUrl).trim().replace(/\/+$/, ""),
    apiKey: String(config.apiKey || "").trim(),
    model: String(config.model || DEFAULT_AI_CONFIG.model).trim() || DEFAULT_AI_CONFIG.model,
    temperature,
    maxTokens,
    contextTokensK
  };
}

function normalizeDrive115Config(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  return {
    enabled: Boolean(config.enabled ?? DEFAULT_DRIVE115_CONFIG.enabled),
    cookie: String(config.cookie || "").trim(),
    defaultCid: String(config.defaultCid || DEFAULT_DRIVE115_CONFIG.defaultCid || "0").trim() || "0",
    hasCookie: Boolean(config.hasCookie || String(config.cookie || "").trim()),
    cookieMasked: String(config.cookieMasked || "").trim()
  };
}

function normalizeCoverStudioConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const clampInt = (value, fallback, minimum, maximum) => {
    let parsed = Number.parseInt(String(value ?? fallback), 10);
    if (!Number.isFinite(parsed)) {
      parsed = fallback;
    }
    return Math.max(minimum, Math.min(maximum, parsed));
  };
  const validModes = new Set(DEFAULT_COVER_STUDIO_MODES.map((mode) => mode.key));
  const validAligns = new Set(DEFAULT_COVER_STUDIO_TITLE_ALIGN_OPTIONS.map((item) => item.key));
  const validTones = new Set(DEFAULT_COVER_STUDIO_ACCENT_TONES.map((item) => item.key));
  const resolveModeDefaults = (templateKey) =>
    DEFAULT_COVER_STUDIO_MODES.find((mode) => mode.key === templateKey)?.defaults || DEFAULT_COVER_STUDIO_MODES[0].defaults;
  const resolveMode = (templateKey) =>
    DEFAULT_COVER_STUDIO_MODES.find((mode) => mode.key === templateKey) || DEFAULT_COVER_STUDIO_MODES[0];
  const resolvePosterLimit = (templateKey) => clampInt(resolveMode(templateKey).maxPosterCount, 8, 2, 8);
  const normalizePosterRotation = (value, templateKey, fallback) =>
    (resolveMode(templateKey).supports || []).includes("posterRotation")
      ? clampInt(value, fallback, 0, 100)
      : 0;
  const normalizeTemplateKey = (value) => {
    const key = String(value || "").trim();
    return validModes.has(key) ? key : DEFAULT_COVER_STUDIO_MODES[0].key;
  };
  const draftSource = config.draft && typeof config.draft === "object" ? config.draft : {};
  const draftTemplateKey = normalizeTemplateKey(draftSource.templateKey);
  const draftDefaults = resolveModeDefaults(draftTemplateKey);
  const draft = {
    viewId: "",
    viewIds: [],
    templateKey: draftTemplateKey,
    pickMode: String(draftSource.pickMode || "random").trim().toLowerCase() === "recent" ? "recent" : "random",
    titleText: String(draftSource.titleText || "").trim(),
    subtitleText: String(draftSource.subtitleText || "").trim(),
    fontKey: normalizeCoverStudioFontKey(draftSource.fontKey),
    titleFontSize: clampInt(draftSource.titleFontSize, 108, 56, 180),
    subtitleFontSize: clampInt(draftSource.subtitleFontSize, 44, 22, 72),
    presetName: String(draftSource.presetName || "默认封面").trim() || "默认封面",
    titleAlign: validAligns.has(String(draftSource.titleAlign || "").trim()) ? String(draftSource.titleAlign || "").trim() : draftDefaults.titleAlign,
    overlayStrength: 0,
    posterCount: clampInt(draftSource.posterCount, draftDefaults.posterCount, 2, resolvePosterLimit(draftTemplateKey)),
    accentTone: validTones.has(String(draftSource.accentTone || "").trim()) ? String(draftSource.accentTone || "").trim() : draftDefaults.accentTone,
    posterRotation: normalizePosterRotation(draftSource.posterRotation, draftTemplateKey, draftDefaults.posterRotation),
    titleYOffset: clampInt(draftSource.titleYOffset, draftDefaults.titleYOffset, -160, 160),
    lockedItemIds: Array.isArray(draftSource.lockedItemIds)
      ? draftSource.lockedItemIds.map((item) => String(item || "").trim()).filter(Boolean)
      : []
  };
  const viewIdSource = Array.isArray(draftSource.viewIds) ? draftSource.viewIds : [draftSource.viewId];
  draft.viewIds = [...new Set(viewIdSource.map((item) => String(item || "").trim()).filter(Boolean))].slice(0, 30);
  draft.viewId = draft.viewIds[0] || "";
  const presets = Array.isArray(config.presets) && config.presets.length
    ? config.presets
        .map((item, index) => {
          if (!item || typeof item !== "object") {
            return null;
          }
          const id = String(item.id || `preset-${index + 1}`).trim();
          if (!id) {
            return null;
          }
          const templateKey = normalizeTemplateKey(item.templateKey);
          const defaults = resolveModeDefaults(templateKey);
          return {
            id,
            name: String(item.name || item.presetName || id).trim() || id,
            templateKey,
            pickMode: String(item.pickMode || "random").trim().toLowerCase() === "recent" ? "recent" : "random",
            titleText: String(item.titleText || "").trim(),
            subtitleText: String(item.subtitleText || "").trim(),
            fontKey: normalizeCoverStudioFontKey(item.fontKey),
            titleFontSize: clampInt(item.titleFontSize, 108, 56, 180),
            subtitleFontSize: clampInt(item.subtitleFontSize, 44, 22, 72),
            titleAlign: validAligns.has(String(item.titleAlign || "").trim()) ? String(item.titleAlign || "").trim() : defaults.titleAlign,
            overlayStrength: 0,
            posterCount: clampInt(item.posterCount, defaults.posterCount, 2, resolvePosterLimit(templateKey)),
            accentTone: validTones.has(String(item.accentTone || "").trim()) ? String(item.accentTone || "").trim() : defaults.accentTone,
            posterRotation: normalizePosterRotation(item.posterRotation, templateKey, defaults.posterRotation),
            titleYOffset: clampInt(item.titleYOffset, defaults.titleYOffset, -160, 160),
            lockedItemIds: Array.isArray(item.lockedItemIds)
              ? item.lockedItemIds.map((row) => String(row || "").trim()).filter(Boolean)
              : []
          };
        })
        .filter(Boolean)
    : DEFAULT_COVER_STUDIO_CONFIG.presets;
  const backupsSource = config.backups && typeof config.backups === "object" ? config.backups : {};
  const backups = Object.fromEntries(
    Object.entries(backupsSource).map(([key, value]) => [
      String(key || "").trim(),
      value && typeof value === "object"
        ? {
            primary: value.primary && typeof value.primary === "object" ? value.primary : {},
            thumb: value.thumb && typeof value.thumb === "object" ? value.thumb : {},
            appliedAt: String(value.appliedAt || "").trim()
          }
        : { primary: {}, thumb: {}, appliedAt: "" }
    ])
  );
  const scheduleSource = config.schedule && typeof config.schedule === "object" ? config.schedule : {};
  const schedule = {
    enabled: Boolean(scheduleSource.enabled),
    cron: String(scheduleSource.cron || "0 */6 * * *").trim() || "0 */6 * * *",
    lastRunAt: String(scheduleSource.lastRunAt || "").trim(),
    lastStatus: String(scheduleSource.lastStatus || "idle").trim().toLowerCase(),
    lastMessage: String(scheduleSource.lastMessage || "未启用自动更新。").trim(),
    lastResultCount: Math.max(0, Number.parseInt(String(scheduleSource.lastResultCount || 0), 10) || 0)
  };
  const schedules = Array.isArray(config.schedules)
    ? config.schedules
        .map((item) => {
          if (!item || typeof item !== "object") {
            return null;
          }
          const viewId = String(item.viewId || "").trim();
          if (!viewId) {
            return null;
          }
          const templateSource = item.template && typeof item.template === "object" ? item.template : draft;
          return {
            id: String(item.id || `view-${viewId}`).trim() || `view-${viewId}`,
            viewId,
            viewName: String(item.viewName || "").trim(),
            enabled: Boolean(item.enabled),
            cron: String(item.cron || "0 */6 * * *").trim() || "0 */6 * * *",
            template: buildCoverStudioScheduleTemplate(templateSource),
            fingerprint: item.fingerprint && typeof item.fingerprint === "object" ? item.fingerprint : {},
            initializedAt: String(item.initializedAt || "").trim(),
            lastCheckedAt: String(item.lastCheckedAt || "").trim(),
            lastUpdatedAt: String(item.lastUpdatedAt || "").trim(),
            lastStatus: String(item.lastStatus || "idle").trim(),
            lastMessage: String(item.lastMessage || "尚未检查。").trim()
          };
        })
        .filter(Boolean)
    : [];
  const scheduleDraftSource = config.scheduleDraft && typeof config.scheduleDraft === "object"
    ? config.scheduleDraft
    : draft;
  const scheduleDraft = buildCoverStudioScheduleTemplate(scheduleDraftSource);
  return {
    currentPresetId: String(config.currentPresetId || presets[0]?.id || "default").trim() || presets[0]?.id || "default",
    lastViewId: String(config.lastViewId || draft.viewId || "").trim(),
    draft,
    scheduleDraft,
    presets,
    backups,
    schedule,
    schedules
  };
}

function normalizeLibraryDirectoryConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const rawRoots = Array.isArray(config.roots) ? config.roots : Array.isArray(config.directories) ? config.directories : [];
  const roots = rawRoots
    .map((root) => {
      const source = typeof root === "string" ? { path: root, enabled: true } : root;
      if (!source || typeof source !== "object") {
        return null;
      }
      const path = String(source.path || "").trim();
      if (!path) {
        return null;
      }
      let maxDepth = Number.parseInt(String(source.maxDepth ?? 4), 10);
      if (!Number.isFinite(maxDepth)) {
        maxDepth = 4;
      }
      maxDepth = Math.max(1, Math.min(8, maxDepth));
      const rawCategories = Array.isArray(source.categories) ? source.categories : [];
      const categories = rawCategories
        .map((category) => {
          const row = typeof category === "string" ? { label: category } : category;
          if (!row || typeof row !== "object") {
            return null;
          }
          const label = String(row.label || row.name || "").trim();
          const aliases = Array.isArray(row.aliases)
            ? row.aliases.map((item) => String(item || "").trim()).filter(Boolean)
            : [];
          const pathValue = String(row.path || row.relativePath || "").trim();
          if (!label && !pathValue) {
            return null;
          }
          return {
            label,
            aliases,
            path: pathValue
          };
        })
        .filter(Boolean);
      return {
        name: String(source.name || "本地媒体库").trim() || "本地媒体库",
        path,
        enabled: Boolean(source.enabled ?? true),
        maxDepth,
        categories
      };
    })
    .filter(Boolean);
  return { roots };
}

function normalizeHDHiveConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const user = config.user && typeof config.user === "object" ? config.user : {};
  return {
    enabled: Boolean(config.enabled),
    authMode: String(config.authMode || "broker") === "direct" ? "direct" : "broker",
    brokerUrl: String(config.brokerUrl || "").trim().replace(/\/+$/, ""),
    installationId: String(config.installationId || "").trim(),
    registered: Boolean(config.registered),
    oauthSessionId: String(config.oauthSessionId || "").trim(),
    oauthSessionExpiresAt: Number(config.oauthSessionExpiresAt || 0),
    autoCheckin: Boolean(config.autoCheckin ?? true),
    timezone: String(config.timezone || "Asia/Shanghai").trim() || "Asia/Shanghai",
    lastCheckin: config.lastCheckin && typeof config.lastCheckin === "object" ? config.lastCheckin : {},
    lastCheckinDate: String(config.lastCheckinDate || "").trim(),
    clientId: String(config.clientId || "").trim(),
    redirectUri: String(config.redirectUri || config.callbackUri || "").trim(),
    callbackUri: String(config.callbackUri || config.redirectUri || "").trim(),
    hasAppSecret: Boolean(config.hasAppSecret),
    appSecretMasked: String(config.appSecretMasked || "").trim(),
    authorized: Boolean(config.authorized),
    accessExpiresAt: Number(config.accessExpiresAt || 0),
    scopes: String(config.scopes || "meta query unlock write").trim(),
    user: {
      id: String(user.id || ""),
      username: String(user.username || "").trim(),
      level: String(user.level || "").trim(),
      points: user.points ?? null,
      avatar: String(user.avatar || "").trim()
    }
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
    botConfig: normalizeList(source.botConfig),
    notificationConfig: normalizeList(source.notificationConfig),
    aiConfig: normalizeList(source.aiConfig),
    drive115Config: normalizeList(source.drive115Config),
    hdhiveConfig: normalizeList(source.hdhiveConfig),
    adminAuth: normalizeList(source.adminAuth)
  };
}

function mergeEnvControlledFields(raw, groupHint = "") {
  const current = normalizeEnvControlledFields(appState?.envControlledFields);
  const normalizeList = (value) =>
    Array.isArray(value)
      ? value.map((item) => String(item || "").trim()).filter(Boolean)
      : [];

  if (Array.isArray(raw)) {
    if (["embyConfig", "botConfig", "notificationConfig", "aiConfig", "drive115Config", "hdhiveConfig", "adminAuth"].includes(groupHint)) {
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
    if (Object.prototype.hasOwnProperty.call(raw, "notificationConfig")) {
      current.notificationConfig = normalizeList(raw.notificationConfig);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "aiConfig")) {
      current.aiConfig = normalizeList(raw.aiConfig);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "drive115Config")) {
      current.drive115Config = normalizeList(raw.drive115Config);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "hdhiveConfig")) {
      current.hdhiveConfig = normalizeList(raw.hdhiveConfig);
    }
    if (Object.prototype.hasOwnProperty.call(raw, "adminAuth")) {
      current.adminAuth = normalizeList(raw.adminAuth);
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
  const notificationManaged = appState?.envControlledFields?.notificationConfig || [];
  const aiManaged = appState?.envControlledFields?.aiConfig || [];
  const drive115Managed = appState?.envControlledFields?.drive115Config || [];
  const hdhiveManaged = appState?.envControlledFields?.hdhiveConfig || [];
  const activeServerType = getActiveMediaServerType();

  setFieldEnvControlled(elements.serverUrl, activeServerType === "emby" && embyManaged.includes("serverUrl"));
  setFieldEnvControlled(elements.apiKey, activeServerType === "emby" && embyManaged.includes("apiKey"));
  if (elements.apiKeyToggle) {
    elements.apiKeyToggle.disabled = activeServerType === "emby" && embyManaged.includes("apiKey");
  }
  if (elements.settingsEnvManagedHint) {
    const hasManaged = activeServerType === "emby" && embyManaged.length > 0;
    elements.settingsEnvManagedHint.hidden = !hasManaged;
    if (hasManaged) {
      elements.settingsEnvManagedHint.textContent = "该配置由环境变量控制，请在 .env 或 docker-compose.yml 中修改。";
    }
  }

  const tokenManaged = botManaged.includes("telegramToken") || notificationManaged.includes("channels.telegram.botToken");
  const chatManaged = botManaged.includes("telegramChatId") || notificationManaged.includes("channels.telegram.chatId");
  setFieldEnvControlled(elements.botTelegramToken, tokenManaged);
  setFieldEnvControlled(elements.botTelegramChatId, chatManaged);
  if (elements.botTelegramTokenToggle) {
    elements.botTelegramTokenToggle.disabled = tokenManaged;
  }
  setFieldEnvControlled(elements.aiBaseUrl, aiManaged.includes("baseUrl"));
  setFieldEnvControlled(elements.aiApiKey, aiManaged.includes("apiKey"));
  setFieldEnvControlled(elements.aiModel, aiManaged.includes("model"));
  if (elements.aiApiKeyToggle) {
    elements.aiApiKeyToggle.disabled = aiManaged.includes("apiKey");
  }
  setFieldEnvControlled(elements.drive115Cookie, drive115Managed.includes("cookie"));
  setFieldEnvControlled(elements.drive115DefaultCid, drive115Managed.includes("defaultCid"));
  if (document.getElementById("drive115-cookie-toggle")) {
    document.getElementById("drive115-cookie-toggle").disabled = drive115Managed.includes("cookie");
  }
  setFieldEnvControlled(elements.hdhiveClientId, hdhiveManaged.includes("clientId"));
  setFieldEnvControlled(elements.hdhiveAppSecret, hdhiveManaged.includes("appSecret"));
  setFieldEnvControlled(elements.hdhiveRedirectUri, hdhiveManaged.includes("redirectUri"));
  if (elements.botEnvManagedHint) {
    const hasManaged = botManaged.length > 0 || notificationManaged.length > 0;
    elements.botEnvManagedHint.hidden = !hasManaged;
    if (hasManaged) {
      elements.botEnvManagedHint.textContent = "该配置由环境变量控制，请在 .env 或 docker-compose.yml 中修改。";
    }
  }
}

appState.config = normalizeAppConfig(appState.config);
appState.botConfig = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...appState.botConfig });
appState.notificationConfig = normalizeNotificationConfig({ ...DEFAULT_NOTIFICATION_CONFIG, ...appState.notificationConfig }, appState.botConfig);
appState.aiConfig = normalizeAiConfig({ ...DEFAULT_AI_CONFIG, ...appState.aiConfig });
appState.coverStudioConfig = normalizeCoverStudioConfig({ ...DEFAULT_COVER_STUDIO_CONFIG, ...appState.coverStudioConfig });
appState.libraryDirectoryConfig = normalizeLibraryDirectoryConfig({ ...DEFAULT_LIBRARY_DIRECTORY_CONFIG, ...appState.libraryDirectoryConfig });
appState.drive115Config = normalizeDrive115Config({ ...DEFAULT_DRIVE115_CONFIG, ...appState.drive115Config });
appState.hdhiveConfig = normalizeHDHiveConfig({ ...DEFAULT_HDHIVE_CONFIG, ...appState.hdhiveConfig });
appState.qualityResolutionFilters = normalizeQualityResolutionFilters(appState.qualityResolutionFilters);

const elements = {
  appRoot: document.getElementById("app-root"),
  authLoading: document.getElementById("admin-auth-loading"),
  authShell: document.getElementById("admin-auth-shell"),
  authForm: document.getElementById("admin-auth-form"),
  authUsername: document.getElementById("admin-auth-username"),
  authPassword: document.getElementById("admin-auth-password"),
  authRemember: document.getElementById("admin-auth-remember"),
  authError: document.getElementById("admin-auth-error"),
  authSubmit: document.getElementById("admin-auth-submit"),
  adminCredentialForm: document.getElementById("admin-credential-form"),
  adminCredentialCurrentUsername: document.getElementById("admin-credential-current-username"),
  adminCredentialCurrentPassword: document.getElementById("admin-credential-current-password"),
  adminCredentialNextUsername: document.getElementById("admin-credential-next-username"),
  adminCredentialNextPassword: document.getElementById("admin-credential-next-password"),
  adminCredentialConfirmPassword: document.getElementById("admin-credential-confirm-password"),
  adminCredentialEnvHint: document.getElementById("admin-credential-env-hint"),
  adminCredentialFeedback: document.getElementById("admin-credential-feedback"),
  adminCredentialSubmit: document.getElementById("admin-credential-submit"),
  navItems: document.querySelectorAll(".nav-item"),
  primarySidebar: document.getElementById("primary-sidebar"),
  sidebarToggleBtn: document.getElementById("sidebar-toggle-btn"),
  sidebarGlobalSearchTrigger: document.getElementById("global-search-trigger"),
  sidebarGlobalSearchInput: document.getElementById("sidebar-global-search"),
  viewSections: document.querySelectorAll(".view-section"),
  overviewStatsGrid: document.getElementById("overview-stats-grid"),
  mainContent: document.querySelector(".main-content"),
  topbar: document.querySelector(".topbar"),
  topbarActions: document.getElementById("topbar-actions"),
  topbarLogsToolbarHost: document.getElementById("topbar-logs-toolbar-host"),
  topbarUserCenterActions: document.getElementById("topbar-user-center-actions"),
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
  mediaServerButtons: document.querySelectorAll("[data-media-server]"),
  mediaServerConfigTitle: document.getElementById("media-server-config-title"),
  mediaServerConfigDesc: document.getElementById("media-server-config-desc"),
  tmdbEnabled: document.getElementById("tmdb-enabled"),
  tmdbToken: document.getElementById("tmdb-token"),
  tmdbLanguage: document.getElementById("tmdb-language"),
  tmdbRegion: document.getElementById("tmdb-region"),
  tmdbStatusTip: document.getElementById("tmdb-status-tip"),
  tmdbTokenHint: document.getElementById("tmdb-token-hint"),
  tmdbTestBtn: document.getElementById("tmdb-test-btn"),
  aiEnabled: document.getElementById("ai-enabled"),
  aiBaseUrl: document.getElementById("ai-base-url"),
  aiApiKey: document.getElementById("ai-api-key"),
  aiApiKeyToggle: document.getElementById("ai-api-key-toggle"),
  aiModel: document.getElementById("ai-model"),
  aiTemperature: document.getElementById("ai-temperature"),
  aiMaxTokens: document.getElementById("ai-max-tokens"),
  aiContextTokensK: document.getElementById("ai-context-tokens-k"),
  aiTestBtn: document.getElementById("ai-test-btn"),
  aiFeedback: document.getElementById("ai-feedback"),
  libraryDirectoryRootPath: document.getElementById("library-directory-root-path"),
  libraryDirectoryRootName: document.getElementById("library-directory-root-name"),
  libraryDirectoryMaxDepth: document.getElementById("library-directory-max-depth"),
  libraryDirectoryCategories: document.getElementById("library-directory-categories"),
  libraryDirectoryFeedback: document.getElementById("library-directory-feedback"),
  coverStudioStatusBadge: document.getElementById("cover-studio-status-badge"),
  coverStudioSelectionChip: document.getElementById("cover-studio-selection-chip"),
  coverStudioViewSelect: document.getElementById("cover-studio-view-select"),
  coverStudioViewPicker: document.getElementById("cover-studio-view-picker"),
  coverStudioSelectAllBtn: document.getElementById("cover-studio-select-all-btn"),
  coverStudioClearSelectionBtn: document.getElementById("cover-studio-clear-selection-btn"),
  coverStudioTemplateKey: document.getElementById("cover-studio-template-key"),
  coverStudioPickMode: document.getElementById("cover-studio-pick-mode"),
  coverStudioPresetName: document.getElementById("cover-studio-preset-name"),
  coverStudioSaveCurrentBtn: document.getElementById("cover-studio-save-current-btn"),
  coverStudioSaveAsBtn: document.getElementById("cover-studio-save-as-btn"),
  coverStudioModeTabs: document.getElementById("cover-studio-mode-tabs"),
  coverStudioManualPanel: document.getElementById("cover-studio-manual-panel"),
  coverStudioAutoPanel: document.getElementById("cover-studio-auto-panel"),
  coverStudioScheduleViewSelect: document.getElementById("cover-studio-schedule-view-select"),
  coverStudioScheduleCron: document.getElementById("cover-studio-schedule-cron"),
  coverStudioScheduleTemplateKey: document.getElementById("cover-studio-schedule-template-key"),
  coverStudioSchedulePickMode: document.getElementById("cover-studio-schedule-pick-mode"),
  coverStudioScheduleTitleText: document.getElementById("cover-studio-schedule-title-text"),
  coverStudioScheduleSubtitleText: document.getElementById("cover-studio-schedule-subtitle-text"),
  coverStudioScheduleFontKey: document.getElementById("cover-studio-schedule-font-key"),
  coverStudioScheduleTitleAlign: document.getElementById("cover-studio-schedule-title-align"),
  coverStudioScheduleTitleSize: document.getElementById("cover-studio-schedule-title-size"),
  coverStudioScheduleSubtitleSize: document.getElementById("cover-studio-schedule-subtitle-size"),
  coverStudioSchedulePosterCount: document.getElementById("cover-studio-schedule-poster-count"),
  coverStudioScheduleAccentTone: document.getElementById("cover-studio-schedule-accent-tone"),
  coverStudioSchedulePosterRotation: document.getElementById("cover-studio-schedule-poster-rotation"),
  coverStudioScheduleTitleYOffset: document.getElementById("cover-studio-schedule-title-y-offset"),
  coverStudioSchedulePreviewStage: document.getElementById("cover-studio-schedule-preview-stage"),
  coverStudioSchedulePreviewBtn: document.getElementById("cover-studio-schedule-preview-btn"),
  coverStudioScheduleAddBtn: document.getElementById("cover-studio-schedule-add-btn"),
  coverStudioScheduleList: document.getElementById("cover-studio-schedule-list"),
  coverStudioScheduleEditModal: document.getElementById("cover-studio-schedule-edit-modal"),
  coverStudioScheduleEditTitle: document.getElementById("cover-studio-schedule-edit-title"),
  coverStudioScheduleEditForm: document.getElementById("cover-studio-schedule-edit-form"),
  coverStudioScheduleEditClose: document.getElementById("cover-studio-schedule-edit-close"),
  coverStudioScheduleEditCancel: document.getElementById("cover-studio-schedule-edit-cancel"),
  coverStudioScheduleEditSave: document.getElementById("cover-studio-schedule-edit-save"),
  coverStudioTitleText: document.getElementById("cover-studio-title-text"),
  coverStudioSubtitleText: document.getElementById("cover-studio-subtitle-text"),
  coverStudioFontKey: document.getElementById("cover-studio-font-key"),
  coverStudioTitleSize: document.getElementById("cover-studio-title-size"),
  coverStudioSubtitleSize: document.getElementById("cover-studio-subtitle-size"),
  coverStudioTemplateChip: document.getElementById("cover-studio-template-chip"),
  coverStudioTitleAlign: document.getElementById("cover-studio-title-align"),
  coverStudioOverlayStrength: document.getElementById("cover-studio-overlay-strength"),
  coverStudioPosterCount: document.getElementById("cover-studio-poster-count"),
  coverStudioAccentTone: document.getElementById("cover-studio-accent-tone"),
  coverStudioPosterRotation: document.getElementById("cover-studio-poster-rotation"),
  coverStudioTitleYOffset: document.getElementById("cover-studio-title-y-offset"),
  coverStudioTemplateHint: document.getElementById("cover-studio-template-hint"),
  coverStudioFeedback: document.getElementById("cover-studio-feedback"),
  coverStudioPreviewState: document.getElementById("cover-studio-preview-state"),
  coverStudioPreviewStage: document.getElementById("cover-studio-preview-stage"),
  coverStudioSelectedItems: document.getElementById("cover-studio-selected-items"),
  coverStudioPreviewBtn: document.getElementById("cover-studio-preview-btn"),
  coverStudioApplyBtn: document.getElementById("cover-studio-apply-btn"),
  coverStudioRestoreBtn: document.getElementById("cover-studio-restore-btn"),
  drive115Enabled: document.getElementById("drive115-enabled"),
  drive115Cookie: document.getElementById("drive115-cookie"),
  drive115DefaultCid: document.getElementById("drive115-default-cid"),
  drive115SaveBtn: document.getElementById("drive115-save-btn"),
  drive115TestBtn: document.getElementById("drive115-test-btn"),
  drive115ConfigFeedback: document.getElementById("drive115-config-feedback"),
  drive115LoginCollapseBtn: document.getElementById("drive115-login-collapse-btn"),
  drive115LoginBody: document.getElementById("drive115-login-body"),
  drive115QrClient: document.getElementById("drive115-qr-client"),
  drive115QrStartBtn: document.getElementById("drive115-qr-start-btn"),
  drive115QrStopBtn: document.getElementById("drive115-qr-stop-btn"),
  drive115QrPanel: document.getElementById("drive115-qr-panel"),
  drive115QrPlaceholder: document.getElementById("drive115-qr-placeholder"),
  drive115QrImage: document.getElementById("drive115-qr-image"),
  drive115QrStatus: document.getElementById("drive115-qr-status"),
  drive115ShareUrl: document.getElementById("drive115-share-url"),
  drive115ReceiveCode: document.getElementById("drive115-receive-code"),
  drive115TargetCid: document.getElementById("drive115-target-cid"),
  drive115ParseBtn: document.getElementById("drive115-parse-btn"),
  drive115TransferBtn: document.getElementById("drive115-transfer-btn"),
  drive115ParseResult: document.getElementById("drive115-parse-result"),
  drive115Records: document.getElementById("drive115-records"),
  hdhiveEnabled: document.getElementById("hdhive-enabled"),
  hdhiveAuthMode: document.getElementById("hdhive-auth-mode"),
  hdhiveBrokerUrl: document.getElementById("hdhive-broker-url"),
  hdhiveBrokerField: document.getElementById("hdhive-broker-field"),
  hdhiveDirectSettings: document.getElementById("hdhive-direct-settings"),
  hdhiveClientId: document.getElementById("hdhive-client-id"),
  hdhiveAppSecret: document.getElementById("hdhive-app-secret"),
  hdhiveRedirectUri: document.getElementById("hdhive-redirect-uri"),
  hdhiveSaveBtn: document.getElementById("hdhive-save-btn"),
  hdhiveTestBtn: document.getElementById("hdhive-test-btn"),
  hdhiveAuthorizeBtn: document.getElementById("hdhive-authorize-btn"),
  hdhiveRefreshBtn: document.getElementById("hdhive-refresh-btn"),
  hdhiveDisconnectBtn: document.getElementById("hdhive-disconnect-btn"),
  hdhiveAutoCheckin: document.getElementById("hdhive-auto-checkin"),
  hdhiveTimezone: document.getElementById("hdhive-timezone"),
  hdhiveCheckinBtn: document.getElementById("hdhive-checkin-btn"),
  hdhiveCheckinFeedback: document.getElementById("hdhive-checkin-feedback"),
  hdhiveStatusBadge: document.getElementById("hdhive-status-badge"),
  hdhiveConfigFeedback: document.getElementById("hdhive-config-feedback"),
  hdhiveAccountSummary: document.getElementById("hdhive-account-summary"),
  hdhiveSearchKeyword: document.getElementById("hdhive-search-keyword"),
  hdhiveSearchType: document.getElementById("hdhive-search-type"),
  hdhiveOnly115: document.getElementById("hdhive-only-115"),
  hdhiveSearchBtn: document.getElementById("hdhive-search-btn"),
  hdhiveSearchSummary: document.getElementById("hdhive-search-summary"),
  hdhiveSearchResults: document.getElementById("hdhive-search-results"),
  hdhiveRecords: document.getElementById("hdhive-records"),
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
  profileOpenLogs: document.getElementById("profile-open-logs"),
  profileOpenSupport: document.getElementById("profile-open-support"),
  quickLogBtn: document.getElementById("quick-log-btn"),
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
  missingScanBtn: document.getElementById("missing-scan-btn"),
  missingStatus: document.getElementById("missing-status"),
  missingSearch: document.getElementById("missing-search"),
  missingFeedback: document.getElementById("missing-feedback"),
  missingList: document.getElementById("missing-list"),
  missingStatScannedSeries: document.getElementById("missing-stat-scanned-series"),
  missingStatMatchedSeries: document.getElementById("missing-stat-matched-series"),
  missingStatMissingSeries: document.getElementById("missing-stat-missing-series"),
  missingStatMissingEpisodes: document.getElementById("missing-stat-missing-episodes"),
  projectLogLevel: document.getElementById("project-log-level"),
  projectLogModule: document.getElementById("project-log-module"),
  projectLogSearch: document.getElementById("project-log-search"),
  projectLogRefresh: document.getElementById("project-log-refresh"),
  projectLogDownload: document.getElementById("project-log-download"),
  projectLogClear: document.getElementById("project-log-clear"),
  projectLogModal: document.getElementById("project-log-modal"),
  projectLogClose: document.getElementById("project-log-close"),
  projectLogDebug: document.getElementById("project-log-debug"),
  projectLogStatus: document.getElementById("project-log-status"),
  projectLogList: document.getElementById("project-log-list"),
  playbackTodayCount: document.getElementById("playback-today-count"),
  playbackTodayDuration: document.getElementById("playback-today-duration"),
  playbackActiveUsers: document.getElementById("playback-active-users"),
  playbackTotalCount: document.getElementById("playback-total-count"),
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
  notifyChannelCardList: document.getElementById("notify-channel-card-list"),
  notifyChannelEmptyState: document.getElementById("notify-channel-empty-state"),
  notifyChannelEmptyAdd: document.getElementById("notify-channel-empty-add"),
  notifyChannelToolbar: document.getElementById("notify-channel-toolbar"),
  notifyChannelAddMenu: document.getElementById("notify-channel-add-menu"),
  notifyChannelModal: document.getElementById("notify-channel-modal"),
  notifyChannelModalClose: document.getElementById("notify-channel-modal-close"),
  notifyChannelModalTitle: document.getElementById("notify-channel-modal-title"),
  notifyChannelModalSubtitle: document.getElementById("notify-channel-modal-subtitle"),
  notifyChannelModalIcon: document.getElementById("notify-channel-modal-icon"),
  notifyChannelModalContent: document.getElementById("notify-channel-modal-content"),
  notifyChannelPlatformPanel: document.getElementById("notify-channel-platform-panel"),
  notifyChannelPlatformSummary: document.getElementById("notify-channel-platform-summary"),
  notifyChannelModalTest: document.getElementById("notify-channel-modal-test"),
  notifyChannelModalSave: document.getElementById("notify-channel-modal-save"),
  notifyPlaybackUserScopeAll: document.getElementById("notify-playback-user-scope-all"),
  notifyPlaybackUserScopeSelected: document.getElementById("notify-playback-user-scope-selected"),
  notifyPlaybackScopeCard: document.getElementById("notify-playback-scope-card"),
  notifyPlaybackUsersRefresh: document.getElementById("notify-playback-users-refresh"),
  notifyPlaybackUsersStatus: document.getElementById("notify-playback-users-status"),
  notifyPlaybackUsersList: document.getElementById("notify-playback-users-list"),
  notifyPlaybackUserPicker: document.getElementById("notify-playback-user-picker"),
  botTelegramToken: document.getElementById("bot-telegram-token"),
  botTelegramTokenToggle: document.getElementById("bot-telegram-token-toggle"),
  botTelegramChatId: document.getElementById("bot-telegram-chat-id"),
  notifyTelegramEnabled: document.getElementById("notify-telegram-enabled"),
  notifyTelegramProxyUrl: document.getElementById("notify-telegram-proxy-url"),
  notifyTelegramStatus: document.getElementById("notify-telegram-status"),
  botWechatCorpId: document.getElementById("bot-wechat-corp-id"),
  botWechatAgentId: document.getElementById("bot-wechat-agent-id"),
  botWechatSecret: document.getElementById("bot-wechat-secret"),
  botWechatToUser: document.getElementById("bot-wechat-to-user"),
  notifyWecomEnabled: document.getElementById("notify-wecom-enabled"),
  notifyWecomProxyUrl: document.getElementById("notify-wecom-proxy-url"),
  notifyWecomStatus: document.getElementById("notify-wecom-status"),
  notifyChannelAddToggle: document.getElementById("notify-channel-add-toggle"),
  notifyPaneTelegram: document.getElementById("notify-pane-telegram"),
  notifyPaneWecom: document.getElementById("notify-pane-wecom"),
  botWechatCallbackToken: document.getElementById("bot-wechat-callback-token"),
  botWechatCallbackAes: document.getElementById("bot-wechat-callback-aes"),
  botWechatCallbackUrl: document.getElementById("bot-wechat-callback-url"),
  botCopyCallbackUrl: document.getElementById("bot-copy-callback-url"),
  notifyRoutesSave: document.getElementById("notify-routes-save"),
  notifyTemplatesSave: document.getElementById("notify-templates-save"),
  botTemplateReset: document.getElementById("bot-template-reset"),
  notifyRoutesEmpty: document.getElementById("notify-routes-empty"),
  notifyRouteChannelTabs: document.getElementById("notify-route-channel-tabs"),
  notifyTemplatesEmpty: document.getElementById("notify-templates-empty"),
  notifyTemplateChannelTabs: document.getElementById("notify-template-channel-tabs"),
  notifyTemplateEventList: document.getElementById("notify-template-event-list"),
  notifyTemplateWorkspaceEditor: document.getElementById("notify-template-workspace-editor"),
  notifyRouteGridTelegram: document.getElementById("notify-route-grid-telegram"),
  notifyRouteGridWecom: document.getElementById("notify-route-grid-wecom"),
  notifyUpcomingEvents: document.getElementById("notify-upcoming-events"),
  notifyTemplateListTelegram: document.getElementById("notify-template-list-telegram"),
  notifyTemplateListWecom: document.getElementById("notify-template-list-wecom"),
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
  "drive-115": {
    icon: "📦",
    title: "115网盘",
    subtitle: "配置 115 账号、解析分享链接并确认转存"
  },
  hdhive: {
    icon: "🪺",
    title: "影巢搜索",
    subtitle: "通过 HDHive OpenAPI 搜索资源并确认转存到 115"
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
    icon: "🔔",
    title: "通知配置",
    subtitle: "配置通知机器人和自动化助手流程"
  },
  "task-center": {
    icon: "⚡",
    title: "任务中心",
    subtitle: "管理自动化任务队列、执行状态与重试"
  },
  workshop: {
    icon: "🛠️",
    title: "封面工坊",
    subtitle: "管理媒体库视图封面预览、应用与恢复"
  },
  "media-config": {
    icon: "🗃️",
    title: "媒体库配置",
    subtitle: "管理媒体服务器连接、API Key 与 TMDB 兜底策略"
  },
  "ai-config": {
    icon: "🤖",
    title: "AI 配置",
    subtitle: "管理 AI 助手接入参数与本地目录分类映射"
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

function normalizeRequestPath(path = "") {
  const raw = String(path || "").trim();
  if (!raw) {
    return "";
  }
  try {
    return new URL(raw, window.location.origin).pathname;
  } catch {
    return raw.split("?")[0] || raw;
  }
}

function sessionHasPlayableItem(session = null) {
  if (!session || typeof session !== "object") {
    return false;
  }
  const item = session.NowPlayingItem;
  if (item && typeof item === "object") {
    if (item.Name || item.Id || item.SeriesName || item.Album || item.Type) {
      return true;
    }
  }
  const playState = session.PlayState;
  if (playState && typeof playState === "object") {
    const positionTicks = Number(playState.PositionTicks || playState.positionTicks || 0);
    if (Number.isFinite(positionTicks) && positionTicks > 0) {
      return true;
    }
  }
  return false;
}

function isAdminAuthRoute(path = "") {
  return ["/api/auth/me", "/api/auth/login", "/api/auth/logout"].includes(normalizeRequestPath(path));
}

function isAdminReady() {
  return Boolean(
    appState.authBootstrapped &&
    (!appState.authEnabled || appState.authenticated) &&
    document.body.classList.contains("auth-authenticated")
  );
}

function requireAdminReadyForRequest(path = "") {
  if (isAdminAuthRoute(path) || isAdminReady()) {
    return true;
  }
  throw new Error("后台尚未登录，已跳过请求。");
}

function stopAuthenticatedBackgroundWork() {
  if (appState.botWebhookStatusTimer) {
    clearInterval(appState.botWebhookStatusTimer);
    appState.botWebhookStatusTimer = null;
  }
  appState.botWebhookRefreshPromise = null;
  stopLiveSessionsPolling({ clearSessions: true });
  postAuthBootstrapPromise = null;
  window.dispatchEvent(new CustomEvent("vistamirror:auth-lock"));
}

function notifyAuthenticatedReady() {
  window.dispatchEvent(new CustomEvent("vistamirror:auth-ready"));
}

function notifySessionsUpdated() {
  window.dispatchEvent(new CustomEvent("vistamirror:sessions-updated"));
}

function appendApiKeyToPath(path, apiKey) {
  const [pathname, query = ""] = String(path || "").split("?");
  const params = new URLSearchParams(query);
  params.set("api_key", apiKey);
  const nextQuery = params.toString();
  return nextQuery ? `${pathname}?${nextQuery}` : pathname;
}

async function embyFetch(path, options = {}) {
  requireAdminReadyForRequest(`/api/emby${path}`);
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

function appendCacheBuster(path, key = "_ts") {
  const raw = String(path || "").trim();
  if (!raw) {
    return raw;
  }
  const [pathname, query = ""] = raw.split("?");
  const params = new URLSearchParams(query);
  params.set(key, String(Date.now()));
  const nextQuery = params.toString();
  return nextQuery ? `${pathname}?${nextQuery}` : pathname;
}

async function refreshLiveSessions(options = {}) {
  const force = Boolean(options.force);
  if (!isAdminReady() || !appState?.config?.serverUrl || !appState?.config?.apiKey) {
    appState.sessions = [];
    notifySessionsUpdated();
    return [];
  }

  if (appState.liveSessionsRefreshPromise && !force) {
    return appState.liveSessionsRefreshPromise;
  }

  const requestPath = appendCacheBuster("/Sessions");
  const requestPromise = (async () => {
    try {
      const rows = await embyFetch(requestPath, {
        cache: "no-store",
        headers: {
          "Cache-Control": "no-cache, no-store, max-age=0",
          Pragma: "no-cache"
        }
      });
      appState.sessions = Array.isArray(rows) ? rows : [];
      notifySessionsUpdated();
      return appState.sessions;
    } catch {
      notifySessionsUpdated();
      return Array.isArray(appState.sessions) ? appState.sessions : [];
    } finally {
      if (appState.liveSessionsRefreshPromise === requestPromise) {
        appState.liveSessionsRefreshPromise = null;
      }
    }
  })();

  appState.liveSessionsRefreshPromise = requestPromise;
  return requestPromise;
}

function stopLiveSessionsPolling(options = {}) {
  if (appState.liveSessionsTimer) {
    clearInterval(appState.liveSessionsTimer);
    appState.liveSessionsTimer = null;
  }
  appState.liveSessionsRefreshPromise = null;
  if (options.clearSessions) {
    appState.sessions = [];
    notifySessionsUpdated();
  }
}

function ensureLiveSessionsPolling() {
  if (!isAdminReady()) {
    return;
  }
  if (!appState.liveSessionsTimer) {
    refreshLiveSessions({ force: true });
    appState.liveSessionsTimer = setInterval(() => {
      refreshLiveSessions();
    }, 1000);
  }
}

window.addEventListener("vistamirror:auth-ready", () => {
  ensureLiveSessionsPolling();
  refreshLiveSessions({ force: true });
});

window.addEventListener("vistamirror:auth-lock", () => {
  stopLiveSessionsPolling({ clearSessions: true });
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && isAdminReady()) {
    ensureLiveSessionsPolling();
    refreshLiveSessions({ force: true });
  }
});

window.addEventListener("focus", () => {
  if (isAdminReady()) {
    ensureLiveSessionsPolling();
    refreshLiveSessions({ force: true });
  }
});

if (typeof document !== "undefined" && isAdminReady()) {
  ensureLiveSessionsPolling();
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

function setAuthError(message = "") {
  if (!elements.authError) {
    return;
  }
  const text = String(message || "").trim();
  elements.authError.textContent = text;
  elements.authError.hidden = !text;
}

function closeAuthOnlyOverlays() {
  closeProfileMenu?.();
  closeProjectLogModal?.();
  globalSearchModal?.close?.();
  stopAuthenticatedBackgroundWork();
  document.body.classList.remove(
    "global-search-open",
    "mobile-drawer-open",
    "adaptive-drawer-open",
    "has-glass-overlay"
  );
  document.querySelectorAll([
    ".global-search-modal",
    ".mobile-menu-overlay",
    ".mobile-menu-drawer",
    "#radar-live-backdrop",
    "#radar-live-popover"
  ].join(",")).forEach((node) => {
    if (!(node instanceof HTMLElement)) {
      return;
    }
    node.hidden = true;
    node.classList.remove("open");
  });
}

function applyAuthUiState({ mode = "loading", user = "" } = {}) {
  const safeMode = String(mode || "loading");
  document.body.classList.toggle("auth-pending", safeMode === "loading");
  document.body.classList.toggle("auth-required", safeMode === "login");
  document.body.classList.toggle("auth-login", safeMode === "login");
  document.body.classList.toggle("auth-authenticated", safeMode === "ready");

  if (elements.authLoading) {
    elements.authLoading.hidden = safeMode !== "loading";
    elements.authLoading.style.display = safeMode === "loading" ? "" : "none";
  }
  if (elements.authShell) {
    elements.authShell.hidden = safeMode !== "login";
    elements.authShell.style.display = safeMode === "login" ? "" : "none";
  }
  if (elements.appRoot) {
    elements.appRoot.hidden = safeMode !== "ready";
    elements.appRoot.style.display = safeMode === "ready" ? "" : "none";
    elements.appRoot.inert = safeMode !== "ready";
  }
  if (safeMode !== "ready") {
    closeAuthOnlyOverlays();
  } else {
    notifyAuthenticatedReady();
  }
  appState.authUser = String(user || "");
}

function onUnauthorizedDetected() {
  if (!appState.authEnabled) {
    return;
  }
  const hadAuthenticatedUi = appState.authenticated || document.body.classList.contains("auth-authenticated");
  appState.authenticated = false;
  applyAuthUiState({ mode: "login" });
  setAuthError(hadAuthenticatedUi ? "登录已过期，请重新登录。" : "");
}

async function inviteApiFetch(path, options = {}) {
  requireAdminReadyForRequest(path);
  const response = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });

  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await response.json() : await response.text();
  const isAuthRoute = String(path || "").startsWith("/api/auth/");
  if (response.status === 401 && !isAuthRoute) {
    onUnauthorizedDetected();
  }
  if (!response.ok) {
    if (body && typeof body === "object" && body.error) {
      throw new Error(body.error);
    }
    throw new Error(typeof body === "string" ? body : `请求失败 ${response.status}`);
  }
  return body;
}

async function fetchAuthMe() {
  const response = await fetch("/api/auth/me", {
    method: "GET",
    headers: {
      Accept: "application/json"
    }
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : {};
  if (response.ok) {
    return payload && typeof payload === "object" ? payload : {};
  }
  if (response.status === 401) {
    return payload && typeof payload === "object" ? payload : { ok: false, authEnabled: true, error: "unauthorized" };
  }
  throw new Error(
    payload && typeof payload === "object" && payload.error
      ? payload.error
      : `鉴权检查失败 (${response.status})`
  );
}

async function bootstrapAdminAuth() {
  try {
    const payload = await fetchAuthMe();
    const authEnabled = Boolean(payload?.authEnabled);
    appState.authEnabled = authEnabled;
    appState.authenticated = Boolean(payload?.ok);
    const authUser = String(payload?.user?.name || "");
    appState.authUser = authUser;

    if (!authEnabled || appState.authenticated) {
      applyAuthUiState({ mode: "ready", user: authUser });
      appState.authBootstrapped = true;
      return true;
    }
    applyAuthUiState({ mode: "login" });
    setAuthError("");
    appState.authBootstrapped = true;
    return false;
  } catch (error) {
    appState.authEnabled = true;
    appState.authenticated = false;
    applyAuthUiState({ mode: "login" });
    setAuthError(`登录状态检查失败：${String(error?.message || error)}`);
    appState.authBootstrapped = true;
    return false;
  }
}

async function handleAdminLoginSubmit(event) {
  event.preventDefault();
  if (!elements.authUsername || !elements.authPassword || !elements.authSubmit) {
    return;
  }
  const username = String(elements.authUsername.value || "").trim();
  const password = String(elements.authPassword.value || "");
  const rememberMe = Boolean(elements.authRemember?.checked);
  if (!username || !password) {
    setAuthError("请输入管理员账号和密码。");
    return;
  }

  elements.authSubmit.disabled = true;
  setAuthError("");
  try {
    const payload = await inviteApiFetch("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password, rememberMe })
    });
    appState.authEnabled = Boolean(payload?.authEnabled);
    appState.authenticated = Boolean(payload?.ok);
    appState.authUser = String(payload?.user?.name || username);
    elements.authPassword.value = "";
    applyAuthUiState({ mode: "ready", user: appState.authUser });
    await startPostAuthBootstrap();
  } catch (error) {
    setAuthError(String(error?.message || "登录失败，请稍后重试。"));
  } finally {
    elements.authSubmit.disabled = false;
  }
}

async function triggerAdminLogout() {
  try {
    await inviteApiFetch("/api/auth/logout", { method: "POST" });
  } catch (_error) {
    // ignore network/logout errors and still return to login view
  }
  appState.authenticated = false;
  applyAuthUiState({ mode: "login" });
  setAuthError("");
}

function setAdminCredentialFeedback(message = "", type = "default") {
  if (!elements.adminCredentialFeedback) {
    return;
  }
  const text = String(message || "").trim();
  elements.adminCredentialFeedback.textContent = text;
  elements.adminCredentialFeedback.hidden = !text;
  elements.adminCredentialFeedback.dataset.state = text ? type : "";
}

function renderAdminCredentialForm() {
  const meta = appState.adminCredentialMeta || {};
  const managedByEnv = Boolean(meta.managedByEnv);
  const allowUpdate = Boolean(meta.allowUpdate);
  const username = String(meta.username || appState.authUser || "").trim();
  const source = String(meta.source || "missing");

  if (elements.adminCredentialCurrentUsername) {
    elements.adminCredentialCurrentUsername.value = username || "-";
  }
  if (elements.adminCredentialEnvHint) {
    elements.adminCredentialEnvHint.hidden = !managedByEnv;
  }

  [
    elements.adminCredentialCurrentPassword,
    elements.adminCredentialNextUsername,
    elements.adminCredentialNextPassword,
    elements.adminCredentialConfirmPassword,
  ].forEach((input) => {
    if (input) {
      input.disabled = managedByEnv || appState.adminCredentialLoading || !allowUpdate;
    }
  });
  if (elements.adminCredentialSubmit) {
    elements.adminCredentialSubmit.disabled = appState.adminCredentialLoading;
    if (appState.adminCredentialLoading) {
      elements.adminCredentialSubmit.textContent = "更新中...";
    } else if (managedByEnv) {
      elements.adminCredentialSubmit.textContent = "请在 .env.local 中修改";
    } else if (!allowUpdate) {
      elements.adminCredentialSubmit.textContent = "暂不可更新，请刷新重试";
    } else {
      elements.adminCredentialSubmit.textContent = "更新账号与密码";
    }
  }

  if (managedByEnv) {
    setAdminCredentialFeedback("当前管理员凭据由环境变量接管，请在 .env/.env.local 或 docker compose 的 APP_ADMIN_PASSWORD 修改后重启服务。", "warning");
  } else if (!allowUpdate && source === "missing") {
    setAdminCredentialFeedback("管理员凭据状态读取失败，请刷新页面或重启服务后重试。", "error");
  }
}

async function loadAdminCredentialMeta(options = {}) {
  const { silent = false } = options;
  if (!appState.authenticated) {
    return;
  }
  try {
    const payload = await inviteApiFetch("/api/auth/admin-credential-meta", { method: "GET" });
    appState.adminCredentialMeta = {
      username: String(payload?.username || appState.authUser || ""),
      managedByEnv: Boolean(payload?.managedByEnv),
      allowUpdate: Boolean(payload?.allowUpdate),
      source: String(payload?.source || "missing")
    };
    appState.envControlledFields = mergeEnvControlledFields(payload?.envControlledFields, "adminAuth");
    renderAdminCredentialForm();
  } catch (error) {
    if (!silent) {
      setAdminCredentialFeedback(`读取管理员凭据状态失败：${error.message || "未知错误"}`, "error");
    }
  }
}

async function handleAdminCredentialSubmit(event) {
  event.preventDefault();
  if (!appState.authenticated) {
    return;
  }
  if (appState.adminCredentialLoading) {
    return;
  }
  if (appState.adminCredentialMeta?.managedByEnv) {
    setAdminCredentialFeedback("当前管理员凭据由环境变量接管，请在 .env/.env.local 或 docker compose 的 APP_ADMIN_PASSWORD 修改后重启服务。", "warning");
    return;
  }
  if (!appState.adminCredentialMeta?.allowUpdate) {
    setAdminCredentialFeedback("管理员凭据状态未就绪，请刷新页面后重试。", "error");
    return;
  }
  const currentPassword = String(elements.adminCredentialCurrentPassword?.value || "");
  const nextUsername = String(elements.adminCredentialNextUsername?.value || "").trim();
  const nextPassword = String(elements.adminCredentialNextPassword?.value || "");
  const confirmPassword = String(elements.adminCredentialConfirmPassword?.value || "");

  if (!currentPassword || !nextUsername || !nextPassword || !confirmPassword) {
    setAdminCredentialFeedback("请完整填写当前密码、新用户名和新密码。", "error");
    return;
  }
  if (nextUsername.length < 2) {
    setAdminCredentialFeedback("新用户名至少 2 位。", "error");
    return;
  }
  if (nextPassword.length < 6) {
    setAdminCredentialFeedback("新密码至少 6 位。", "error");
    return;
  }
  if (nextPassword !== confirmPassword) {
    setAdminCredentialFeedback("两次输入的新密码不一致。", "error");
    return;
  }

  appState.adminCredentialLoading = true;
  renderAdminCredentialForm();
  setAdminCredentialFeedback("");
  try {
    const payload = await inviteApiFetch("/api/auth/admin-credentials", {
      method: "POST",
      body: JSON.stringify({
        currentPassword,
        nextUsername,
        nextPassword
      })
    });
    if (elements.adminCredentialCurrentPassword) {
      elements.adminCredentialCurrentPassword.value = "";
    }
    if (elements.adminCredentialNextUsername) {
      elements.adminCredentialNextUsername.value = "";
    }
    if (elements.adminCredentialNextPassword) {
      elements.adminCredentialNextPassword.value = "";
    }
    if (elements.adminCredentialConfirmPassword) {
      elements.adminCredentialConfirmPassword.value = "";
    }
    appState.authenticated = false;
    appState.authUser = "";
    applyAuthUiState({ mode: "login" });
    if (elements.authUsername) {
      elements.authUsername.value = String(payload?.username || nextUsername || "");
    }
    if (elements.authPassword) {
      elements.authPassword.value = "";
    }
    if (elements.authRemember) {
      elements.authRemember.checked = false;
    }
    setAuthError("管理员账号密码已更新，请使用新凭据重新登录。");
    showToast("管理员凭据已更新，请重新登录。", 1300);
  } catch (error) {
    setAdminCredentialFeedback(String(error?.message || "更新失败，请稍后重试。"), "error");
  } finally {
    appState.adminCredentialLoading = false;
    renderAdminCredentialForm();
  }
}

async function loadPlaybackHistory(options = {}) {
  const limit = Number(options.limit) > 0 ? Number(options.limit) : 300;
  const scanLimit = Number(options.scanLimit) > 0 ? Number(options.scanLimit) : 2000;
  const quiet = Boolean(options.quiet);
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    appState.playbackHistoryRows = [];
    appState.playbackHistoryDebug = {};
    appState.playbackHistoryWarning = "未配置 Emby，无法加载播放历史。";
    return [];
  }
  appState.playbackHistoryLoading = true;
  try {
    const query = new URLSearchParams({
      limit: String(limit),
      scanLimit: String(scanLimit)
    });
    const payload = await inviteApiFetch(`/api/playback/history?${query.toString()}`);
    appState.playbackHistoryRows = Array.isArray(payload?.rows) ? payload.rows : [];
    appState.playbackHistoryDebug = payload?.debug && typeof payload.debug === "object" ? payload.debug : {};
    appState.playbackHistoryWarning = String(payload?.warning || "").trim();
    if (appState.playbackHistoryWarning) {
      sendProjectLog({
        level: "warning",
        module: "playback",
        action: "playback_history_frontend_warning",
        message: appState.playbackHistoryWarning,
        detail: appState.playbackHistoryDebug
      });
      if (!quiet) {
        addSyncEvent("播放历史提示", appState.playbackHistoryWarning, "warning");
      }
    }
    return appState.playbackHistoryRows;
  } catch (error) {
    appState.playbackHistoryRows = [];
    appState.playbackHistoryDebug = {};
    appState.playbackHistoryWarning = String(error?.message || "加载失败");
    sendProjectLog({
      level: "error",
      module: "playback",
      action: "playback_history_frontend_failed",
      message: "前端读取统一播放历史接口失败。",
      detail: { error: appState.playbackHistoryWarning }
    });
    if (!quiet) {
      addSyncEvent("播放历史加载失败", appState.playbackHistoryWarning, "danger");
    }
    throw error;
  } finally {
    appState.playbackHistoryLoading = false;
  }
}

function mapSyncToneToLogLevel(tone) {
  const safe = String(tone || "").toLowerCase();
  if (safe === "danger" || safe === "error") {
    return "error";
  }
  if (safe === "warning" || safe === "warn") {
    return "warning";
  }
  return "info";
}

function inferLogModuleFromTitle(title) {
  const text = String(title || "");
  if (/邀请|邀请码/.test(text)) {
    return "invite";
  }
  if (/webhook|Telegram|企业微信|机器人/.test(text)) {
    return "webhook";
  }
  if (/播放|日志|同步/.test(text)) {
    return "playback";
  }
  if (/用户|密码|权限|续期/.test(text)) {
    return "auth";
  }
  return "system";
}

function summarizeAuthSignalsFromActivityLogs(rows = []) {
  const summary = { loginSuccess: 0, loginFailed: 0 };
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const text = [row.Name, row.ShortOverview, row.Overview, row.Type]
      .map((item) => String(item || "").toLowerCase())
      .join(" ");
    if (!text) {
      return;
    }
    if (/(login|logged in|sign in|authenticated|登录成功|登陆成功)/i.test(text)) {
      summary.loginSuccess += 1;
    }
    if (/(failed login|login failed|authentication failed|invalid password|登录失败|登陆失败|密码错误)/i.test(text)) {
      summary.loginFailed += 1;
    }
  });
  return summary;
}

function sendProjectLog(payload = {}) {
  if (!isAdminReady()) {
    return;
  }
  const body = {
    level: payload.level || "info",
    module: payload.module || "system",
    action: payload.action || "client_event",
    message: payload.message || "",
    user_id: payload.user_id || payload.userId || "",
    status: payload.status || "",
    detail: payload.detail || {}
  };
  fetch("/api/logs/client", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  }).catch(() => {});
}

function projectLogLevelLabel(level) {
  const safe = String(level || "info").toLowerCase();
  const labels = {
    info: "信息",
    warning: "警告",
    error: "错误"
  };
  return labels[safe] || labels.info;
}

function projectLogModuleLabel(module) {
  const labels = {
    auth: "账号认证",
    invite: "邀请码",
    webhook: "Webhook 回调",
    playback: "播放事件",
    system: "系统",
    docker: "Docker 服务"
  };
  const key = String(module || "system").toLowerCase();
  return labels[key] || labels.system;
}

function projectLogLevelClass(level) {
  const safe = String(level || "info").toLowerCase();
  return safe === "error" || safe === "warning" ? safe : "info";
}

function projectLogActionLabel(action) {
  const labels = {
    api_error: "接口错误",
    bot_config_saved: "机器人配置已保存",
    client_sync_event: "前端同步事件",
    emby_proxy_error: "媒体服务器代理错误",
    invite_created: "邀请码创建",
    invite_used: "邀请码使用",
    login_activity_synced: "登录活动同步",
    logs_cleared: "日志已清空",
    previous_exit_unclean: "上次异常退出",
    register_failed: "注册失败",
    register_success: "注册成功",
    server_config_synced: "服务器配置同步",
    service_started: "服务启动",
    service_stopped: "服务停止",
    verification: "接口验证"
  };
  const key = String(action || "").trim();
  return labels[key] || key.replace(/_/g, " ") || "-";
}

function formatProjectLogDetail(detail) {
  if (detail == null || detail === "") {
    return "-";
  }
  if (typeof detail === "string") {
    return detail;
  }
  try {
    return JSON.stringify(detail);
  } catch {
    return String(detail);
  }
}

function renderProjectLogs() {
  if (!elements.projectLogList) {
    return;
  }
  const rows = Array.isArray(appState.projectLogs) ? appState.projectLogs : [];
  if (elements.projectLogStatus) {
    elements.projectLogStatus.textContent = rows.length
      ? `已显示 ${rows.length} 条日志，共 ${appState.projectLogTotal || rows.length} 条。`
      : "暂无匹配日志。";
  }
  if (!rows.length) {
    elements.projectLogList.innerHTML = `<tr><td colspan="8"><div class="empty-state">暂无日志记录。</div></td></tr>`;
    return;
  }
  elements.projectLogList.innerHTML = rows
    .map((event) => {
      const level = projectLogLevelLabel(event.level);
      const levelClass = projectLogLevelClass(event.level);
      const module = projectLogModuleLabel(event.module);
      const action = projectLogActionLabel(event.action);
      const source = [event.ip, event.path].filter(Boolean).join(" · ") || "-";
      const detail = formatProjectLogDetail(event.detail);
      return `
        <tr>
          <td>${escapeHtml(formatDate(event.time))}</td>
          <td><span class="project-log-level project-log-level-${escapeHtml(levelClass)}">${escapeHtml(level)}</span></td>
          <td><span class="project-log-module">${escapeHtml(module)}</span></td>
          <td>${escapeHtml(action)}</td>
          <td class="project-log-message">${escapeHtml(event.message || "-")}</td>
          <td>${escapeHtml(String(event.status || "-"))}</td>
          <td>${escapeHtml(source)}</td>
          <td><code class="project-log-detail">${escapeHtml(detail)}</code></td>
        </tr>
      `;
    })
    .join("");
}

async function refreshProjectLogs() {
  if (!elements.projectLogList) {
    return;
  }
  const params = new URLSearchParams();
  const level = elements.projectLogLevel ? String(elements.projectLogLevel.value || "") : String(appState.projectLogFilters.level || "");
  const module = elements.projectLogModule ? String(elements.projectLogModule.value || "") : String(appState.projectLogFilters.module || "");
  const keyword = elements.projectLogSearch ? String(elements.projectLogSearch.value || "") : String(appState.projectLogFilters.keyword || "");
  appState.projectLogFilters = { level, module, keyword };
  if (level) {
    params.set("level", level);
  }
  if (module) {
    params.set("module", module);
  }
  if (keyword) {
    params.set("q", keyword);
  }
  params.set("limit", "300");
  if (elements.projectLogStatus) {
    elements.projectLogStatus.textContent = "正在读取日志...";
  }
  try {
    const result = await inviteApiFetch(`/api/logs?${params.toString()}`);
    appState.projectLogs = Array.isArray(result?.events) ? result.events : [];
    appState.projectLogTotal = Number(result?.total || appState.projectLogs.length) || appState.projectLogs.length;
    renderProjectLogs();
  } catch (error) {
    if (elements.projectLogStatus) {
      elements.projectLogStatus.textContent = `读取日志失败：${error.message || "未知错误"}`;
    }
  }
}

function downloadProjectLogs() {
  window.location.href = "/api/logs/download";
}

async function clearProjectLogs() {
  const confirmed = window.confirm("确定要清空全部项目日志吗？这个操作不能撤销。");
  if (!confirmed) {
    return;
  }
  try {
    await inviteApiFetch("/api/logs", { method: "DELETE" });
    showToast("日志已清空", 1000);
    await refreshProjectLogs();
  } catch (error) {
    showToast(`清空失败：${error.message || "未知错误"}`, 1400);
  }
}

function openProjectLogModal() {
  if (!elements.projectLogModal) {
    return;
  }
  closeProfileMenu();
  closeUserCenterInviteModal();
  closeUserCenterInviteManageModal();
  closeUserCenterInviteResultModal();
  closeCreateUserModal();
  closeUserConfigModal();
  elements.projectLogModal.hidden = false;
  refreshProjectLogs();
}

function closeProjectLogModal() {
  if (!elements.projectLogModal) {
    return;
  }
  elements.projectLogModal.hidden = true;
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
    clientName: appState.config.clientName || DEFAULT_EMBY_CLIENT_NAME,
    tmdbEnabled: Boolean(appState.config.tmdbEnabled),
    tmdbToken: String(appState.config.tmdbToken || "").trim(),
    tmdbLanguage: String(appState.config.tmdbLanguage || "zh-CN").trim() || "zh-CN",
    tmdbRegion: String(appState.config.tmdbRegion || "CN").trim().toUpperCase() || "CN"
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
  if (embyManaged.includes("tmdbToken")) {
    delete embyConfig.tmdbToken;
  }

  return {
    embyConfig,
    libraryDirectoryConfig: normalizeLibraryDirectoryConfig(appState.libraryDirectoryConfig),
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
      const backendConfig = result.embyConfig;
      writeMediaServerConfig("emby", {
        ...getMediaServerConfig("emby"),
        ...backendConfig
      });
      appState.config = normalizeAppConfig({
        ...appState.config,
        mediaServers: appState.config.mediaServers,
        tmdbEnabled: Boolean(backendConfig.tmdbEnabled),
        tmdbToken: String(backendConfig.tmdbToken || "").trim(),
        tmdbLanguage: String(backendConfig.tmdbLanguage || "zh-CN").trim() || "zh-CN",
        tmdbRegion: String(backendConfig.tmdbRegion || "CN").trim().toUpperCase() || "CN"
      });
    }
    if (result?.libraryDirectoryConfig && typeof result.libraryDirectoryConfig === "object") {
      appState.libraryDirectoryConfig = normalizeLibraryDirectoryConfig(result.libraryDirectoryConfig);
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
  sendProjectLog({
    level: mapSyncToneToLogLevel(tone),
    module: inferLogModuleFromTitle(title),
    action: "client_sync_event",
    message: title,
    detail: { description }
  });
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

function formatHourMinute(value) {
  if (!value) {
    return "--:--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--:--";
  }
  return date.toLocaleTimeString("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
}

function parseDurationWindowRange(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }
  const match = text.match(/(\d{2}:\d{2})\s*→\s*(\d{2}:\d{2})/);
  if (!match) {
    return null;
  }
  return {
    start: match[1],
    end: match[2]
  };
}

function resolvePlaybackStartEnd(row, dateText) {
  const action = String(row?.action || "").trim().toLowerCase();
  const eventTime = formatHourMinute(dateText);
  let start = formatHourMinute(row?.startTime);
  let end = formatHourMinute(row?.endTime);

  if (start === "--:--" || end === "--:--") {
    const range = parseDurationWindowRange(row?.durationWindow);
    if (range) {
      if (start === "--:--") {
        start = range.start;
      }
      if (end === "--:--") {
        end = range.end;
      }
    }
  }

  if (start === "--:--" && action === "start" && eventTime !== "--:--") {
    start = eventTime;
  }
  if (end === "--:--" && ["stop", "pause", "resume"].includes(action) && eventTime !== "--:--") {
    end = eventTime;
  }

  return { start, end };
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

async function testTmdbConnection(options = {}) {
  const { silent = false } = options;
  const token = String(elements.tmdbToken?.value || appState.config.tmdbToken || "").trim();
  if (!token) {
    if (elements.tmdbTokenHint) {
      elements.tmdbTokenHint.className = "tmdb-token-hint field-span-2 is-warning";
      elements.tmdbTokenHint.textContent = "TMDB Token 尚未保存。";
    }
    if (!silent) {
      showToast("请先填写 TMDB Token", 1200);
    }
    return false;
  }

  if (elements.tmdbTestBtn) {
    elements.tmdbTestBtn.disabled = true;
    elements.tmdbTestBtn.textContent = "测试中...";
  }
  if (elements.tmdbTokenHint) {
    elements.tmdbTokenHint.className = "tmdb-token-hint field-span-2 is-warning";
    elements.tmdbTokenHint.textContent = "正在验证 TMDB Token 与网络连接...";
  }

  try {
    const result = await inviteApiFetch("/api/tmdb/test", {
      method: "POST",
      body: JSON.stringify({
        tmdbConfig: {
          tmdbToken: token,
          tmdbLanguage: String(elements.tmdbLanguage?.value || appState.config.tmdbLanguage || "zh-CN").trim() || "zh-CN",
          tmdbRegion: String(elements.tmdbRegion?.value || appState.config.tmdbRegion || "CN").trim().toUpperCase() || "CN"
        }
      })
    });
    if (elements.tmdbTokenHint) {
      elements.tmdbTokenHint.className = "tmdb-token-hint field-span-2 is-ok";
      elements.tmdbTokenHint.textContent = result?.message || "TMDB 连接正常，Token 已生效。";
    }
    if (elements.tmdbStatusTip) {
      elements.tmdbStatusTip.className = "tmdb-status-badge is-on";
      elements.tmdbStatusTip.textContent = "连接正常";
    }
    if (!silent) {
      showToast("TMDB 连接正常", 1200);
    }
    return true;
  } catch (error) {
    if (elements.tmdbTokenHint) {
      elements.tmdbTokenHint.className = "tmdb-token-hint field-span-2 is-warning";
      elements.tmdbTokenHint.textContent = error.message || "TMDB 连接测试失败。";
    }
    if (elements.tmdbStatusTip) {
      elements.tmdbStatusTip.className = "tmdb-status-badge is-pending";
      elements.tmdbStatusTip.textContent = "连接失败";
    }
    if (!silent) {
      showToast("TMDB 连接测试失败", 1200);
    }
    return false;
  } finally {
    if (elements.tmdbTestBtn) {
      elements.tmdbTestBtn.disabled = false;
      elements.tmdbTestBtn.textContent = "测试连接";
    }
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
  requireAdminReadyForRequest(`/api/emby${path}`);
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
          <td class="user-center-cell-info">
            <div class="user-cell">
              <div class="user-avatar">${initials(row.name)}</div>
              <div class="user-meta">
                <span class="user-center-tag ${roleTagClass}">${roleTagText}</span>
                <strong>${row.name}</strong>
              </div>
            </div>
          </td>
          <td class="user-center-cell-meta" data-label="并发限制">${row.concurrencyText}</td>
          <td class="user-center-cell-meta" data-label="账号状态"><span class="status-badge ${row.status.className}">${row.status.label}</span></td>
          <td class="user-center-cell-meta" data-label="有效期至">${row.expiryText}</td>
          <td class="user-center-cell-meta" data-label="最后登录">${formatDateOnly(row.lastLoginRaw)}</td>
          <td class="user-center-cell-actions">
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
  const totalCount = items.reduce((sum, item) => sum + Number(item.count || 0), 0);
  const activeFilter = String(appState.clientDeviceFilter || "").trim();
  const rows = items
    .map((item, index) => {
      const ratio = Math.max(6, Math.round((item.count / maxCount) * 100));
      const share = totalCount > 0 ? Math.round((item.count / totalCount) * 100) : 0;
      const isSelected = activeFilter === item.label;
      return `
        <button
          class="client-device-rank-row${isSelected ? " is-selected" : ""}"
          type="button"
          data-index="${index}"
          data-label="${escapeHtml(item.label)}"
          data-count="${item.count}"
          aria-pressed="${isSelected ? "true" : "false"}"
          title="点击${isSelected ? "取消" : "按"}${escapeHtml(item.label)}筛选"
        >
          <span class="client-device-rank-number">${String(index + 1).padStart(2, "0")}</span>
          <span class="client-device-rank-main">
            <span class="client-device-rank-label">${escapeHtml(item.label)}</span>
            <span class="client-device-rank-meter" aria-hidden="true"><i style="--device-rank-progress:${ratio}%;"></i></span>
          </span>
          <span class="client-device-rank-count"><strong>${item.count}</strong><em>次播放</em></span>
          <span class="client-device-rank-share">${share}%</span>
        </button>
      `;
    })
    .join("");

  elements.clientTopDevicesChart.innerHTML = `
    <div class="client-device-ranking${items.length === 1 ? " is-single" : ""}">
      <div class="client-device-ranking-summary">
        <span class="client-device-ranking-kicker">${items.length === 1 ? "唯一活跃设备" : `共 ${items.length} 台设备有播放记录`}</span>
        <span class="client-device-ranking-total">${totalCount} <em>次历史播放</em></span>
      </div>
      <div class="client-device-ranking-list">${rows}</div>
    </div>
  `;

  const rankingRows = elements.clientTopDevicesChart.querySelectorAll(".client-device-rank-row");
  if (!rankingRows.length) {
    elements.clientTopDevicesChart.hidden = false;
    elements.clientTopDevicesEmpty.hidden = true;
    return;
  }

  rankingRows.forEach((row) => {
    row.onclick = () => {
      const label = String(row.getAttribute("data-label") || "").trim();
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
                <button class="invite-copy-icon-btn" type="button" data-copy-invite-link="${escapeHtml(link)}" aria-label="复制链接">📋</button>
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
      await copyTextToClipboard(button.dataset.copyCode);
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

  const allRows = Array.isArray(appState.playbackHistoryRows) ? appState.playbackHistoryRows : [];
  const keyword = appState.logSearch.trim().toLowerCase();
  const selectedUser = (elements.logUserFilter?.value || "").trim();

  const users = Array.from(
    new Set(
      allRows
        .map((row) => String(row?.user || row?.userName || "").trim())
        .filter(Boolean)
    )
  ).sort((a, b) => a.localeCompare(b, "zh-CN"));
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
    const title = String(row?.title || row?.mediaName || "").toLowerCase();
    const userName = String(row?.user || row?.userName || "").toLowerCase();
    const device = String(row?.device || row?.player?.device || "").toLowerCase();
    const client = String(row?.client || row?.player?.software || "").toLowerCase();
    const matchKeyword =
      !keyword ||
      title.includes(keyword) ||
      device.includes(keyword) ||
      client.includes(keyword) ||
      userName.includes(keyword);
    const matchUser = !selectedUser || String(row?.user || row?.userName || "") === selectedUser;
    return matchKeyword && matchUser;
  });

  const today = new Date().toDateString();
  const todayRows = filteredRows.filter((row) => {
    const dateText = row?.time || row?.date;
    return dateText && new Date(dateText).toDateString() === today;
  });
  const todayDuration = todayRows.reduce((sum, row) => sum + Number(row?.duration ?? row?.durationMin ?? 0), 0);
  const activeUsers = new Set(todayRows.map((row) => String(row?.user || row?.userName || "未知用户"))).size;

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
    const emptyTip = appState.playbackHistoryWarning || "暂无播放历史记录。请先连接 Emby 并产生播放行为。";
    elements.logList.innerHTML = `<tr><td colspan="6">${emptyTip}</td></tr>`;
    return;
  }

  elements.logList.innerHTML = filteredRows
    .slice(0, 300)
    .map((row) => {
      const dateText = row?.time || row?.date || "";
      const userName = String(row?.user || row?.userName || "未知用户");
      const title = String(row?.title || row?.mediaName || "未知媒体");
      const { start: startText, end: endText } = resolvePlaybackStartEnd(row, dateText);
      const client = String(row?.client || row?.player?.software || "未知客户端");
      const device = String(row?.device || row?.player?.device || "未知设备");
      return `
        <tr>
          <td class="playback-cell-time" data-label="时间">
            <div class="user-meta">
              <strong>${formatDate(dateText).split(" ")[0] || "-"}</strong>
              <span>${formatDate(dateText).split(" ")[1] || "-"}</span>
            </div>
          </td>
          <td class="playback-cell-user" data-label="用户">
            <div class="playback-user-cell">
              <span class="playback-user-avatar">${initials(userName)}</span>
              <strong>${userName}</strong>
            </div>
          </td>
          <td class="playback-cell-media" data-label="内容媒体">
            <div class="playback-media-cell">
              <div class="playback-media-cell-text">
                <strong>${title}</strong>
              </div>
            </div>
          </td>
          <td class="playback-cell-window" data-label="观影时段">
            <div class="playback-time-cell">
              <div class="playback-time-row">
                <span class="playback-time-label">开始</span>
                <span class="playback-time-value">${startText}</span>
              </div>
              <div class="playback-time-row">
                <span class="playback-time-label">结束</span>
                <span class="playback-time-value">${endText}</span>
              </div>
            </div>
          </td>
          <td class="playback-cell-client" data-label="客户端接入">
            <span class="playback-client">${client}</span>
          </td>
          <td class="playback-cell-device" data-label="终端设备">
            <span class="playback-device">${device}</span>
          </td>
        </tr>
      `
    })
    .join("");
}

function renderMissing() {
  if (!elements.missingList) {
    return;
  }
  const summary = appState.missingSummary && typeof appState.missingSummary === "object" ? appState.missingSummary : {};
  const rows = Array.isArray(appState.missingRows) ? appState.missingRows : [];
  const warnings = Array.isArray(appState.missingWarnings) ? appState.missingWarnings : [];

  if (elements.missingStatScannedSeries) {
    elements.missingStatScannedSeries.textContent = String(summary.scannedSeries || 0);
  }
  if (elements.missingStatMatchedSeries) {
    elements.missingStatMatchedSeries.textContent = String(summary.matchedTmdbSeries || 0);
  }
  if (elements.missingStatMissingSeries) {
    elements.missingStatMissingSeries.textContent = String(summary.missingSeries || 0);
  }
  if (elements.missingStatMissingEpisodes) {
    elements.missingStatMissingEpisodes.textContent = String(summary.missingEpisodeCount || 0);
  }

  const scannedAt = summary.scannedAt ? formatDate(summary.scannedAt) : "尚未巡检";
  const statusText = appState.missingLoading
    ? "正在执行缺集巡检，请稍候..."
    : `最近巡检：${scannedAt}。`;
  const warningText = warnings.length ? `警告：${warnings[0]}` : "";
  if (elements.missingFeedback) {
    elements.missingFeedback.textContent = warningText ? `${statusText} ${warningText}` : statusText;
  }

  if (!rows.length) {
    elements.missingList.innerHTML = `<tr><td colspan="7">暂无缺集结果。请点击“立即巡检”开始扫描。</td></tr>`;
    return;
  }

  elements.missingList.innerHTML = rows
    .map((row) => {
      const seriesName = escapeHtml(String(row?.seriesName || "未命名剧集"));
      const seasonNoRaw = Number(row?.seasonNo || 0);
      const seasonText = seasonNoRaw > 0 ? `第 ${seasonNoRaw} 季` : "-";
      const missingEpisodes = Array.isArray(row?.missingEpisodes) ? row.missingEpisodes : [];
      const missingText = missingEpisodes.length ? missingEpisodes.map((item) => `E${item}`).join(", ") : "-";
      const completeness = escapeHtml(String(row?.completeness || "-"));
      const status = String(row?.status || "");
      const reasonText =
        status === "missing"
          ? "-"
          : String(row?.reason || "").trim() || "未返回明确原因（可能是 TMDB 匹配失败）";
      const scannedAtText = row?.scannedAt ? formatDate(row.scannedAt) : scannedAt;
      const statusBadge =
        status === "missing"
          ? `<span class="badge badge-danger">缺失中</span>`
          : `<span class="badge badge-warning">匹配失败</span>`;
      return `
        <tr>
          <td class="missing-cell-series"><strong>${seriesName}</strong></td>
          <td class="missing-cell-season" data-label="季">${escapeHtml(seasonText)}</td>
          <td class="missing-cell-episodes" data-label="缺失集号"><span class="missing-episodes">${escapeHtml(missingText)}</span></td>
          <td class="missing-cell-completeness" data-label="已播出完整度">${completeness}</td>
          <td class="missing-cell-status" data-label="状态">${statusBadge}</td>
          <td class="missing-cell-reason" data-label="失败原因"><span class="missing-reason">${escapeHtml(reasonText)}</span></td>
          <td class="missing-cell-scanned" data-label="最后巡检">${escapeHtml(scannedAtText)}</td>
        </tr>
      `;
    })
    .join("");
}

async function loadMissingList(options = {}) {
  const quiet = Boolean(options.quiet);
  const params = new URLSearchParams();
  params.set("limit", "2000");
  const keyword = String(elements.missingSearch?.value || "").trim();
  const status = String(elements.missingStatus?.value || "all").trim();
  if (keyword) {
    params.set("q", keyword);
  }
  if (status && status !== "all") {
    params.set("status", status);
  }

  try {
    const payload = await inviteApiFetch(`/api/missing/list?${params.toString()}`);
    appState.missingRows = Array.isArray(payload?.rows) ? payload.rows : [];
    appState.missingSummary = payload?.summary && typeof payload.summary === "object" ? payload.summary : appState.missingSummary;
    appState.missingWarnings = Array.isArray(payload?.warnings) ? payload.warnings : [];
    appState.missingScannedOnce = Boolean(summaryHasScanData(appState.missingSummary));
    renderMissing();
    return appState.missingRows;
  } catch (error) {
    if (!quiet && elements.missingFeedback) {
      elements.missingFeedback.textContent = `读取缺集结果失败：${error.message || "未知错误"}`;
    }
    throw error;
  }
}

function summaryHasScanData(summary) {
  if (!summary || typeof summary !== "object") {
    return false;
  }
  return Boolean(summary.scannedAt || Number(summary.scannedSeries || 0) > 0);
}

async function scanMissingEpisodes() {
  if (appState.missingLoading) {
    return;
  }
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    if (elements.missingFeedback) {
      elements.missingFeedback.textContent = "请先在媒体库配置里配置 Emby 连接。";
    }
    return;
  }
  const tmdbToken = String(appState.config.tmdbToken || "").trim();
  if (!tmdbToken) {
    if (elements.missingFeedback) {
      elements.missingFeedback.textContent = "请先在媒体库配置里填写 TMDB Token。";
    }
    return;
  }

  appState.missingLoading = true;
  if (elements.missingScanBtn) {
    elements.missingScanBtn.disabled = true;
    elements.missingScanBtn.textContent = "巡检中...";
  }
  if (elements.missingFeedback) {
    elements.missingFeedback.textContent = "正在核对 Emby 已入库季与 TMDB 已播出单集...";
  }

  try {
    await inviteApiFetch("/api/missing/scan", {
      method: "POST",
      body: JSON.stringify({
        tmdbToken,
        tmdbLanguage: appState.config.tmdbLanguage || "zh-CN",
        tmdbRegion: appState.config.tmdbRegion || "CN",
        scanLimit: 2000,
      }),
    });
    await loadMissingList({ quiet: true });
    appState.missingScannedOnce = true;
    if (elements.missingFeedback) {
      elements.missingFeedback.textContent = "巡检完成，已更新缺集结果。";
    }
    addSyncEvent("缺集巡检完成", "缺集管理已完成全库巡检并刷新结果。", "success");
  } catch (error) {
    if (elements.missingFeedback) {
      elements.missingFeedback.textContent = `缺集巡检失败：${error.message || "未知错误"}`;
    }
    addSyncEvent("缺集巡检失败", String(error?.message || "未知错误"), "danger");
  } finally {
    appState.missingLoading = false;
    if (elements.missingScanBtn) {
      elements.missingScanBtn.disabled = false;
      elements.missingScanBtn.textContent = "立即巡检";
    }
    renderMissing();
  }
}

function persistLocalState() {
  saveJson(STORAGE_KEYS.config, appState.config);
  saveJson(STORAGE_KEYS.invites, appState.invites);
  saveJson(STORAGE_KEYS.renewals, appState.renewals);
  saveJson(STORAGE_KEYS.botConfig, appState.botConfig);
  saveJson(STORAGE_KEYS.notificationConfig, appState.notificationConfig);
  saveJson(STORAGE_KEYS.aiConfig, appState.aiConfig);
  saveJson(STORAGE_KEYS.coverStudioConfig, appState.coverStudioConfig);
  saveJson(STORAGE_KEYS.libraryDirectoryConfig, appState.libraryDirectoryConfig);
  saveJson(STORAGE_KEYS.drive115Config, appState.drive115Config);
  saveJson(STORAGE_KEYS.hdhiveConfig, appState.hdhiveConfig);
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

function getPrimaryLibraryDirectoryRoot(config = appState.libraryDirectoryConfig) {
  const normalized = normalizeLibraryDirectoryConfig(config);
  return normalized.roots[0] || { name: "本地媒体库", path: "", enabled: true, maxDepth: 4, categories: [] };
}

function formatLibraryDirectoryCategories(categories) {
  if (!Array.isArray(categories) || categories.length === 0) {
    return "";
  }
  return categories
    .map((category) => {
      const label = String(category?.label || "").trim();
      const aliases = Array.isArray(category?.aliases) ? category.aliases.map((item) => String(item || "").trim()).filter(Boolean) : [];
      const pathValue = String(category?.path || "").trim();
      if (!label && !pathValue) {
        return "";
      }
      return `${label}${aliases.length ? ` | ${aliases.join(",")}` : ""}${pathValue ? ` | ${pathValue}` : ""}`;
    })
    .filter(Boolean)
    .join("\n");
}

function parseLibraryDirectoryCategoryLines(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
    .map((line) => {
      const parts = line.split("|").map((part) => part.trim());
      const label = String(parts[0] || "").trim();
      let aliasesText = "";
      let pathValue = "";
      if (parts.length === 2) {
        if (/[/\\]/.test(parts[1])) {
          pathValue = parts[1];
        } else {
          aliasesText = parts[1];
        }
      } else if (parts.length >= 3) {
        aliasesText = parts[1];
        pathValue = parts.slice(2).join("|").trim();
      }
      const aliases = aliasesText
        .split(/[，,]/)
        .map((item) => item.trim())
        .filter(Boolean);
      const normalizedPath = String(pathValue || label).trim();
      if (!label && !normalizedPath) {
        return null;
      }
      return {
        label,
        aliases,
        path: normalizedPath
      };
    })
    .filter(Boolean);
}

function readLibraryDirectoryConfigFromInputs() {
  const path = String(elements.libraryDirectoryRootPath?.value || "").trim();
  const name = String(elements.libraryDirectoryRootName?.value || "").trim() || "本地媒体库";
  let maxDepth = Number.parseInt(String(elements.libraryDirectoryMaxDepth?.value || "4"), 10);
  if (!Number.isFinite(maxDepth)) {
    maxDepth = 4;
  }
  maxDepth = Math.max(1, Math.min(8, maxDepth));
  const categories = parseLibraryDirectoryCategoryLines(elements.libraryDirectoryCategories?.value || "");
  if (!path) {
    return normalizeLibraryDirectoryConfig({ roots: [] });
  }
  return normalizeLibraryDirectoryConfig({
    roots: [
      {
        name,
        path,
        enabled: true,
        maxDepth,
        categories
      }
    ]
  });
}

function renderLibraryDirectorySettings() {
  const root = getPrimaryLibraryDirectoryRoot();
  if (elements.libraryDirectoryRootPath) {
    elements.libraryDirectoryRootPath.value = root.path || "";
  }
  if (elements.libraryDirectoryRootName) {
    elements.libraryDirectoryRootName.value = root.name || "本地媒体库";
  }
  if (elements.libraryDirectoryMaxDepth) {
    elements.libraryDirectoryMaxDepth.value = String(root.maxDepth || 4);
  }
  if (elements.libraryDirectoryCategories) {
    elements.libraryDirectoryCategories.value = formatLibraryDirectoryCategories(root.categories);
  }
  updateLibraryDirectoryFeedback({ saved: true });
}

function updateLibraryDirectoryFeedback(options = {}) {
  if (!elements.libraryDirectoryFeedback) {
    return;
  }
  const { saved = false, message = "" } = options;
  if (message) {
    elements.libraryDirectoryFeedback.textContent = message;
    return;
  }
  const config = readLibraryDirectoryConfigFromInputs();
  const root = getPrimaryLibraryDirectoryRoot(config);
  const categoryCount = Array.isArray(root.categories) ? root.categories.length : 0;
  if (!root.path) {
    elements.libraryDirectoryFeedback.textContent =
      "未配置本地目录分类时，“亚洲电影 / 韩影 / 国产动漫”会明确提示未配置，不再回退到 Emby 元数据猜测。";
    return;
  }
  if (categoryCount === 0) {
    elements.libraryDirectoryFeedback.textContent =
      "已填写根目录，但还没有分类映射。建议至少配置“亚洲电影 | 韩影,日影 | 电影/亚洲电影”这类规则。";
    return;
  }
  if (saved || JSON.stringify(config) === JSON.stringify(appState.libraryDirectoryConfig)) {
    elements.libraryDirectoryFeedback.textContent = `当前已配置 ${categoryCount} 条目录分类映射，AI 将优先按本地目录严格查询。`;
    return;
  }
  elements.libraryDirectoryFeedback.textContent = `已填写 ${categoryCount} 条目录分类映射，点击“保存”后目录类查询生效。`;
}

function readCoverStudioDraftFromInputs() {
  const viewIds = getSelectedCoverStudioViewIds();
  return normalizeCoverStudioConfig({
    ...appState.coverStudioConfig,
    draft: {
      ...(appState.coverStudioConfig?.draft || {}),
      viewId: viewIds[0] || "",
      viewIds,
      templateKey: String(elements.coverStudioTemplateKey?.value || "fan_spread").trim() || "fan_spread",
      pickMode: String(elements.coverStudioPickMode?.value || "random").trim(),
      titleText: String(elements.coverStudioTitleText?.value || "").trim(),
      subtitleText: String(elements.coverStudioSubtitleText?.value || "").trim(),
      fontKey: normalizeCoverStudioFontKey(elements.coverStudioFontKey?.value),
      titleFontSize: elements.coverStudioTitleSize?.value || 108,
      subtitleFontSize: elements.coverStudioSubtitleSize?.value || 44,
      presetName: String(elements.coverStudioPresetName?.value || "").trim() || "默认封面",
      titleAlign: String(elements.coverStudioTitleAlign?.value || "left").trim() || "left",
      overlayStrength: 0,
      posterCount: elements.coverStudioPosterCount?.value ?? 5,
      accentTone: String(elements.coverStudioAccentTone?.value || "blue").trim() || "blue",
      posterRotation: elements.coverStudioPosterRotation?.value ?? 42,
      titleYOffset: elements.coverStudioTitleYOffset?.value ?? 0
    }
  }).draft;
}

function getSelectedCoverStudioViewIds() {
  const checked = elements.coverStudioViewPicker
    ? [...elements.coverStudioViewPicker.querySelectorAll('input[type="checkbox"]:checked')]
        .map((input) => String(input.value || "").trim())
        .filter(Boolean)
    : [];
  if (elements.coverStudioViewPicker) {
    return [...new Set(checked)].slice(0, 30);
  }
  const draftIds = appState.coverStudioConfig?.draft?.viewIds;
  if (Array.isArray(draftIds) && draftIds.length) {
    return [...new Set(draftIds.map((item) => String(item || "").trim()).filter(Boolean))].slice(0, 30);
  }
  const legacy = String(elements.coverStudioViewSelect?.value || appState.coverStudioConfig?.draft?.viewId || "").trim();
  return legacy ? [legacy] : [];
}

function getCoverStudioModeMeta(templateKey) {
  const modes = Array.isArray(appState.coverStudioModes) && appState.coverStudioModes.length
    ? appState.coverStudioModes
    : DEFAULT_COVER_STUDIO_MODES;
  return modes.find((mode) => String(mode?.key || "") === String(templateKey || "")) || modes[0];
}

function getCoverStudioModeCapabilities(templateKey) {
  const mode = getCoverStudioModeMeta(templateKey) || {};
  const supports = new Set(Array.isArray(mode.supports) ? mode.supports : []);
  const parsedLimit = Number.parseInt(String(mode.maxPosterCount ?? 8), 10);
  return {
    supports,
    posterLimit: Math.max(2, Math.min(8, Number.isFinite(parsedLimit) ? parsedLimit : 8))
  };
}

function clearCoverStudioPreview({ keepFeedback = false } = {}) {
  appState.coverStudioPreviewToken = "";
  appState.coverStudioPreviewDataUrl = "";
  appState.coverStudioPreviews = [];
  appState.coverStudioSelectedItems = [];
  if (!keepFeedback && elements.coverStudioFeedback) {
    elements.coverStudioFeedback.textContent = "参数已更新，请重新生成预览。";
  }
}

function syncCoverStudioDraftFromInputs({ invalidatePreview = true, keepFeedback = false } = {}) {
  appState.coverStudioConfig = normalizeCoverStudioConfig({
    ...appState.coverStudioConfig,
    draft: readCoverStudioDraftFromInputs()
  });
  if (invalidatePreview) {
    clearCoverStudioPreview({ keepFeedback });
  }
  persistLocalState();
}

function renderCoverStudioPresetOptions() {
  if (!elements.coverStudioPresetName) {
    return;
  }
  const draft = appState.coverStudioConfig?.draft || DEFAULT_COVER_STUDIO_CONFIG.draft;
  elements.coverStudioPresetName.value = draft.presetName || "默认封面";
}

function renderCoverStudioModes() {
  if (!elements.coverStudioTemplateKey) {
    return;
  }
  const modes = Array.isArray(appState.coverStudioModes) && appState.coverStudioModes.length
    ? appState.coverStudioModes
    : DEFAULT_COVER_STUDIO_MODES;
  elements.coverStudioTemplateKey.innerHTML = modes
    .map((mode) => `<option value="${escapeHtml(mode.key)}">${escapeHtml(mode.label)}</option>`)
    .join("");
  const draft = appState.coverStudioConfig?.draft || DEFAULT_COVER_STUDIO_CONFIG.draft;
  const current = modes.some((mode) => mode.key === draft.templateKey) ? draft.templateKey : modes[0].key;
  elements.coverStudioTemplateKey.value = current;
}

function renderCoverStudioFonts() {
  if (!elements.coverStudioFontKey) {
    return;
  }
  const fonts = Array.isArray(appState.coverStudioFonts) && appState.coverStudioFonts.length
    ? appState.coverStudioFonts
    : [{ key: DEFAULT_COVER_STUDIO_FONT_KEY, label: "华文黑体" }];
  elements.coverStudioFontKey.innerHTML = fonts
    .map((font) => `<option value="${escapeHtml(font.key)}">${escapeHtml(font.label)}</option>`)
    .join("");
  const draft = appState.coverStudioConfig?.draft || DEFAULT_COVER_STUDIO_CONFIG.draft;
  elements.coverStudioFontKey.value = fonts.some((font) => font.key === draft.fontKey) ? draft.fontKey : fonts[0].key;
}

function renderCoverStudioAccentTones() {
  if (!elements.coverStudioAccentTone) {
    return;
  }
  const tones = Array.isArray(appState.coverStudioAccentTones) && appState.coverStudioAccentTones.length
    ? appState.coverStudioAccentTones
    : DEFAULT_COVER_STUDIO_ACCENT_TONES;
  elements.coverStudioAccentTone.innerHTML = tones
    .map((tone) => `<option value="${escapeHtml(tone.key)}">${escapeHtml(tone.label)}</option>`)
    .join("");
}

function renderCoverStudioTitleAlignOptions() {
  if (!elements.coverStudioTitleAlign) {
    return;
  }
  const options = Array.isArray(appState.coverStudioTitleAlignOptions) && appState.coverStudioTitleAlignOptions.length
    ? appState.coverStudioTitleAlignOptions
    : DEFAULT_COVER_STUDIO_TITLE_ALIGN_OPTIONS;
  elements.coverStudioTitleAlign.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.key)}">${escapeHtml(option.label)}</option>`)
    .join("");
}

function buildCoverStudioViewOption(view) {
  const count = Number(view?.recursiveItemCount || view?.childCount || 0);
  const suffix = count > 0 ? ` · ${count} 项` : "";
  return `<option value="${escapeHtml(view.id)}">${escapeHtml(view.name)}${escapeHtml(suffix)}</option>`;
}

function getCoverStudioLibraryCopy(viewId) {
  const normalizedId = String(viewId || "").trim();
  const view = (appState.coverStudioViews || []).find((item) => String(item?.id || "").trim() === normalizedId);
  const titleText = String(view?.name || "").trim();
  if (!titleText) {
    return null;
  }
  const normalizedTitle = titleText.replace(/\s+/g, "");
  const match = COVER_STUDIO_LIBRARY_SUBTITLES.find(([name]) => normalizedTitle.includes(name));
  return {
    titleText,
    subtitleText: match ? match[1] : ""
  };
}

function applyCoverStudioManualLibraryCopy() {
  const selectedViewIds = getSelectedCoverStudioViewIds();
  if (selectedViewIds.length !== 1) {
    return false;
  }
  const copy = getCoverStudioLibraryCopy(selectedViewIds[0]);
  if (!copy) {
    return false;
  }
  if (elements.coverStudioTitleText) {
    elements.coverStudioTitleText.value = copy.titleText;
  }
  if (elements.coverStudioSubtitleText) {
    elements.coverStudioSubtitleText.value = copy.subtitleText;
  }
  return true;
}

function applyCoverStudioScheduleLibraryCopy() {
  const viewId = String(elements.coverStudioScheduleViewSelect?.value || "").trim();
  const copy = getCoverStudioLibraryCopy(viewId);
  if (!copy) {
    clearCoverStudioSchedulePreview();
    return false;
  }
  appState.coverStudioScheduleDraft = {
    ...readCoverStudioScheduleTemplateFromInputs(),
    titleText: copy.titleText,
    subtitleText: copy.subtitleText
  };
  renderCoverStudioScheduleTemplateInputs();
  clearCoverStudioSchedulePreview();
  return true;
}

function renderCoverStudioViews() {
  if (!elements.coverStudioViewSelect) {
    return;
  }
  const draftViewIds = Array.isArray(appState.coverStudioConfig?.draft?.viewIds)
    ? appState.coverStudioConfig.draft.viewIds
    : [appState.coverStudioConfig?.draft?.viewId || appState.coverStudioConfig?.lastViewId || ""];
  const selectedIds = new Set(draftViewIds.map((item) => String(item || "").trim()).filter(Boolean));
  const current = [...selectedIds][0] || "";
  const options = ['<option value="">请选择 Emby 视图</option>'];
  (appState.coverStudioViews || []).forEach((view) => {
    options.push(buildCoverStudioViewOption(view));
  });
  elements.coverStudioViewSelect.innerHTML = options.join("");
  if ((appState.coverStudioViews || []).some((view) => String(view.id) === current)) {
    elements.coverStudioViewSelect.value = current;
  }
  if (elements.coverStudioViewPicker) {
    const views = appState.coverStudioViews || [];
    elements.coverStudioViewPicker.innerHTML = views.length
      ? views
          .map((view) => {
            const id = String(view?.id || "").trim();
            const count = Number(view?.recursiveItemCount || view?.childCount || 0);
            const suffix = count > 0 ? `${count} 项` : "媒体库视图";
            return `<label class="cover-studio-view-option"><input type="checkbox" value="${escapeHtml(id)}"${selectedIds.has(id) ? " checked" : ""}><span>${escapeHtml(view?.name || "未命名视图")}</span><small>${escapeHtml(suffix)}</small></label>`;
          })
          .join("")
      : '<div class="cover-studio-view-empty">暂无可选 Emby 媒体库视图。</div>';
  }
}

function renderCoverStudioPreview() {
  if (!elements.coverStudioPreviewStage) {
    return;
  }
  if (appState.coverStudioPreviewDataUrl) {
    elements.coverStudioPreviewStage.innerHTML = `<img src="${appState.coverStudioPreviewDataUrl}" alt="封面预览">`;
  } else {
    elements.coverStudioPreviewStage.innerHTML = '<div class="empty-state">生成预览后会显示封面图。</div>';
  }
  const items = Array.isArray(appState.coverStudioSelectedItems) ? appState.coverStudioSelectedItems : [];
  if (elements.coverStudioPreviewState) {
    elements.coverStudioPreviewState.textContent = appState.coverStudioPreviewDataUrl ? "已有预览" : "未生成";
  }
  if (elements.coverStudioSelectedItems) {
    const previewCount = Array.isArray(appState.coverStudioPreviews) ? appState.coverStudioPreviews.length : 0;
    elements.coverStudioSelectedItems.textContent = previewCount > 1
      ? `已为 ${previewCount} 个媒体库分别生成预览；当前展示第一份。`
      : items.length
      ? `已选海报：${items.map((item) => String(item?.name || item?.id || "")).filter(Boolean).join(" / ")}`
      : "当前还没有取图结果。";
  }
}

function renderCoverStudioModeControls() {
  const draft = appState.coverStudioConfig?.draft || DEFAULT_COVER_STUDIO_CONFIG.draft;
  const mode = getCoverStudioModeMeta(draft.templateKey);
  const { supports, posterLimit } = getCoverStudioModeCapabilities(draft.templateKey);
  if (elements.coverStudioTemplateChip) {
    elements.coverStudioTemplateChip.textContent = String(mode?.label || "模板").trim() || "模板";
  }
  if (elements.coverStudioTemplateHint) {
    const disabledLabels = [
      supports.has("titleAlign") ? null : "标题对齐",
      "全局遮罩已关闭",
      supports.has("posterCount") ? null : "海报数量",
      supports.has("accentTone") ? null : "主色倾向",
      supports.has("posterRotation") ? null : "旋转幅度",
      supports.has("titleYOffset") ? null : "标题纵向位置"
    ].filter(Boolean);
    const summary = String(mode?.description || "").trim();
    elements.coverStudioTemplateHint.textContent = disabledLabels.length
      ? `${summary} 当前模板固定：${disabledLabels.join(" / ")}。`
      : `${summary} 当前模板支持全部布局微调。`;
  }
  if (elements.coverStudioTitleAlign) {
    elements.coverStudioTitleAlign.value = draft.titleAlign || "left";
    elements.coverStudioTitleAlign.disabled = !supports.has("titleAlign");
  }
  if (elements.coverStudioOverlayStrength) {
    elements.coverStudioOverlayStrength.value = "0";
    elements.coverStudioOverlayStrength.disabled = true;
  }
  if (elements.coverStudioPosterCount) {
    elements.coverStudioPosterCount.max = String(posterLimit);
    elements.coverStudioPosterCount.value = String(draft.posterCount ?? 5);
    elements.coverStudioPosterCount.disabled = !supports.has("posterCount");
  }
  if (elements.coverStudioAccentTone) {
    elements.coverStudioAccentTone.value = draft.accentTone || "blue";
    elements.coverStudioAccentTone.disabled = !supports.has("accentTone");
  }
  if (elements.coverStudioPosterRotation) {
    elements.coverStudioPosterRotation.value = String(supports.has("posterRotation") ? (draft.posterRotation ?? 42) : 0);
    elements.coverStudioPosterRotation.disabled = !supports.has("posterRotation");
  }
  if (elements.coverStudioTitleYOffset) {
    elements.coverStudioTitleYOffset.value = String(draft.titleYOffset ?? 0);
    elements.coverStudioTitleYOffset.disabled = !supports.has("titleYOffset");
  }
}

function renderCoverStudioStatus() {
  const draft = appState.coverStudioConfig?.draft || DEFAULT_COVER_STUDIO_CONFIG.draft;
  const selectedViewIds = Array.isArray(draft.viewIds) && draft.viewIds.length ? draft.viewIds : [draft.viewId];
  const selectedViews = (appState.coverStudioViews || []).filter((view) => selectedViewIds.includes(String(view.id)));
  if (elements.coverStudioSelectionChip) {
    elements.coverStudioSelectionChip.textContent = selectedViews.length ? `已选 ${selectedViews.length} 个` : "未选择";
  }
  if (elements.coverStudioStatusBadge) {
    const ready = Boolean(appState.coverStudioViews?.length);
    elements.coverStudioStatusBadge.textContent = ready ? "已就绪" : "未加载";
    elements.coverStudioStatusBadge.classList.toggle("is-on", ready);
    elements.coverStudioStatusBadge.classList.toggle("is-off", !ready);
  }
  if (elements.coverStudioRestoreBtn) {
    const backups = appState.coverStudioConfig?.backups || {};
    const hasBackup = selectedViews.some((view) => Boolean((backups[String(view.id || "").trim()] || {})?.primary?.path));
    elements.coverStudioRestoreBtn.disabled = !hasBackup;
  }
  if (elements.coverStudioApplyBtn) {
    elements.coverStudioApplyBtn.disabled = !selectedViews.length || !(appState.coverStudioPreviews?.length || appState.coverStudioPreviewToken);
  }
}

function buildCoverStudioScheduleTemplate(draft) {
  const templateKey = String(draft.templateKey || "fan_spread");
  const { supports, posterLimit } = getCoverStudioModeCapabilities(templateKey);
  const posterCount = Number(draft.posterCount ?? 5);
  const posterRotation = Number(draft.posterRotation ?? 42);
  return {
    templateKey,
    pickMode: String(draft.pickMode || "random"),
    titleText: String(draft.titleText || ""),
    subtitleText: String(draft.subtitleText || ""),
    fontKey: normalizeCoverStudioFontKey(draft.fontKey),
    titleFontSize: Number(draft.titleFontSize || 108),
    subtitleFontSize: Number(draft.subtitleFontSize || 44),
    titleAlign: String(draft.titleAlign || "left"),
    posterCount: Math.max(2, Math.min(posterLimit, Number.isFinite(posterCount) ? posterCount : 5)),
    accentTone: String(draft.accentTone || "blue"),
    posterRotation: supports.has("posterRotation") && Number.isFinite(posterRotation) ? posterRotation : 0,
    titleYOffset: Number(draft.titleYOffset ?? 0)
  };
}

function getCoverStudioScheduleTemplateDraft() {
  if (appState.coverStudioScheduleDraft) {
    return appState.coverStudioScheduleDraft;
  }
  if (appState.coverStudioConfig?.scheduleDraft) {
    return appState.coverStudioConfig.scheduleDraft;
  }
  return buildCoverStudioScheduleTemplate(readCoverStudioDraftFromInputs());
}

function setCoverStudioScheduleSelectOptions(element, options, selected) {
  if (!element) {
    return;
  }
  element.innerHTML = options.map((option) => `<option value="${escapeHtml(option.key)}">${escapeHtml(option.label)}</option>`).join("");
  const fallback = options[0]?.key || "";
  element.value = options.some((option) => option.key === selected) ? selected : fallback;
}

function renderCoverStudioScheduleTemplateInputs() {
  const draft = getCoverStudioScheduleTemplateDraft();
  const modes = Array.isArray(appState.coverStudioModes) && appState.coverStudioModes.length
    ? appState.coverStudioModes
    : DEFAULT_COVER_STUDIO_MODES;
  const fonts = Array.isArray(appState.coverStudioFonts) && appState.coverStudioFonts.length
    ? appState.coverStudioFonts
    : [{ key: DEFAULT_COVER_STUDIO_FONT_KEY, label: "华文黑体" }];
  const tones = Array.isArray(appState.coverStudioAccentTones) && appState.coverStudioAccentTones.length
    ? appState.coverStudioAccentTones
    : DEFAULT_COVER_STUDIO_ACCENT_TONES;
  const alignments = Array.isArray(appState.coverStudioTitleAlignOptions) && appState.coverStudioTitleAlignOptions.length
    ? appState.coverStudioTitleAlignOptions
    : DEFAULT_COVER_STUDIO_TITLE_ALIGN_OPTIONS;
  const { supports, posterLimit } = getCoverStudioModeCapabilities(draft.templateKey);
  setCoverStudioScheduleSelectOptions(elements.coverStudioScheduleTemplateKey, modes, draft.templateKey);
  setCoverStudioScheduleSelectOptions(elements.coverStudioScheduleFontKey, fonts, draft.fontKey);
  setCoverStudioScheduleSelectOptions(elements.coverStudioScheduleAccentTone, tones, draft.accentTone);
  setCoverStudioScheduleSelectOptions(elements.coverStudioScheduleTitleAlign, alignments, draft.titleAlign);
  if (elements.coverStudioSchedulePickMode) {
    elements.coverStudioSchedulePickMode.value = draft.pickMode === "recent" ? "recent" : "random";
  }
  if (elements.coverStudioScheduleTitleText) elements.coverStudioScheduleTitleText.value = draft.titleText || "";
  if (elements.coverStudioScheduleSubtitleText) elements.coverStudioScheduleSubtitleText.value = draft.subtitleText || "";
  if (elements.coverStudioScheduleTitleSize) elements.coverStudioScheduleTitleSize.value = String(draft.titleFontSize || 108);
  if (elements.coverStudioScheduleSubtitleSize) elements.coverStudioScheduleSubtitleSize.value = String(draft.subtitleFontSize || 44);
  if (elements.coverStudioSchedulePosterCount) {
    elements.coverStudioSchedulePosterCount.max = String(posterLimit);
    elements.coverStudioSchedulePosterCount.disabled = !supports.has("posterCount");
    elements.coverStudioSchedulePosterCount.value = String(Math.max(2, Math.min(posterLimit, Number(draft.posterCount ?? 5))));
  }
  if (elements.coverStudioSchedulePosterRotation) {
    elements.coverStudioSchedulePosterRotation.disabled = !supports.has("posterRotation");
    elements.coverStudioSchedulePosterRotation.value = String(supports.has("posterRotation") ? Number(draft.posterRotation ?? 42) : 0);
  }
  if (elements.coverStudioScheduleTitleYOffset) elements.coverStudioScheduleTitleYOffset.value = String(Number(draft.titleYOffset ?? 0));
}

function readCoverStudioScheduleTemplateFromInputs() {
  const fallback = getCoverStudioScheduleTemplateDraft();
  const value = (element, defaultValue = "") => String(element?.value ?? defaultValue).trim();
  const numberValue = (element, defaultValue) => {
    const parsed = Number(element?.value);
    return Number.isFinite(parsed) ? parsed : defaultValue;
  };
  const templateKey = value(elements.coverStudioScheduleTemplateKey, fallback.templateKey) || fallback.templateKey;
  const { supports, posterLimit } = getCoverStudioModeCapabilities(templateKey);
  const posterCount = numberValue(elements.coverStudioSchedulePosterCount, fallback.posterCount);
  const posterRotation = numberValue(elements.coverStudioSchedulePosterRotation, fallback.posterRotation);
  return {
    templateKey,
    pickMode: value(elements.coverStudioSchedulePickMode, fallback.pickMode) === "recent" ? "recent" : "random",
    titleText: value(elements.coverStudioScheduleTitleText, fallback.titleText),
    subtitleText: value(elements.coverStudioScheduleSubtitleText, fallback.subtitleText),
    fontKey: value(elements.coverStudioScheduleFontKey, fallback.fontKey) || fallback.fontKey,
    titleFontSize: numberValue(elements.coverStudioScheduleTitleSize, fallback.titleFontSize),
    subtitleFontSize: numberValue(elements.coverStudioScheduleSubtitleSize, fallback.subtitleFontSize),
    titleAlign: value(elements.coverStudioScheduleTitleAlign, fallback.titleAlign) || fallback.titleAlign,
    posterCount: Math.max(2, Math.min(posterLimit, posterCount)),
    accentTone: value(elements.coverStudioScheduleAccentTone, fallback.accentTone) || fallback.accentTone,
    posterRotation: supports.has("posterRotation") ? Math.max(0, Math.min(100, posterRotation)) : 0,
    titleYOffset: numberValue(elements.coverStudioScheduleTitleYOffset, fallback.titleYOffset)
  };
}

function readCoverStudioSchedulePreviewDraft() {
  const viewId = String(elements.coverStudioScheduleViewSelect?.value || "").trim();
  return {
    ...readCoverStudioScheduleTemplateFromInputs(),
    viewId,
    viewIds: viewId ? [viewId] : [],
    overlayStrength: 0,
    lockedItemIds: [],
    previewOnly: true
  };
}

function renderCoverStudioSchedulePreview() {
  if (!elements.coverStudioSchedulePreviewStage) {
    return;
  }
  elements.coverStudioSchedulePreviewStage.innerHTML = appState.coverStudioSchedulePreviewDataUrl
    ? `<img src="${appState.coverStudioSchedulePreviewDataUrl}" alt="自动封面预览">`
    : '<div class="empty-state">选择媒体库并生成预览后，会在这里显示自动封面。</div>';
}

function clearCoverStudioSchedulePreview() {
  appState.coverStudioSchedulePreviewDataUrl = "";
  renderCoverStudioSchedulePreview();
}

function setCoverStudioMode(mode) {
  const nextMode = mode === "auto" ? "auto" : "manual";
  appState.coverStudioMode = nextMode;
  localStorage.setItem(STORAGE_KEYS.coverStudioMode, nextMode);
  elements.coverStudioManualPanel?.toggleAttribute("hidden", nextMode !== "manual");
  elements.coverStudioAutoPanel?.toggleAttribute("hidden", nextMode !== "auto");
  elements.coverStudioModeTabs?.querySelectorAll("[data-cover-studio-mode]").forEach((button) => {
    const isActive = button.dataset.coverStudioMode === nextMode;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-selected", isActive ? "true" : "false");
  });
}

function renderCoverStudioScheduleViewOptions() {
  const select = elements.coverStudioScheduleViewSelect;
  if (!select) {
    return;
  }
  const selected = String(select.value || "").trim();
  const scheduledIds = new Set((appState.coverStudioConfig?.schedules || []).map((plan) => String(plan?.viewId || "").trim()));
  const availableViews = (appState.coverStudioViews || []).filter((view) => !scheduledIds.has(String(view?.id || "").trim()));
  select.innerHTML = ['<option value="">请选择 Emby 媒体库</option>']
    .concat(availableViews.map((view) => `<option value="${escapeHtml(view.id)}">${escapeHtml(view.name || view.id)}</option>`))
    .join("");
  if (availableViews.some((view) => String(view?.id || "") === selected)) {
    select.value = selected;
  }
  if (elements.coverStudioScheduleAddBtn) {
    elements.coverStudioScheduleAddBtn.disabled = !availableViews.length;
    elements.coverStudioScheduleAddBtn.textContent = availableViews.length ? "添加自动计划" : "已全部添加";
  }
}

function getCoverStudioSchedulePlan(planId) {
  return (appState.coverStudioConfig?.schedules || []).find((plan) => plan.id === planId) || null;
}

function getCoverStudioSchedulePlanViewName(plan) {
  return String(
    plan?.viewName
    || (appState.coverStudioViews || []).find((view) => String(view?.id || "") === String(plan?.viewId || ""))?.name
    || plan?.viewId
    || "未命名媒体库"
  );
}

function getCoverStudioScheduleTemplate(plan) {
  return {
    ...buildCoverStudioScheduleTemplate(DEFAULT_COVER_STUDIO_CONFIG.draft),
    ...(plan?.template && typeof plan.template === "object" ? plan.template : {})
  };
}

function closeCoverStudioScheduleEditor() {
  appState.coverStudioEditingScheduleId = "";
  if (elements.coverStudioScheduleEditModal) {
    elements.coverStudioScheduleEditModal.hidden = true;
  }
}

function renderCoverStudioScheduleEditor(plan) {
  const form = elements.coverStudioScheduleEditForm;
  if (!form || !plan) {
    return;
  }
  const template = getCoverStudioScheduleTemplate(plan);
  const modes = Array.isArray(appState.coverStudioModes) && appState.coverStudioModes.length
    ? appState.coverStudioModes
    : DEFAULT_COVER_STUDIO_MODES;
  const fonts = Array.isArray(appState.coverStudioFonts) && appState.coverStudioFonts.length
    ? appState.coverStudioFonts
    : [{ key: DEFAULT_COVER_STUDIO_FONT_KEY, label: "华文黑体" }];
  const tones = Array.isArray(appState.coverStudioAccentTones) && appState.coverStudioAccentTones.length
    ? appState.coverStudioAccentTones
    : DEFAULT_COVER_STUDIO_ACCENT_TONES;
  const alignments = Array.isArray(appState.coverStudioTitleAlignOptions) && appState.coverStudioTitleAlignOptions.length
    ? appState.coverStudioTitleAlignOptions
    : DEFAULT_COVER_STUDIO_TITLE_ALIGN_OPTIONS;
  const selectedTemplate = String(template.templateKey || "fan_spread");
  const { supports, posterLimit } = getCoverStudioModeCapabilities(selectedTemplate);
  const optionsFor = (items, selected) => items
    .map((item) => `<option value="${escapeHtml(item.key)}"${item.key === selected ? " selected" : ""}>${escapeHtml(item.label)}</option>`)
    .join("");
  form.dataset.scheduleId = plan.id;
  form.innerHTML = `
    <div class="cover-studio-schedule-edit-summary">
      <strong>${escapeHtml(getCoverStudioSchedulePlanViewName(plan))}</strong>
      <span>封面与执行参数</span>
    </div>
    <div class="cover-studio-schedule-edit-grid">
      <label class="field"><span>计划时间（Cron）</span><input data-schedule-modal-field="cron" type="text" value="${escapeHtml(plan.cron || "*/5 * * * *")}" placeholder="例如 */5 * * * *"></label>
      <label class="field"><span>布局模板</span><select data-schedule-modal-field="templateKey">${optionsFor(modes, selectedTemplate)}</select></label>
      <label class="field"><span>取图模式</span><select data-schedule-modal-field="pickMode"><option value="recent"${template.pickMode === "recent" ? " selected" : ""}>最近入库</option><option value="random"${template.pickMode === "random" ? " selected" : ""}>随机模式</option></select></label>
      <label class="field"><span>主标题</span><input data-schedule-modal-field="titleText" type="text" value="${escapeHtml(template.titleText || "")}" placeholder="默认使用媒体库名称"></label>
      <label class="field"><span>副标题</span><input data-schedule-modal-field="subtitleText" type="text" value="${escapeHtml(template.subtitleText || "")}" placeholder="可留空"></label>
      <label class="field"><span>字体</span><select data-schedule-modal-field="fontKey">${optionsFor(fonts, normalizeCoverStudioFontKey(template.fontKey))}</select></label>
      <label class="field"><span>标题对齐</span><select data-schedule-modal-field="titleAlign">${optionsFor(alignments, String(template.titleAlign || "left"))}</select></label>
      <label class="field"><span>标题字号</span><input data-schedule-modal-field="titleFontSize" type="number" min="56" max="180" step="2" value="${escapeHtml(template.titleFontSize || 108)}"></label>
      <label class="field"><span>副标题字号</span><input data-schedule-modal-field="subtitleFontSize" type="number" min="22" max="72" step="2" value="${escapeHtml(template.subtitleFontSize || 44)}"></label>
      <label class="field"><span>海报数量</span><input data-schedule-modal-field="posterCount" type="number" min="2" max="${posterLimit}" step="1" value="${escapeHtml(template.posterCount ?? 5)}"${supports.has("posterCount") ? "" : " disabled"}></label>
      <label class="field"><span>主色倾向</span><select data-schedule-modal-field="accentTone">${optionsFor(tones, String(template.accentTone || "blue"))}</select></label>
      <label class="field"><span>旋转幅度</span><input data-schedule-modal-field="posterRotation" type="number" min="0" max="100" step="2" value="${escapeHtml(supports.has("posterRotation") ? (template.posterRotation ?? 42) : 0)}"${supports.has("posterRotation") ? "" : " disabled"}></label>
      <label class="field"><span>标题纵向位置</span><input data-schedule-modal-field="titleYOffset" type="number" min="-160" max="160" step="4" value="${escapeHtml(Number(template.titleYOffset ?? 0))}"></label>
    </div>`;
}

function openCoverStudioScheduleEditor(planId) {
  const plan = getCoverStudioSchedulePlan(planId);
  if (!plan || !elements.coverStudioScheduleEditModal) {
    return;
  }
  appState.coverStudioEditingScheduleId = planId;
  if (elements.coverStudioScheduleEditTitle) {
    elements.coverStudioScheduleEditTitle.textContent = `编辑 ${getCoverStudioSchedulePlanViewName(plan)} 计划`;
  }
  renderCoverStudioScheduleEditor(plan);
  elements.coverStudioScheduleEditModal.hidden = false;
}

function syncCoverStudioScheduleEditorCapabilities() {
  const form = elements.coverStudioScheduleEditForm;
  const templateKey = String(form?.querySelector('[data-schedule-modal-field="templateKey"]')?.value || "fan_spread");
  const { supports, posterLimit } = getCoverStudioModeCapabilities(templateKey);
  const posterCount = form?.querySelector('[data-schedule-modal-field="posterCount"]');
  const posterRotation = form?.querySelector('[data-schedule-modal-field="posterRotation"]');
  if (posterCount) {
    posterCount.max = String(posterLimit);
    posterCount.disabled = !supports.has("posterCount");
    if (Number(posterCount.value) > posterLimit) {
      posterCount.value = String(posterLimit);
    }
  }
  if (posterRotation) {
    posterRotation.disabled = !supports.has("posterRotation");
    if (!supports.has("posterRotation")) {
      posterRotation.value = "0";
    }
  }
}

function renderCoverStudioSchedules() {
  renderCoverStudioScheduleTemplateInputs();
  renderCoverStudioScheduleViewOptions();
  renderCoverStudioSchedulePreview();
  if (!elements.coverStudioScheduleList) {
    return;
  }
  const schedules = Array.isArray(appState.coverStudioConfig?.schedules) ? appState.coverStudioConfig.schedules : [];
  if (!schedules.length) {
    elements.coverStudioScheduleList.innerHTML = '<div class="empty-state">暂无自动封面计划。选择一个媒体库后即可创建。</div>';
    return;
  }
  elements.coverStudioScheduleList.innerHTML = schedules.map((plan) => {
    const resolvedViewName = getCoverStudioSchedulePlanViewName(plan);
    const template = getCoverStudioScheduleTemplate(plan);
    const mode = getCoverStudioModeMeta(template.templateKey);
    const templateLabel = mode?.label || "自定义模板";
    const pickModeLabel = template.pickMode === "recent" ? "最近入库" : "随机模式";
    const statusLabel = plan.enabled ? "已启用" : "已暂停";
    const lastText = plan.lastCheckedAt ? `最近检查：${escapeHtml(plan.lastCheckedAt)}` : "尚未检查";
    const updateText = plan.lastUpdatedAt ? `最近更新：${escapeHtml(plan.lastUpdatedAt)}` : "尚未更新";
    return `<article class="cover-studio-schedule-plan" data-schedule-id="${escapeHtml(plan.id)}">
      <div class="cover-studio-schedule-plan-head">
        <div><strong>${escapeHtml(resolvedViewName)}</strong><small>${lastText}</small></div>
        <span class="cover-studio-schedule-status${plan.enabled ? " is-enabled" : ""}">${statusLabel}</span>
      </div>
      <div class="cover-studio-schedule-plan-meta">
        <span><b>周期</b>${escapeHtml(plan.cron || "*/5 * * * *")}</span>
        <span><b>模板</b>${escapeHtml(templateLabel)}</span>
        <span><b>取图</b>${pickModeLabel}</span>
      </div>
      <p class="cover-studio-schedule-plan-result">${escapeHtml(plan.lastMessage || "尚未检查。")}</p>
      <small class="cover-studio-schedule-plan-update">${updateText}</small>
      <div class="cover-studio-schedule-plan-actions">
        <button class="primary-btn" type="button" data-schedule-action="force">立即更新</button>
        <button class="ghost-btn" type="button" data-schedule-action="edit">编辑</button>
        <button class="ghost-btn" type="button" data-schedule-action="toggle">${plan.enabled ? "停用" : "启用"}</button>
        <button class="text-btn danger" type="button" data-schedule-action="remove">删除计划</button>
      </div>
    </article>`;
  }).join("");
}

async function saveCoverStudioSchedules({ feedback = "封面计划已保存。" } = {}) {
  const next = normalizeCoverStudioConfig(appState.coverStudioConfig);
  try {
    const result = await inviteApiFetch("/api/cover-studio/config", {
      method: "POST",
      body: JSON.stringify({ config: next })
    });
    appState.coverStudioConfig = normalizeCoverStudioConfig(result?.config || next);
    persistLocalState();
    renderCoverStudioSchedules();
    if (feedback) {
      showToast(feedback, 1200);
    }
    return true;
  } catch (error) {
    showToast(`保存失败：${error.message || "未知错误"}`, 1600);
    return false;
  }
}

function updateCoverStudioSchedulePlan(planId, patch) {
  appState.coverStudioConfig = normalizeCoverStudioConfig({
    ...appState.coverStudioConfig,
    schedules: (appState.coverStudioConfig?.schedules || []).map((plan) =>
      plan.id === planId ? { ...plan, ...patch, template: { ...(plan.template || {}), ...(patch.template || {}) } } : plan
    )
  });
}

function updateCoverStudioTitleMode() {
  if (!elements.coverStudioTitleText) {
    return;
  }
  const isBatch = getSelectedCoverStudioViewIds().length > 1;
  elements.coverStudioTitleText.disabled = isBatch;
  elements.coverStudioTitleText.placeholder = isBatch
    ? "批量模式自动使用每个媒体库名称"
    : "例如：国产动漫";
  elements.coverStudioTitleText.title = isBatch
    ? "批量生成时，每个媒体库封面会自动使用自己的媒体库名称。"
    : "可自定义当前媒体库的主标题。";
  const hint = document.getElementById("cover-studio-title-mode-hint");
  if (hint) {
    hint.textContent = isBatch
      ? "已选择多个媒体库：会分别使用各自的媒体库名称作为主标题。"
      : "可自定义当前媒体库的主标题；多选时会自动按媒体库名称生成。";
  }
}

function renderCoverStudioSettings() {
  const draft = normalizeCoverStudioConfig(appState.coverStudioConfig).draft;
  appState.coverStudioConfig = normalizeCoverStudioConfig(appState.coverStudioConfig);
  renderCoverStudioModes();
  renderCoverStudioFonts();
  renderCoverStudioAccentTones();
  renderCoverStudioTitleAlignOptions();
  renderCoverStudioViews();
  renderCoverStudioPresetOptions();
  if (elements.coverStudioPickMode) {
    elements.coverStudioPickMode.value = draft.pickMode || "random";
  }
  if (elements.coverStudioTemplateKey) {
    elements.coverStudioTemplateKey.value = draft.templateKey || "fan_spread";
  }
  if (elements.coverStudioTitleText) {
    elements.coverStudioTitleText.value = draft.titleText || "";
  }
  if (elements.coverStudioSubtitleText) {
    elements.coverStudioSubtitleText.value = draft.subtitleText || "";
  }
  if (elements.coverStudioTitleSize) {
    elements.coverStudioTitleSize.value = String(draft.titleFontSize || 108);
  }
  if (elements.coverStudioSubtitleSize) {
    elements.coverStudioSubtitleSize.value = String(draft.subtitleFontSize || 44);
  }
  renderCoverStudioModeControls();
  updateCoverStudioTitleMode();
  renderCoverStudioSchedules();
  setCoverStudioMode(appState.coverStudioMode);
  renderCoverStudioPreview();
  renderCoverStudioStatus();
}

async function loadCoverStudioConfigFromServer(options = {}) {
  const { silent = false } = options;
  try {
    const result = await inviteApiFetch("/api/cover-studio/config");
    appState.coverStudioConfig = normalizeCoverStudioConfig(result?.config || {});
    appState.coverStudioFonts = Array.isArray(result?.fonts) ? result.fonts : [];
    appState.coverStudioModes = Array.isArray(result?.modes) ? result.modes : [];
    appState.coverStudioAccentTones = Array.isArray(result?.accentTones) ? result.accentTones : [];
    appState.coverStudioTitleAlignOptions = Array.isArray(result?.titleAlignOptions) ? result.titleAlignOptions : [];
    persistLocalState();
    renderCoverStudioSettings();
  } catch (error) {
    if (!silent && elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = `读取封面工坊配置失败：${error.message || "未知错误"}`;
    }
  }
}

async function loadCoverStudioViews(options = {}) {
  const { silent = false } = options;
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    appState.coverStudioViews = [];
    renderCoverStudioSettings();
    return;
  }
  try {
    const result = await inviteApiFetch("/api/cover-studio/views");
    appState.coverStudioViews = Array.isArray(result?.views) ? result.views : [];
    renderCoverStudioSettings();
  } catch (error) {
    appState.coverStudioViews = [];
    renderCoverStudioSettings();
    if (!silent && elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = `读取 Emby 视图失败：${error.message || "未知错误"}`;
    }
  }
}

async function saveCoverStudioConfig({ cloneAsNew = false } = {}) {
  const draft = readCoverStudioDraftFromInputs();
  const next = normalizeCoverStudioConfig(appState.coverStudioConfig);
  let presetId = next.currentPresetId || "default";
  if (cloneAsNew || !next.presets.some((item) => item.id === presetId)) {
    presetId = `preset-${Date.now()}`;
    next.currentPresetId = presetId;
    next.presets = [
      ...next.presets.filter((item) => item.id !== presetId),
      {
        id: presetId,
        name: draft.presetName || "新封面",
        templateKey: draft.templateKey,
        pickMode: draft.pickMode,
        titleText: draft.titleText,
        subtitleText: draft.subtitleText,
        fontKey: draft.fontKey,
        titleFontSize: draft.titleFontSize,
        subtitleFontSize: draft.subtitleFontSize,
        titleAlign: draft.titleAlign,
        overlayStrength: draft.overlayStrength,
        posterCount: draft.posterCount,
        accentTone: draft.accentTone,
        posterRotation: draft.posterRotation,
        titleYOffset: draft.titleYOffset,
        lockedItemIds: draft.lockedItemIds || []
      }
    ];
  } else {
    next.presets = next.presets.map((item) =>
      item.id === presetId
        ? {
            ...item,
            name: draft.presetName || item.name,
            templateKey: draft.templateKey,
            pickMode: draft.pickMode,
            titleText: draft.titleText,
            subtitleText: draft.subtitleText,
            fontKey: draft.fontKey,
            titleFontSize: draft.titleFontSize,
            subtitleFontSize: draft.subtitleFontSize,
            titleAlign: draft.titleAlign,
            overlayStrength: draft.overlayStrength,
            posterCount: draft.posterCount,
            accentTone: draft.accentTone,
            posterRotation: draft.posterRotation,
            titleYOffset: draft.titleYOffset,
            lockedItemIds: draft.lockedItemIds || []
          }
        : item
    );
  }
  next.draft = draft;
  next.lastViewId = draft.viewId;
  try {
    const result = await inviteApiFetch("/api/cover-studio/config", {
      method: "POST",
      body: JSON.stringify({ config: next })
    });
    appState.coverStudioConfig = normalizeCoverStudioConfig(result?.config || next);
    persistLocalState();
    renderCoverStudioSettings();
    if (elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = cloneAsNew ? "封面预设已另存为。" : "封面预设已保存。";
    }
    showToast(cloneAsNew ? "已另存为新预设" : "封面预设已保存", 1200);
  } catch (error) {
    if (elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = `保存封面预设失败：${error.message || "未知错误"}`;
    }
    showToast("保存失败", 1200);
  }
}

async function generateCoverStudioPreview() {
  if (appState.coverStudioLoading) {
    return;
  }
  const draft = readCoverStudioDraftFromInputs();
  if (!draft.viewIds?.length) {
    if (elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = "请先选择目标媒体库视图。";
    }
    showToast("请先选择媒体库", 1000);
    return;
  }
  appState.coverStudioLoading = true;
  if (elements.coverStudioPreviewBtn) {
    elements.coverStudioPreviewBtn.disabled = true;
    elements.coverStudioPreviewBtn.textContent = "生成中...";
  }
  if (elements.coverStudioFeedback) {
    elements.coverStudioFeedback.textContent = "正在从 Emby 读取海报并生成预览…";
  }
  try {
    const result = await inviteApiFetch("/api/cover-studio/preview", {
      method: "POST",
      body: JSON.stringify(draft)
    });
    appState.coverStudioPreviews = Array.isArray(result?.previews) && result.previews.length
      ? result.previews
      : [{
          viewId: draft.viewId,
          previewToken: String(result?.previewToken || "").trim(),
          previewDataUrl: String(result?.previewDataUrl || "").trim(),
          selectedItems: Array.isArray(result?.selectedItems) ? result.selectedItems : []
        }].filter((item) => item.previewToken);
    const primaryPreview = appState.coverStudioPreviews[0] || {};
    appState.coverStudioPreviewToken = String(primaryPreview.previewToken || "").trim();
    appState.coverStudioPreviewDataUrl = String(primaryPreview.previewDataUrl || "").trim();
    appState.coverStudioSelectedItems = Array.isArray(primaryPreview.selectedItems) ? primaryPreview.selectedItems : [];
    appState.coverStudioConfig = normalizeCoverStudioConfig({
      ...appState.coverStudioConfig,
      draft: { ...(appState.coverStudioConfig?.draft || {}), ...draft }
    });
    persistLocalState();
    renderCoverStudioPreview();
    renderCoverStudioStatus();
    if (elements.coverStudioFeedback) {
      const failures = Array.isArray(result?.failures) ? result.failures : [];
      elements.coverStudioFeedback.textContent = failures.length
        ? `已生成 ${appState.coverStudioPreviews.length} 份预览，${failures.length} 个媒体库未成功生成。`
        : `已生成 ${appState.coverStudioPreviews.length} 份预览，可以批量应用到 Emby。`;
    }
  } catch (error) {
    if (elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = `生成预览失败：${error.message || "未知错误"}`;
    }
  } finally {
    appState.coverStudioLoading = false;
    if (elements.coverStudioPreviewBtn) {
      elements.coverStudioPreviewBtn.disabled = false;
      elements.coverStudioPreviewBtn.textContent = "生成预览";
    }
  }
}

async function generateCoverStudioSchedulePreview() {
  if (appState.coverStudioSchedulePreviewLoading) {
    return;
  }
  const draft = readCoverStudioSchedulePreviewDraft();
  if (!draft.viewId) {
    showToast("请先选择媒体库", 1000);
    return;
  }
  appState.coverStudioSchedulePreviewLoading = true;
  if (elements.coverStudioSchedulePreviewBtn) {
    elements.coverStudioSchedulePreviewBtn.disabled = true;
    elements.coverStudioSchedulePreviewBtn.textContent = "生成中...";
  }
  try {
    const result = await inviteApiFetch("/api/cover-studio/preview", {
      method: "POST",
      body: JSON.stringify(draft)
    });
    const preview = Array.isArray(result?.previews) ? result.previews[0] : result;
    appState.coverStudioSchedulePreviewDataUrl = String(preview?.previewDataUrl || result?.previewDataUrl || "").trim();
    renderCoverStudioSchedulePreview();
    showToast("自动封面预览已生成", 1200);
  } catch (error) {
    showToast(`生成预览失败：${error.message || "未知错误"}`, 1600);
  } finally {
    appState.coverStudioSchedulePreviewLoading = false;
    if (elements.coverStudioSchedulePreviewBtn) {
      elements.coverStudioSchedulePreviewBtn.disabled = false;
      elements.coverStudioSchedulePreviewBtn.textContent = "生成预览";
    }
  }
}

async function applyCoverStudioPreview() {
  const previews = Array.isArray(appState.coverStudioPreviews) && appState.coverStudioPreviews.length
    ? appState.coverStudioPreviews
    : appState.coverStudioPreviewToken
      ? [{ viewId: readCoverStudioDraftFromInputs().viewId, previewToken: appState.coverStudioPreviewToken }]
      : [];
  if (!previews.length) {
    showToast("请先生成预览", 1000);
    return;
  }
  const draft = readCoverStudioDraftFromInputs();
  if (!draft.viewIds?.length) {
    showToast("请先选择媒体库", 1000);
    return;
  }
  if (elements.coverStudioApplyBtn) {
    elements.coverStudioApplyBtn.disabled = true;
    elements.coverStudioApplyBtn.textContent = "应用中...";
  }
  try {
    const result = await inviteApiFetch("/api/cover-studio/apply", {
      method: "POST",
      body: JSON.stringify({
        items: previews.map((preview) => ({
          viewId: String(preview?.viewId || "").trim(),
          previewToken: String(preview?.previewToken || "").trim()
        })).filter((item) => item.viewId && item.previewToken)
      })
    });
    await loadCoverStudioConfigFromServer({ silent: true });
    if (elements.coverStudioFeedback) {
      const results = Array.isArray(result?.results) ? result.results : [];
      const failures = Array.isArray(result?.failures) ? result.failures : [];
      elements.coverStudioFeedback.textContent = failures.length
        ? `已更新 ${results.length} 个媒体库封面，${failures.length} 个未成功。`
        : `已批量更新 ${results.length || previews.length} 个媒体库的 Primary 封面。`;
    }
    showToast(`已更新 ${Array.isArray(result?.results) ? result.results.length : previews.length} 个媒体库封面`, 1200);
    await loadCoverStudioViews({ silent: true });
  } catch (error) {
    if (elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = `应用失败：${error.message || "未知错误"}`;
    }
  } finally {
    if (elements.coverStudioApplyBtn) {
      elements.coverStudioApplyBtn.disabled = false;
      elements.coverStudioApplyBtn.textContent = "应用封面";
    }
  }
}

async function restoreCoverStudioBackup() {
  const draft = readCoverStudioDraftFromInputs();
  if (!draft.viewIds?.length) {
    showToast("请先选择媒体库", 1000);
    return;
  }
  if (elements.coverStudioRestoreBtn) {
    elements.coverStudioRestoreBtn.disabled = true;
    elements.coverStudioRestoreBtn.textContent = "恢复中...";
  }
  try {
    const result = await inviteApiFetch("/api/cover-studio/restore", {
      method: "POST",
      body: JSON.stringify({ viewIds: draft.viewIds })
    });
    await loadCoverStudioConfigFromServer({ silent: true });
    if (elements.coverStudioFeedback) {
      const restored = Array.isArray(result?.results) ? result.results.length : 1;
      const failures = Array.isArray(result?.failures) ? result.failures.length : 0;
      elements.coverStudioFeedback.textContent = failures
        ? `已恢复 ${restored} 个媒体库原始封面，${failures} 个未成功。`
        : `已恢复 ${restored} 个媒体库原始封面。`;
    }
    showToast("原封面已恢复", 1200);
    await loadCoverStudioViews({ silent: true });
  } catch (error) {
    if (elements.coverStudioFeedback) {
      elements.coverStudioFeedback.textContent = `恢复失败：${error.message || "未知错误"}`;
    }
  } finally {
    if (elements.coverStudioRestoreBtn) {
      elements.coverStudioRestoreBtn.textContent = "恢复原封面";
    }
    renderCoverStudioStatus();
  }
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
    const originalText = elements.settingsSaveBtn.dataset.originalText || "保存";
    elements.settingsSaveBtn.textContent = originalText;
    elements.settingsSaveBtn.classList.remove("bot-save-success");
    elements.settingsSaveBtn.disabled = false;
  }
}

function isConfigView(view) {
  return view === "media-config" || view === "ai-config";
}

function markSettingsSaveSuccess() {
  const original = elements.settingsSaveBtn?.dataset.originalText || elements.settingsSaveBtn?.textContent || "保存";
  if (elements.settingsSaveBtn) {
    elements.settingsSaveBtn.dataset.originalText = original;
    elements.settingsSaveBtn.textContent = "保存成功";
    elements.settingsSaveBtn.classList.add("bot-save-success");
    elements.settingsSaveBtn.disabled = true;
  }

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

async function saveMediaConfig() {
  if (!elements.settingsSaveBtn || elements.settingsSaveBtn.disabled) {
    return;
  }

  applyConfigFromInputs({ persist: true });
  syncActiveMediaServerFields();
  const activeServerConfig = getMediaServerConfig();
  const hasManaged = (appState?.envControlledFields?.embyConfig || []).length > 0;
  addSyncEvent(
    "媒体库配置已保存",
    hasManaged ? "普通配置已保存，受环境变量接管的字段未被覆盖。" : "服务器地址与 API Key 已写入本地配置。",
    "success"
  );
  showToast(hasManaged ? "媒体库配置已保存（环境变量字段未覆盖）" : "媒体库配置已保存", 1200);
  const synced = await syncInviteStore({
    silentSuccess: true,
    failureToast: "媒体库配置已保存，但服务端同步失败，邀请注册链接可能无法注册。",
    failureEventTitle: "媒体库配置同步失败"
  });
  if (synced) {
    await refreshInviteSyncStatus({ silent: true });
    const backendTmdbToken = String(appState.config.tmdbToken || "").trim();
    if (appState.config.tmdbEnabled && backendTmdbToken) {
      await testTmdbConnection({ silent: true });
    } else {
      refreshTmdbUiState();
    }
  }
  markSettingsSaveSuccess();

  if (activeServerConfig.serverUrl && activeServerConfig.apiKey) {
    appState.config.serverUrl = activeServerConfig.serverUrl;
    appState.config.apiKey = activeServerConfig.apiKey;
    await loadEmbyData();
    await loadCoverStudioViews({ silent: true });
  } else {
    renderConnectionState(false, "配置已保存，请填写媒体服务器地址和 API Key 后再次保存连接。");
  }
}

async function saveAiConfig() {
  if (!elements.settingsSaveBtn || elements.settingsSaveBtn.disabled) {
    return;
  }

  const aiConfig = readAiConfigFromInputs();
  const libraryDirectoryConfig = readLibraryDirectoryConfigFromInputs();
  appState.aiConfig = aiConfig;
  appState.libraryDirectoryConfig = libraryDirectoryConfig;
  persistLocalState();
  addSyncEvent("AI 配置已保存", "AI 服务接入参数与本地目录分类已更新。", "success");
  updateLibraryDirectoryFeedback({ saved: true });

  try {
    await pushAiConfigToServer(aiConfig, { silent: true });
    await loadAiConfigFromServer({ silent: true });
    showToast("AI 配置已保存", 1200);
  } catch (error) {
    if (elements.aiFeedback) {
      elements.aiFeedback.textContent = `AI 配置未保存：${error.message || "未知错误"}`;
    }
    showToast("AI 配置未保存", 1200);
  }

  markSettingsSaveSuccess();
}

async function saveSettingsConfig() {
  if (appState.activeView === "ai-config") {
    await saveAiConfig();
    return;
  }
  await saveMediaConfig();
}

function readAiConfigFromInputs() {
  return normalizeAiConfig({
    enabled: Boolean(elements.aiEnabled?.checked),
    baseUrl: elements.aiBaseUrl?.value.trim() || DEFAULT_AI_CONFIG.baseUrl,
    apiKey: elements.aiApiKey?.value.trim() || "",
    model: elements.aiModel?.value.trim() || DEFAULT_AI_CONFIG.model,
    temperature: elements.aiTemperature?.value || DEFAULT_AI_CONFIG.temperature,
    maxTokens: elements.aiMaxTokens?.value || DEFAULT_AI_CONFIG.maxTokens,
    contextTokensK: elements.aiContextTokensK?.value || DEFAULT_AI_CONFIG.contextTokensK
  });
}

function renderAiSettings() {
  const config = normalizeAiConfig({ ...DEFAULT_AI_CONFIG, ...appState.aiConfig });
  if (elements.aiEnabled) {
    elements.aiEnabled.checked = Boolean(config.enabled);
  }
  if (elements.aiBaseUrl) {
    elements.aiBaseUrl.value = config.baseUrl || DEFAULT_AI_CONFIG.baseUrl;
  }
  if (elements.aiApiKey) {
    elements.aiApiKey.value = config.apiKey || "";
  }
  if (elements.aiModel) {
    elements.aiModel.value = config.model || DEFAULT_AI_CONFIG.model;
  }
  if (elements.aiTemperature) {
    elements.aiTemperature.value = String(config.temperature ?? DEFAULT_AI_CONFIG.temperature);
  }
  if (elements.aiMaxTokens) {
    elements.aiMaxTokens.value = String(config.maxTokens ?? DEFAULT_AI_CONFIG.maxTokens);
  }
  if (elements.aiContextTokensK) {
    elements.aiContextTokensK.value = String(config.contextTokensK ?? DEFAULT_AI_CONFIG.contextTokensK);
  }
  updateAiFeedbackFromInputs({ saved: true });
}

function isSameAiConfig(left, right) {
  const a = normalizeAiConfig(left);
  const b = normalizeAiConfig(right);
  return (
    Boolean(a.enabled) === Boolean(b.enabled) &&
    String(a.baseUrl || "") === String(b.baseUrl || "") &&
    String(a.apiKey || "") === String(b.apiKey || "") &&
    String(a.model || "") === String(b.model || "") &&
    Number(a.temperature) === Number(b.temperature) &&
    Number(a.maxTokens) === Number(b.maxTokens) &&
    Number(a.contextTokensK) === Number(b.contextTokensK)
  );
}

function updateAiFeedbackFromInputs(options = {}) {
  if (!elements.aiFeedback) {
    return;
  }
  const { saved = false, message = "" } = options;
  if (message) {
    elements.aiFeedback.textContent = message;
    return;
  }
  const formConfig = readAiConfigFromInputs();
  if (!formConfig.enabled) {
    elements.aiFeedback.textContent = "AI 未启用，Telegram 不会触发媒体问答。";
    return;
  }
  if (!formConfig.baseUrl || !formConfig.apiKey || !formConfig.model) {
    elements.aiFeedback.textContent = "AI 已开启，请补齐 Base URL、API Key 和模型名称后保存。";
    return;
  }
  if (saved || isSameAiConfig(formConfig, appState.aiConfig)) {
    elements.aiFeedback.textContent = "AI 已启用，Telegram 私聊可直接提问，群聊使用 /ai。";
    return;
  }
  elements.aiFeedback.textContent = "AI 已填写，点击保存后 Telegram 生效。";
}

async function loadAiConfigFromServer(options = {}) {
  const { silent = false } = options;
  try {
    const result = await inviteApiFetch("/api/ai/config");
    appState.aiConfig = normalizeAiConfig({ ...DEFAULT_AI_CONFIG, ...(result?.aiConfig || {}) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "aiConfig");
    persistLocalState();
    renderAiSettings();
    renderEnvControlledState();
    return true;
  } catch (error) {
    if (!silent && elements.aiFeedback) {
      elements.aiFeedback.textContent = `读取 AI 配置失败：${error.message || "未知错误"}`;
    }
    return false;
  }
}

async function pushAiConfigToServer(nextConfig, options = {}) {
  const { silent = false } = options;
  const payloadConfig = { ...nextConfig };
  const aiManaged = appState?.envControlledFields?.aiConfig || [];
  if (aiManaged.includes("baseUrl")) {
    delete payloadConfig.baseUrl;
  }
  if (aiManaged.includes("apiKey")) {
    delete payloadConfig.apiKey;
  }
  if (aiManaged.includes("model")) {
    delete payloadConfig.model;
  }
  try {
    const result = await inviteApiFetch("/api/ai/config", {
      method: "POST",
      body: JSON.stringify({ aiConfig: payloadConfig })
    });
    appState.aiConfig = normalizeAiConfig({ ...DEFAULT_AI_CONFIG, ...(result?.aiConfig || nextConfig) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "aiConfig");
    persistLocalState();
    renderAiSettings();
    renderEnvControlledState();
    return appState.aiConfig;
  } catch (error) {
    if (!silent && elements.aiFeedback) {
      elements.aiFeedback.textContent = `保存 AI 配置失败：${error.message || "未知错误"}`;
    }
    throw error;
  }
}

function readDrive115ConfigFromInputs() {
  return normalizeDrive115Config({
    enabled: Boolean(elements.drive115Enabled?.checked),
    cookie: elements.drive115Cookie?.value.trim() || "",
    defaultCid: elements.drive115DefaultCid?.value.trim() || DEFAULT_DRIVE115_CONFIG.defaultCid
  });
}

function renderDrive115Config() {
  const config = normalizeDrive115Config({ ...DEFAULT_DRIVE115_CONFIG, ...appState.drive115Config });
  if (elements.drive115Enabled) {
    elements.drive115Enabled.checked = Boolean(config.enabled);
  }
  if (elements.drive115Cookie) {
    elements.drive115Cookie.value = config.cookie || "";
    elements.drive115Cookie.type = "password";
    elements.drive115Cookie.placeholder = config.hasCookie ? "已保存 Cookie" : "粘贴 115 网页 Cookie";
  }
  const drive115CookieToggle = document.getElementById("drive115-cookie-toggle");
  if (drive115CookieToggle) {
    drive115CookieToggle.dataset.open = "false";
    drive115CookieToggle.classList.remove("is-open");
    drive115CookieToggle.innerHTML = EYE_CLOSED_SVG;
  }
  if (elements.drive115DefaultCid) {
    elements.drive115DefaultCid.value = config.defaultCid || "0";
  }
  updateDrive115Feedback();
  updateDrive115Completion();
}

function updateDrive115Feedback(message = "") {
  if (!elements.drive115ConfigFeedback) {
    return;
  }
  if (message) {
    elements.drive115ConfigFeedback.textContent = message;
    return;
  }
  const config = normalizeDrive115Config(appState.drive115Config);
  if (!config.enabled) {
    elements.drive115ConfigFeedback.textContent = "115 未启用，Telegram 不会处理 115 链接。";
    return;
  }
  if (!config.hasCookie && !String(elements.drive115Cookie?.value || "").trim()) {
    elements.drive115ConfigFeedback.textContent = "115 已开启，请填写 Cookie 后保存。";
    return;
  }
  elements.drive115ConfigFeedback.textContent = "115 已启用，可解析链接并确认转存。";
}

function updateDrive115Completion() {
  // 115 页面已移除顶部完整度徽标，保留函数避免输入监听分支扩散。
}

function setDrive115QrStatus(message, state = "") {
  if (elements.drive115QrStatus) {
    elements.drive115QrStatus.textContent = message || "";
    elements.drive115QrStatus.dataset.state = state;
  }
}

function stopDrive115QrPolling(options = {}) {
  const { clearSession = false, notifyServer = false } = options;
  if (appState.drive115QrTimer) {
    clearTimeout(appState.drive115QrTimer);
    appState.drive115QrTimer = null;
  }
  appState.drive115QrPolling = false;
  appState.drive115QrFailureCount = 0;
  if (notifyServer && appState.drive115QrSessionId) {
    inviteApiFetch("/api/drive115/qrcode/stop", {
      method: "POST",
      body: JSON.stringify({ sessionId: appState.drive115QrSessionId })
    }).catch(() => {
      // 停止轮询失败不影响页面状态。
    });
  }
  if (clearSession) {
    appState.drive115QrSessionId = "";
    appState.drive115QrStartedAt = 0;
  }
  if (elements.drive115QrStopBtn) {
    elements.drive115QrStopBtn.disabled = true;
  }
  if (elements.drive115QrStartBtn) {
    elements.drive115QrStartBtn.disabled = false;
    elements.drive115QrStartBtn.textContent = "生成二维码";
  }
}

function scheduleDrive115QrPoll(delay = 2000) {
  if (!appState.drive115QrSessionId || appState.drive115QrTimer) return;
  appState.drive115QrTimer = setTimeout(() => {
    appState.drive115QrTimer = null;
    pollDrive115QrStatus();
  }, Math.max(0, Number(delay) || 0));
}

async function pollDrive115QrStatus() {
  const sessionId = String(appState.drive115QrSessionId || "").trim();
  if (!sessionId) {
    stopDrive115QrPolling({ clearSession: true });
    return;
  }
  if (Date.now() - Number(appState.drive115QrStartedAt || 0) > 180000) {
    setDrive115QrStatus("二维码已过期，请重新生成。", "expired");
    stopDrive115QrPolling({ clearSession: true, notifyServer: true });
    return;
  }
  if (appState.drive115QrPolling) return;
  appState.drive115QrPolling = true;
  let continuePolling = true;
  try {
    const result = await inviteApiFetch(`/api/drive115/qrcode/status?sessionId=${encodeURIComponent(sessionId)}`);
    appState.drive115QrFailureCount = 0;
    const status = String(result?.status || "").trim();
    if (status === "waiting") {
      setDrive115QrStatus(result?.message || "等待扫码。", "waiting");
      return;
    }
    if (status === "scanned") {
      setDrive115QrStatus(result?.message || "已扫码，请在 115 App 中确认登录。", "scanned");
      return;
    }
    if (status === "success") {
      appState.drive115Config = normalizeDrive115Config({ ...DEFAULT_DRIVE115_CONFIG, ...(result?.drive115Config || {}) });
      persistLocalState();
      renderDrive115Page();
      setDrive115QrStatus(result?.message || "扫码登录成功，Cookie 已自动保存。", "success");
      addDrive115Record("扫码登录成功", "Cookie 已自动保存。", "success");
      continuePolling = false;
      stopDrive115QrPolling({ clearSession: true });
      showToast("115 Cookie 已自动保存", 1400);
      return;
    }
    setDrive115QrStatus(result?.message || "二维码状态未知。", status || "waiting");
  } catch (error) {
    const message = error.message || "二维码状态查询失败。";
    const expired = message.includes("过期") || message.includes("会话不存在");
    appState.drive115QrFailureCount = Number(appState.drive115QrFailureCount || 0) + 1;
    if (expired || appState.drive115QrFailureCount >= 5) {
      continuePolling = false;
      setDrive115QrStatus(expired ? message : `115 状态接口连续失败 ${appState.drive115QrFailureCount} 次，请重新生成二维码。`, expired ? "expired" : "failed");
      stopDrive115QrPolling({ clearSession: true });
    } else {
      setDrive115QrStatus(`115 状态接口暂时无响应，正在重试（${appState.drive115QrFailureCount}/5）。`, "waiting");
    }
  } finally {
    appState.drive115QrPolling = false;
    if (continuePolling && String(appState.drive115QrSessionId || "") === sessionId) {
      scheduleDrive115QrPoll(2000);
    }
  }
}

async function startDrive115QrLogin() {
  if (appState.drive115QrSessionId) {
    stopDrive115QrPolling({ clearSession: true, notifyServer: true });
  }
  if (elements.drive115QrStartBtn) {
    elements.drive115QrStartBtn.disabled = true;
    elements.drive115QrStartBtn.textContent = "生成中...";
  }
  try {
    const result = await inviteApiFetch("/api/drive115/qrcode/start", {
      method: "POST",
      body: JSON.stringify({ client: elements.drive115QrClient?.value || "qandroid" })
    });
    appState.drive115QrSessionId = String(result?.sessionId || "");
    appState.drive115QrStartedAt = Date.now();
    appState.drive115QrFailureCount = 0;
    if (elements.drive115QrImage && result?.imageUrl) {
      elements.drive115QrImage.src = result.imageUrl;
      elements.drive115QrImage.hidden = false;
    }
    if (elements.drive115QrPlaceholder) {
      elements.drive115QrPlaceholder.hidden = true;
    }
    if (elements.drive115QrStopBtn) {
      elements.drive115QrStopBtn.disabled = false;
    }
    if (elements.drive115QrStartBtn) {
      elements.drive115QrStartBtn.textContent = "重新生成";
      elements.drive115QrStartBtn.disabled = false;
    }
    setDrive115QrStatus(result?.message || "二维码已生成，请扫码。", "waiting");
    stopDrive115QrPolling({ clearSession: false });
    if (elements.drive115QrStopBtn) {
      elements.drive115QrStopBtn.disabled = false;
    }
    scheduleDrive115QrPoll(800);
  } catch (error) {
    setDrive115QrStatus(`生成二维码失败：${error.message || "未知错误"}`, "failed");
    stopDrive115QrPolling({ clearSession: true });
  }
}

function applyDrive115SearchFilter() {
  // 115 页面已移除配置搜索框。
}

function addDrive115Record(title, detail, level = "info") {
  const record = {
    title: String(title || "115 操作"),
    detail: String(detail || ""),
    level,
    time: new Date().toLocaleString("zh-CN", { hour12: false })
  };
  appState.drive115Records = [record, ...(appState.drive115Records || [])].slice(0, 8);
  renderDrive115Records();
}

function renderDrive115Records() {
  if (!elements.drive115Records) {
    return;
  }
  const rows = Array.isArray(appState.drive115Records) ? appState.drive115Records : [];
  if (!rows.length) {
    elements.drive115Records.innerHTML = `<div class="empty-state">暂无 115 转存记录。</div>`;
    return;
  }
  elements.drive115Records.innerHTML = rows
    .map((row) => {
      const badge = row.level === "error" ? "失败" : row.level === "success" ? "成功" : "记录";
      return `
        <div class="drive115-record">
          <strong>${escapeHtml(row.title)} · ${escapeHtml(badge)}</strong>
          <span>${escapeHtml(row.time)}</span>
          <div>${escapeHtml(row.detail)}</div>
        </div>
      `;
    })
    .join("");
}

function renderDrive115ParseResult(payload = undefined) {
  if (!elements.drive115ParseResult) {
    return;
  }
  const result = payload === undefined ? appState.drive115LastParse : payload;
  if (!result) {
    elements.drive115ParseResult.textContent = "尚未解析 115 分享链接。";
    if (elements.drive115TransferBtn) {
      elements.drive115TransferBtn.disabled = true;
    }
    return;
  }
  const files = Array.isArray(result.files) ? result.files.slice(0, 8) : [];
  const lines = [
    `资源：${result.title || "115 分享资源"}`,
    `文件数：${result.fileCount ?? files.length}`,
    `大小：${result.totalSizeText || "-"}`,
    `分享码：${result.shareCode || "-"}`
  ];
  if (files.length) {
    lines.push("", "前几项：");
    files.forEach((file, index) => {
      lines.push(`${index + 1}. ${file.name || "未命名"}${file.sizeText ? `｜${file.sizeText}` : ""}`);
    });
  }
  elements.drive115ParseResult.textContent = lines.join("\n");
  if (elements.drive115TransferBtn) {
    elements.drive115TransferBtn.disabled = !result.shareCode;
  }
}

function renderDrive115Page() {
  renderDrive115Config();
  renderDrive115ParseResult();
  renderDrive115Records();
}

async function loadDrive115ConfigFromServer(options = {}) {
  const { silent = false } = options;
  try {
    const result = await inviteApiFetch("/api/drive115/config");
    appState.drive115Config = normalizeDrive115Config({ ...DEFAULT_DRIVE115_CONFIG, ...(result?.drive115Config || {}) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "drive115Config");
    persistLocalState();
    renderDrive115Page();
    renderEnvControlledState();
    return true;
  } catch (error) {
    if (!silent) {
      updateDrive115Feedback(`读取 115 配置失败：${error.message || "未知错误"}`);
    }
    return false;
  }
}

async function saveDrive115Config() {
  const nextConfig = readDrive115ConfigFromInputs();
  if (elements.drive115SaveBtn) {
    elements.drive115SaveBtn.disabled = true;
    elements.drive115SaveBtn.textContent = "保存中...";
  }
  try {
    const result = await inviteApiFetch("/api/drive115/config", {
      method: "POST",
      body: JSON.stringify({ drive115Config: nextConfig })
    });
    appState.drive115Config = normalizeDrive115Config({ ...DEFAULT_DRIVE115_CONFIG, ...(result?.drive115Config || {}) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "drive115Config");
    persistLocalState();
    renderDrive115Page();
    updateDrive115Feedback("115 配置已保存。");
    addDrive115Record("保存配置", "115 配置已保存。", "success");
    showToast("115 配置已保存", 1200);
  } catch (error) {
    updateDrive115Feedback(`115 配置保存失败：${error.message || "未知错误"}`);
    addDrive115Record("保存配置失败", error.message || "未知错误", "error");
  } finally {
    if (elements.drive115SaveBtn) {
      elements.drive115SaveBtn.disabled = false;
      elements.drive115SaveBtn.textContent = "保存";
    }
  }
}

async function testDrive115Config() {
  if (elements.drive115TestBtn) {
    elements.drive115TestBtn.disabled = true;
    elements.drive115TestBtn.textContent = "测试中...";
  }
  try {
    const result = await inviteApiFetch("/api/drive115/test", {
      method: "POST",
      body: JSON.stringify({ drive115Config: readDrive115ConfigFromInputs() })
    });
    updateDrive115Feedback(result?.message || "115 Cookie 测试成功。");
    addDrive115Record("测试连接", result?.message || "115 Cookie 测试成功。", "success");
  } catch (error) {
    updateDrive115Feedback(`115 测试失败：${error.message || "未知错误"}`);
    addDrive115Record("测试连接失败", error.message || "未知错误", "error");
  } finally {
    if (elements.drive115TestBtn) {
      elements.drive115TestBtn.disabled = false;
      elements.drive115TestBtn.textContent = "测试连接";
    }
  }
}

async function parseDrive115Link() {
  const shareUrl = String(elements.drive115ShareUrl?.value || "").trim();
  const receiveCode = String(elements.drive115ReceiveCode?.value || "").trim();
  if (!shareUrl) {
    updateDrive115Feedback("请先粘贴 115 分享链接。");
    return;
  }
  if (elements.drive115ParseBtn) {
    elements.drive115ParseBtn.disabled = true;
    elements.drive115ParseBtn.textContent = "解析中...";
  }
  try {
    const result = await inviteApiFetch("/api/drive115/parse", {
      method: "POST",
      body: JSON.stringify({ shareUrl, receiveCode })
    });
    appState.drive115LastParse = result;
    renderDrive115ParseResult(result);
    addDrive115Record("解析成功", `${result.title || "115 分享资源"}｜${result.fileCount ?? 0} 项`, "success");
  } catch (error) {
    appState.drive115LastParse = null;
    renderDrive115ParseResult(null);
    if (elements.drive115ParseResult) {
      elements.drive115ParseResult.textContent = `解析失败：${error.message || "未知错误"}`;
    }
    addDrive115Record("解析失败", error.message || "未知错误", "error");
  } finally {
    if (elements.drive115ParseBtn) {
      elements.drive115ParseBtn.disabled = false;
      elements.drive115ParseBtn.textContent = "解析资源";
    }
  }
}

async function transferDrive115Link() {
  const parsed = appState.drive115LastParse;
  if (!parsed?.shareCode) {
    updateDrive115Feedback("请先解析 115 分享链接。");
    return;
  }
  const targetCid = String(elements.drive115TargetCid?.value || appState.drive115Config?.defaultCid || "0").trim() || "0";
  if (elements.drive115TransferBtn) {
    elements.drive115TransferBtn.disabled = true;
    elements.drive115TransferBtn.textContent = "提交中...";
  }
  try {
    const result = await inviteApiFetch("/api/drive115/transfer", {
      method: "POST",
      body: JSON.stringify({
        shareCode: parsed.shareCode,
        receiveCode: parsed.receiveCode || String(elements.drive115ReceiveCode?.value || "").trim(),
        targetCid,
        fileIds: Array.isArray(parsed.files) ? parsed.files.map((file) => String(file?.id || "").trim()).filter(Boolean) : [],
        sourceFiles: Array.isArray(parsed.files) ? parsed.files.map((file) => ({
          name: String(file?.name || ""),
          size: Number(file?.size || 0),
          isDir: Boolean(file?.isDir)
        })) : []
      })
    });
    const message = result?.message || "115 已收到转存请求。";
    updateDrive115Feedback(message);
    const existed = String(result?.status || "") === "exists";
    addDrive115Record(existed ? "资源已存在" : "转存已提交", `${parsed.title || parsed.shareCode}｜目录 ${targetCid}`, existed ? "warning" : "success");
    showToast(existed ? "目标目录已存在该资源" : "115 转存已提交", 1200);
  } catch (error) {
    updateDrive115Feedback(`115 转存失败：${error.message || "未知错误"}`);
    addDrive115Record("转存失败", error.message || "未知错误", "error");
    if (elements.drive115TransferBtn) {
      elements.drive115TransferBtn.disabled = false;
    }
  } finally {
    if (elements.drive115TransferBtn) {
      elements.drive115TransferBtn.textContent = "确认转存";
    }
  }
}

let hdhiveOAuthPollTimer = null;

function renderHDHiveConfig() {
  const config = normalizeHDHiveConfig(appState.hdhiveConfig);
  if (elements.hdhiveEnabled) elements.hdhiveEnabled.checked = config.enabled;
  if (elements.hdhiveAuthMode) elements.hdhiveAuthMode.value = config.authMode;
  if (elements.hdhiveBrokerUrl) elements.hdhiveBrokerUrl.value = config.brokerUrl;
  if (elements.hdhiveBrokerField) elements.hdhiveBrokerField.hidden = config.authMode !== "broker";
  if (elements.hdhiveDirectSettings) {
    elements.hdhiveDirectSettings.hidden = config.authMode !== "direct";
    if (config.authMode === "direct") elements.hdhiveDirectSettings.open = true;
  }
  if (elements.hdhiveClientId) elements.hdhiveClientId.value = config.clientId;
  if (elements.hdhiveAppSecret) {
    elements.hdhiveAppSecret.value = "";
    elements.hdhiveAppSecret.placeholder = config.hasAppSecret ? `已保存 ${config.appSecretMasked || "应用 Secret"}` : "输入应用 Secret";
  }
  if (elements.hdhiveRedirectUri) elements.hdhiveRedirectUri.value = config.callbackUri || config.redirectUri;
  if (elements.hdhiveAutoCheckin) elements.hdhiveAutoCheckin.checked = config.autoCheckin;
  if (elements.hdhiveTimezone) elements.hdhiveTimezone.value = config.timezone;
  if (elements.hdhiveStatusBadge) {
    const state = config.authorized ? "authorized" : config.hasAppSecret && config.clientId ? "configured" : "off";
    elements.hdhiveStatusBadge.className = `tmdb-status-badge ${state === "authorized" ? "is-on" : state === "configured" ? "is-pending" : "is-off"}`;
    elements.hdhiveStatusBadge.textContent = state === "authorized" ? "已授权" : state === "configured" ? "待授权" : "未配置";
  }
  if (elements.hdhiveAccountSummary) {
    const user = config.user || {};
    elements.hdhiveAccountSummary.innerHTML = config.authorized
      ? `<strong>${escapeHtml(user.username || "影巢用户")}</strong><span>等级：${escapeHtml(user.level || "-")} · 积分：${escapeHtml(user.points ?? "-")} · 权限：${escapeHtml(config.scopes)}</span>`
      : `<strong>尚未授权</strong><span>${config.authMode === "broker" ? (config.brokerUrl ? "授权服务已配置，请点击浏览器授权。" : "请先填写 VistaMirror 授权服务地址。") : (config.clientId && config.hasAppSecret ? "独立应用已保存，请完成 OAuth 授权。" : "请先填写独立应用凭据。")}</span>`;
  }
  if (elements.hdhiveAuthorizeBtn) {
    elements.hdhiveAuthorizeBtn.disabled = config.authMode === "broker" ? !config.brokerUrl : !(config.clientId && config.hasAppSecret);
    elements.hdhiveAuthorizeBtn.textContent = config.oauthSessionId ? "等待浏览器确认" : "前往影巢授权";
  }
  if (elements.hdhiveDisconnectBtn) elements.hdhiveDisconnectBtn.disabled = !config.authorized;
  if (elements.hdhiveCheckinBtn) elements.hdhiveCheckinBtn.disabled = !config.authorized || !config.scopes.split(/\s+/).includes("write");
  if (elements.hdhiveCheckinFeedback) {
    const checkin = config.lastCheckin || {};
    elements.hdhiveCheckinFeedback.textContent = checkin.message
      ? `${checkin.message}${checkin.points ? ` · 积分 ${checkin.points > 0 ? "+" : ""}${checkin.points}` : ""}`
      : config.authorized && !config.scopes.split(/\s+/).includes("write")
        ? "当前授权缺少 write 权限，请重新授权后使用签到。"
        : "尚无签到记录。";
  }
  if (elements.hdhiveConfigFeedback) {
    elements.hdhiveConfigFeedback.textContent = config.authorized
      ? "影巢已授权，可搜索资源并在确认后解锁转存。"
      : config.authMode === "broker"
        ? (config.brokerUrl ? "一键授权服务已保存，请完成浏览器授权。" : "请填写并保存 VistaMirror 授权服务地址。")
        : (config.clientId && config.hasAppSecret ? "独立 OpenAPI 应用已保存，请完成影巢账号授权。" : "请填写并保存独立应用配置。")
  }
}

function addHDHiveRecord(title, detail, state = "success") {
  appState.hdhiveRecords = [{ title, detail, state, at: new Date().toLocaleString("zh-CN") }, ...(appState.hdhiveRecords || [])].slice(0, 8);
  renderHDHiveRecords();
}

function renderHDHiveRecords() {
  if (!elements.hdhiveRecords) return;
  const rows = Array.isArray(appState.hdhiveRecords) ? appState.hdhiveRecords : [];
  elements.hdhiveRecords.innerHTML = rows.length
    ? rows.map((row) => `<div class="drive115-record"><strong>${escapeHtml(row.title)}</strong><span>${escapeHtml(row.detail)}</span><span>${escapeHtml(row.at)}</span></div>`).join("")
    : `<div class="empty-state">暂无影巢操作记录。</div>`;
}

function renderHDHiveResults() {
  if (!elements.hdhiveSearchResults) return;
  const only115 = Boolean(elements.hdhiveOnly115?.checked);
  const rows = (Array.isArray(appState.hdhiveResources) ? appState.hdhiveResources : []).filter((row) => !only115 || row.is115);
  if (!rows.length) {
    elements.hdhiveSearchResults.innerHTML = `<div class="empty-state">${appState.hdhiveResources?.length ? "当前筛选条件下没有 115 资源。" : "没有找到影巢资源。"}</div>`;
    return;
  }
  elements.hdhiveSearchResults.innerHTML = rows.map((row) => {
    const resolution = Array.isArray(row.resolution) ? row.resolution.join(" / ") : String(row.resolution || "");
    const source = Array.isArray(row.source) ? row.source.join(" / ") : String(row.source || "");
    const cost = row.isUnlocked ? "已解锁" : `${Number(row.unlockPoints || 0)} 积分`;
    return `<article class="hdhive-resource-card">
      <h5>${escapeHtml(row.title || "影巢资源")}</h5>
      <div class="hdhive-resource-meta">
        <span>${escapeHtml(row.panType || "未知网盘")}</span>
        ${row.shareSize ? `<span>${escapeHtml(row.shareSize)}</span>` : ""}
        ${resolution ? `<span>${escapeHtml(resolution)}</span>` : ""}
        ${source ? `<span>${escapeHtml(source)}</span>` : ""}
      </div>
      <p>发布者：${escapeHtml(row.publisher || "-")}</p>
      <div class="hdhive-resource-actions">
        <strong>${escapeHtml(cost)}</strong>
        <button class="primary-btn" type="button" data-hdhive-transfer="${escapeHtml(row.slug)}" ${row.is115 ? "" : "disabled"}>${row.is115 ? "解锁并转存" : "非 115 资源"}</button>
      </div>
    </article>`;
  }).join("");
}

async function loadHDHiveConfigFromServer(options = {}) {
  const { silent = false } = options;
  try {
    const result = await inviteApiFetch("/api/hdhive/config");
    appState.hdhiveConfig = normalizeHDHiveConfig({ ...DEFAULT_HDHIVE_CONFIG, ...(result?.hdhiveConfig || {}) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "hdhiveConfig");
    persistLocalState();
    renderHDHiveConfig();
    renderEnvControlledState();
    if (appState.hdhiveConfig.authMode === "broker" && appState.hdhiveConfig.oauthSessionId && !hdhiveOAuthPollTimer) {
      startHDHiveOAuthPolling(appState.hdhiveConfig.oauthSessionId, appState.hdhiveConfig.oauthSessionExpiresAt);
    }
    return true;
  } catch (error) {
    if (!silent && elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = `读取影巢配置失败：${error.message || "未知错误"}`;
    return false;
  }
}

async function saveHDHiveConfig() {
  const payload = {
    enabled: Boolean(elements.hdhiveEnabled?.checked),
    authMode: String(elements.hdhiveAuthMode?.value || "broker"),
    brokerUrl: String(elements.hdhiveBrokerUrl?.value || "").trim(),
    autoCheckin: Boolean(elements.hdhiveAutoCheckin?.checked),
    timezone: String(elements.hdhiveTimezone?.value || "Asia/Shanghai"),
    clientId: String(elements.hdhiveClientId?.value || "").trim(),
    appSecret: String(elements.hdhiveAppSecret?.value || "").trim(),
    redirectUri: String(elements.hdhiveRedirectUri?.value || "").trim()
  };
  if (elements.hdhiveSaveBtn) { elements.hdhiveSaveBtn.disabled = true; elements.hdhiveSaveBtn.textContent = "保存中..."; }
  try {
    const result = await inviteApiFetch("/api/hdhive/config", { method: "POST", body: JSON.stringify({ hdhiveConfig: payload }) });
    appState.hdhiveConfig = normalizeHDHiveConfig({ ...DEFAULT_HDHIVE_CONFIG, ...(result?.hdhiveConfig || {}) });
    appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "hdhiveConfig");
    persistLocalState();
    renderHDHiveConfig();
    addHDHiveRecord("配置已保存", payload.authMode === "broker" ? "一键授权代理配置已更新。" : "独立 OpenAPI 应用配置已更新。", "success");
    showToast("影巢配置已保存", 1200);
  } catch (error) {
    if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = `保存失败：${error.message || "未知错误"}`;
    addHDHiveRecord("配置保存失败", error.message || "未知错误", "error");
  } finally {
    if (elements.hdhiveSaveBtn) { elements.hdhiveSaveBtn.disabled = false; elements.hdhiveSaveBtn.textContent = "保存"; }
  }
}

async function authorizeHDHive() {
  try {
    const result = await inviteApiFetch("/api/hdhive/oauth/start");
    if (!result?.authorizeUrl) throw new Error("未取得影巢授权地址");
    if (result.mode === "broker") {
      window.open(result.authorizeUrl, "_blank", "noopener,noreferrer");
      appState.hdhiveConfig = normalizeHDHiveConfig({ ...appState.hdhiveConfig, oauthSessionId: result.sessionId, oauthSessionExpiresAt: result.expiresAt });
      renderHDHiveConfig();
      startHDHiveOAuthPolling(result.sessionId, result.expiresAt);
    } else {
      window.location.assign(result.authorizeUrl);
    }
  } catch (error) {
    if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = `无法发起授权：${error.message || "未知错误"}`;
  }
}

function stopHDHiveOAuthPolling() {
  if (hdhiveOAuthPollTimer) window.clearInterval(hdhiveOAuthPollTimer);
  hdhiveOAuthPollTimer = null;
}

function startHDHiveOAuthPolling(sessionId, expiresAt = 0) {
  stopHDHiveOAuthPolling();
  const poll = async () => {
    try {
      const result = await inviteApiFetch(`/api/hdhive/oauth/status?sessionId=${encodeURIComponent(sessionId || "")}`);
      if (result?.status === "authorized") {
        stopHDHiveOAuthPolling();
        await loadHDHiveConfigFromServer({ silent: true });
        addHDHiveRecord("授权成功", "影巢账号已通过浏览器授权。", "success");
        showToast("影巢授权成功", 1600);
      } else if (["failed", "expired"].includes(String(result?.status || "")) || (expiresAt && Date.now() / 1000 > Number(expiresAt))) {
        stopHDHiveOAuthPolling();
        if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = result?.error || "影巢授权会话已过期，请重新授权。";
      }
    } catch (error) {
      if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = `授权状态读取失败：${error.message || "未知错误"}`;
    }
  };
  poll();
  hdhiveOAuthPollTimer = window.setInterval(poll, 2000);
}

async function refreshHDHiveStatus() {
  const config = normalizeHDHiveConfig(appState.hdhiveConfig);
  if (config.authMode === "broker" && config.oauthSessionId) {
    startHDHiveOAuthPolling(config.oauthSessionId, config.oauthSessionExpiresAt);
    return;
  }
  await testHDHive();
}

async function checkinHDHive() {
  if (elements.hdhiveCheckinBtn) { elements.hdhiveCheckinBtn.disabled = true; elements.hdhiveCheckinBtn.textContent = "签到中..."; }
  try {
    const result = await inviteApiFetch("/api/hdhive/checkin", { method: "POST", body: "{}" });
    const message = result?.result?.message || "签到请求已完成。";
    await loadHDHiveConfigFromServer({ silent: true });
    addHDHiveRecord("影巢签到", message, "success");
    showToast(message, 1800);
  } catch (error) {
    if (elements.hdhiveCheckinFeedback) elements.hdhiveCheckinFeedback.textContent = `签到失败：${error.message || "未知错误"}`;
  } finally {
    if (elements.hdhiveCheckinBtn) { elements.hdhiveCheckinBtn.textContent = "立即签到"; renderHDHiveConfig(); }
  }
}

async function testHDHive() {
  if (elements.hdhiveTestBtn) { elements.hdhiveTestBtn.disabled = true; elements.hdhiveTestBtn.textContent = "测试中..."; }
  try {
    const result = await inviteApiFetch("/api/hdhive/test", { method: "POST", body: "{}" });
    if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = result?.user?.username
      ? `连接正常，当前用户：${result.user.username}`
      : appState.hdhiveConfig.authMode === "broker" ? "授权代理连接正常，账号尚未授权。" : "应用 Secret 验证通过，账号尚未授权。";
    await loadHDHiveConfigFromServer({ silent: true });
  } catch (error) {
    if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = `连接测试失败：${error.message || "未知错误"}`;
  } finally {
    if (elements.hdhiveTestBtn) { elements.hdhiveTestBtn.disabled = false; elements.hdhiveTestBtn.textContent = "测试连接"; }
  }
}

async function disconnectHDHive() {
  if (!window.confirm("确定解除当前影巢账号授权吗？应用配置会保留。")) return;
  try {
    await inviteApiFetch("/api/hdhive/oauth/disconnect", { method: "POST", body: "{}" });
    await loadHDHiveConfigFromServer();
    stopHDHiveOAuthPolling();
    addHDHiveRecord("授权已解除", "当前影巢账号授权已撤销。", "success");
  } catch (error) {
    if (elements.hdhiveConfigFeedback) elements.hdhiveConfigFeedback.textContent = `解除授权失败：${error.message || "未知错误"}`;
  }
}

async function searchHDHive() {
  const query = String(elements.hdhiveSearchKeyword?.value || "").trim();
  if (!query) {
    if (elements.hdhiveSearchSummary) elements.hdhiveSearchSummary.textContent = "请输入电影或剧集名称。";
    return;
  }
  if (elements.hdhiveSearchBtn) { elements.hdhiveSearchBtn.disabled = true; elements.hdhiveSearchBtn.textContent = "搜索中..."; }
  if (elements.hdhiveSearchSummary) elements.hdhiveSearchSummary.textContent = "正在确认 TMDB 身份并查询影巢资源...";
  try {
    const result = await inviteApiFetch("/api/hdhive/search", {
      method: "POST",
      body: JSON.stringify({ query, mediaType: elements.hdhiveSearchType?.value || "" })
    });
    appState.hdhiveIdentity = result?.identity || null;
    appState.hdhiveResources = Array.isArray(result?.resources) ? result.resources : [];
    if (result?.user) appState.hdhiveConfig.user = result.user;
    const identity = appState.hdhiveIdentity || {};
    if (elements.hdhiveSearchSummary) elements.hdhiveSearchSummary.textContent = `《${identity.title || query}》${identity.year ? `（${identity.year}）` : ""} · TMDB ${identity.tmdbId || "-"} · 找到 ${appState.hdhiveResources.length} 条资源。`;
    renderHDHiveResults();
    renderHDHiveConfig();
    addHDHiveRecord("搜索完成", `《${identity.title || query}》找到 ${appState.hdhiveResources.length} 条资源。`, "success");
  } catch (error) {
    appState.hdhiveResources = [];
    renderHDHiveResults();
    if (elements.hdhiveSearchSummary) elements.hdhiveSearchSummary.textContent = `搜索失败：${error.message || "未知错误"}`;
    addHDHiveRecord("搜索失败", error.message || "未知错误", "error");
  } finally {
    if (elements.hdhiveSearchBtn) { elements.hdhiveSearchBtn.disabled = false; elements.hdhiveSearchBtn.textContent = "搜索资源"; }
  }
}

async function transferHDHiveResource(slug) {
  const resource = (appState.hdhiveResources || []).find((row) => row.slug === slug);
  if (!resource || !resource.is115) return;
  const targetCid = String(appState.drive115Config?.defaultCid || "0").trim() || "0";
  const cost = resource.isUnlocked ? "该资源已解锁，不会重复扣积分" : `预计消耗 ${Number(resource.unlockPoints || 0)} 积分`;
  if (!window.confirm(`${resource.title || "影巢资源"}\n${cost}\n转存到 115 目录：${targetCid}\n\n确认继续吗？`)) return;
  const button = elements.hdhiveSearchResults?.querySelector(`[data-hdhive-transfer="${CSS.escape(slug)}"]`);
  if (button) { button.disabled = true; button.textContent = "转存中..."; }
  try {
    const result = await inviteApiFetch("/api/hdhive/transfer", { method: "POST", body: JSON.stringify({ slug, targetCid }) });
    addHDHiveRecord("转存已提交", `${resource.title || slug} · 目录 ${result?.targetCid || targetCid}`, "success");
    showToast("影巢资源已提交 115 转存", 1600);
    resource.isUnlocked = true;
    renderHDHiveResults();
  } catch (error) {
    addHDHiveRecord("转存失败", error.message || "未知错误", "error");
    if (elements.hdhiveSearchSummary) elements.hdhiveSearchSummary.textContent = `转存失败：${error.message || "未知错误"}`;
    if (button) { button.disabled = false; button.textContent = "解锁并转存"; }
  }
}

async function testAiConfig() {
  if (!elements.aiTestBtn || elements.aiTestBtn.disabled) {
    return;
  }
  const config = readAiConfigFromInputs();
  const original = elements.aiTestBtn.textContent || "测试连接";
  elements.aiTestBtn.disabled = true;
  elements.aiTestBtn.textContent = "测试中";
  if (elements.aiFeedback) {
    elements.aiFeedback.textContent = "正在测试 AI 连接...";
  }
  try {
    const result = await inviteApiFetch("/api/ai/test", {
      method: "POST",
      body: JSON.stringify({ aiConfig: config })
    });
    updateAiFeedbackFromInputs({ message: result?.message ? `${result.message}，请点击保存让机器人生效。` : "连接测试成功，请点击保存让机器人生效。" });
    showToast("AI 连接测试成功", 1200);
  } catch (error) {
    if (elements.aiFeedback) {
      elements.aiFeedback.textContent = `AI 连接测试失败：${error.message || "未知错误"}`;
    }
    showToast("AI 连接测试失败", 1200);
  } finally {
    elements.aiTestBtn.disabled = false;
    elements.aiTestBtn.textContent = original;
  }
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
  const lastPlaybackReceivedAt = String(source.lastPlaybackReceivedAt || "").trim();
  const playbackRaw = source.playbackStatus && typeof source.playbackStatus === "object" ? source.playbackStatus : {};
  const playbackProcessed = normalizeBotWebhookProcessed(playbackRaw.lastProcessed || source.lastPlaybackProcessed);
  return {
    lastReceivedAt,
    lastProcessed: processed,
    lastPlaybackReceivedAt,
    playbackStatus: {
      received: Boolean(playbackRaw.received || lastPlaybackReceivedAt),
      lastReceivedAt: String(playbackRaw.lastReceivedAt || lastPlaybackReceivedAt || "").trim(),
      lastProcessed: playbackProcessed,
      result: String(playbackRaw.result || (playbackProcessed?.result ?? "")).trim(),
      detail: String(playbackRaw.detail || (playbackProcessed?.detail ?? "")).trim()
    }
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
  const raw = String(url || "").trim();
  if (!raw) {
    return "当前 Webhook 地址为空，建议设置 VISTAMIRROR_PUBLIC_BASE_URL 为 VistaMirror 的公网访问地址。";
  }
  try {
    const parsed = new URL(raw);
    if (isPrivateOrLocalHost(parsed.hostname)) {
      return "当前 Webhook 地址是本机或内网地址，外部 Emby 无法可靠回调。请设置 VISTAMIRROR_PUBLIC_BASE_URL 为 VistaMirror 的公网访问地址。";
    }
  } catch (_error) {
    return "当前 Webhook 地址格式异常，建议设置 VISTAMIRROR_PUBLIC_BASE_URL 为 VistaMirror 的公网访问地址。";
  }
  return "";
}

function getBotWebhookResultLabel(result) {
  const labels = {
    not_received: "not_received（未收到播放回调）",
    sent: "sent（已发送）",
    core_disabled: "core_disabled（总开关关闭）",
    playback_disabled: "playback_disabled（播放通知关闭）",
    playback_event_disabled: "playback_event_disabled（该播放事件已关闭）",
    playback_user_filtered: "playback_user_filtered（该用户不在通知名单）",
    library_disabled: "library_disabled（入库通知关闭）",
    unsupported_event: "unsupported_event（未识别事件）",
    telegram_not_configured: "telegram_not_configured（TG 未配置）",
    wecom_not_configured: "wecom_not_configured（企业微信未配置）",
    token_invalid: "token_invalid（token 无效）",
    invalid_payload: "invalid_payload（请求体无效）",
    telegram_error: "telegram_error（发送失败）",
    dispatch_error: "dispatch_error（通知派发失败）",
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
  const playback = appState.botWebhookState?.playbackStatus || null;
  elements.botWebhookStatus.hidden = true;
  elements.botWebhookStatus.textContent = "";
  elements.botWebhookStatus.classList.remove("route-status-warning");
  if (!playback) {
    return;
  }
  const result = String(playback.result || "").trim() || "not_received";
  const detail = String(playback.detail || "").trim();
  const receivedAt = String(playback.lastReceivedAt || "").trim();
  let text = "";
  if (result === "not_received") {
    text = "播放回调：最近未收到 Emby 播放回调，播放通知不会触发。";
  } else if (result === "sent") {
    text = `播放回调：最近一次已处理并发送。${detail ? ` ${detail}` : ""}`;
  } else {
    text = `播放回调：最近一次已收到，但处理结果为 ${getBotWebhookResultLabel(result)}。${detail ? ` ${detail}` : ""}`;
  }
  if (receivedAt) {
    text += `\n最近接收：${formatDate(receivedAt)}`;
  }
  if (appState.botWebhookWarning) {
    text += `\n地址提示：${appState.botWebhookWarning}`;
  }
  elements.botWebhookStatus.hidden = false;
  elements.botWebhookStatus.textContent = text;
  elements.botWebhookStatus.classList.toggle("route-status-warning", result !== "sent");
}

async function refreshBotWebhookInfo(options = {}) {
  const { silent = true, statusOnly = false } = options;
  if (!isAdminReady()) {
    return false;
  }
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
  if (!isAdminReady()) {
    return;
  }
  if (appState.botWebhookStatusTimer) {
    return;
  }
  appState.botWebhookStatusTimer = setInterval(() => {
    if (document.visibilityState === "visible" && isAdminReady()) {
      refreshBotWebhookInfo({ silent: true, statusOnly: true });
    }
  }, 15000);
}

function normalizeNotificationPlaybackUsers(rows) {
  const list = Array.isArray(rows) ? rows : [];
  const next = [];
  const seen = new Set();
  list.forEach((row) => {
    if (!row || typeof row !== "object") {
      return;
    }
    const id = String(row.id || row.userId || row.Id || "").trim();
    const name = String(row.name || row.userName || row.Name || "").trim();
    if (!id && !name) {
      return;
    }
    const key = `${name.toLowerCase()}::${id.toLowerCase()}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    next.push({
      id,
      name,
      disabled: Boolean(row.disabled)
    });
  });
  return next.sort((a, b) => a.name.localeCompare(b.name, "zh-CN"));
}

function getNotificationPlaybackKnownUserKeys(users = []) {
  const keys = new Set();
  users.forEach((user) => {
    const id = String(user?.id || "").trim();
    const name = String(user?.name || "").trim();
    if (id) {
      keys.add(`id:${id.toLowerCase()}`);
    }
    if (name) {
      keys.add(`name:${name.toLowerCase()}`);
    }
  });
  return keys;
}

function renderNotificationPlaybackUserScope(config = appState.notificationConfig) {
  const normalized = normalizeNotificationConfig(config, appState.botConfig);
  const scope = normalized.runtime?.playback?.userScope || normalizePlaybackUserScope(null);
  const mode = String(scope.mode || "all") === "selected" ? "selected" : "all";
  const users = Array.isArray(appState.notificationPlaybackUsers) ? appState.notificationPlaybackUsers : [];
  const selectedNames = new Set((scope.selectedUserNames || []).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean));
  const selectedIds = new Set(
    (scope.selectedUsersMeta || [])
      .map((row) => (row && typeof row === "object" ? String(row.id || row.userId || "").trim().toLowerCase() : ""))
      .filter(Boolean)
  );
  if (elements.notifyPlaybackUserScopeAll) {
    elements.notifyPlaybackUserScopeAll.checked = mode === "all";
  }
  if (elements.notifyPlaybackUserScopeSelected) {
    elements.notifyPlaybackUserScopeSelected.checked = mode === "selected";
  }
  const disabled = mode !== "selected";
  if (elements.notifyPlaybackUserPicker) {
    elements.notifyPlaybackUserPicker.classList.toggle("is-disabled", disabled);
  }
  if (elements.notifyPlaybackUsersList) {
    elements.notifyPlaybackUsersList.innerHTML = users.length
      ? users
          .map((user) => {
            const id = String(user.id || "").trim();
            const name = String(user.name || "").trim() || id || "未命名用户";
            const checked = selectedIds.has(id.toLowerCase()) || selectedNames.has(name.toLowerCase());
            return `
              <label class="notify-playback-user-option${disabled ? " is-disabled" : ""}">
                <input
                  type="checkbox"
                  data-notify-playback-user
                  data-user-id="${escapeHtml(id)}"
                  data-user-name="${escapeHtml(name)}"
                  ${checked ? "checked" : ""}
                  ${disabled ? "disabled" : ""}
                >
                <span class="notify-playback-user-option-copy">
                  <strong>${escapeHtml(name)}</strong>
                  <small>${id ? `Emby ID：${escapeHtml(id)}` : "未返回用户 ID"}</small>
                </span>
              </label>
            `;
          })
          .join("")
      : '<div class="empty-state compact">当前还没有读取到 Emby 用户。</div>';
  }
  if (elements.notifyPlaybackUsersStatus) {
    const selectedCount = users.filter((user) => {
      const id = String(user.id || "").trim().toLowerCase();
      const name = String(user.name || "").trim().toLowerCase();
      return (id && selectedIds.has(id)) || (name && selectedNames.has(name));
    }).length;
    const preservedNames = (scope.selectedUserNames || []).filter(
      (name) => !users.some((user) => String(user.name || "").trim().toLowerCase() === String(name || "").trim().toLowerCase())
    );
    if (!users.length) {
      elements.notifyPlaybackUsersStatus.textContent = appState.notificationPlaybackUsersMessage || "当前还没有读取到 Emby 用户。";
    } else if (mode === "all") {
      elements.notifyPlaybackUsersStatus.textContent = `当前为全部用户通知，共读取到 ${users.length} 个 Emby 用户。`;
    } else {
      const extraText = preservedNames.length ? ` 另保留 ${preservedNames.length} 个历史用户名。` : "";
      elements.notifyPlaybackUsersStatus.textContent = `当前只通知已勾选用户，已选择 ${selectedCount} / ${users.length} 个。${extraText}`;
    }
  }
}

async function loadNotificationPlaybackUsers(options = {}) {
  const { silent = true } = options;
  if (appState.notificationPlaybackUsersPromise) {
    return appState.notificationPlaybackUsersPromise;
  }
  appState.notificationPlaybackUsersPromise = (async () => {
    try {
      const result = await inviteApiFetch("/api/notifications/playback-users");
      appState.notificationPlaybackUsers = normalizeNotificationPlaybackUsers(result?.users || []);
      appState.notificationPlaybackUsersMessage = String(result?.detail || "").trim();
    } catch (error) {
      const fallbackUsers = normalizeNotificationPlaybackUsers(appState.users || []);
      appState.notificationPlaybackUsers = fallbackUsers;
      appState.notificationPlaybackUsersMessage =
        fallbackUsers.length > 0
          ? "读取通知专用用户列表失败，已先使用当前已同步的 Emby 用户。"
          : `读取 Emby 用户列表失败：${error.message || "未知错误"}`;
      if (!silent && elements.botFeedback) {
        elements.botFeedback.textContent = appState.notificationPlaybackUsersMessage;
        elements.botFeedback.classList.remove("feedback-success");
      }
    } finally {
      appState.notificationPlaybackUsersPromise = null;
      renderNotificationPlaybackUserScope(appState.notificationConfig);
    }
    return Array.isArray(appState.notificationPlaybackUsers) ? appState.notificationPlaybackUsers : [];
  })();
  return appState.notificationPlaybackUsersPromise;
}

function getNotificationEventDefinitions() {
  return Array.isArray(appState.notificationCapabilities?.events) ? appState.notificationCapabilities.events : [];
}

function getNotificationUpcomingDefinitions() {
  return Array.isArray(appState.notificationCapabilities?.upcomingEvents) ? appState.notificationCapabilities.upcomingEvents : [];
}

function getNotificationEventDefinition(eventKey) {
  return getNotificationEventDefinitions().find((row) => row.key === eventKey) || null;
}

function getNotificationDefaultTemplate(channel, eventKey) {
  const eventDef = getNotificationEventDefinition(eventKey);
  return (
    String(eventDef?.defaultTemplateByChannel?.[channel] || "").trim() ||
    String(DEFAULT_NOTIFICATION_CONFIG.templates?.[channel]?.[eventKey] || "").trim()
  );
}

function getNotificationPreviewPayload(eventKey, sampleKey = "default") {
  const eventDef = getNotificationEventDefinition(eventKey);
  const samplePayloads = eventDef?.samplePayloads && typeof eventDef.samplePayloads === "object" ? eventDef.samplePayloads : {};
  if (samplePayloads[sampleKey] && typeof samplePayloads[sampleKey] === "object") {
    return samplePayloads[sampleKey];
  }
  const firstKey = Object.keys(samplePayloads)[0];
  return firstKey ? samplePayloads[firstKey] || {} : {};
}

function getNotificationRuntimeAdjustedPayload(eventKey, sampleKey = "default") {
  const basePayload = getNotificationPreviewPayload(eventKey, sampleKey);
  const payload = basePayload && typeof basePayload === "object" ? { ...basePayload } : {};
  if (!String(eventKey || "").startsWith("playback.")) {
    return payload;
  }
  const runtime = normalizeNotificationConfig(appState.notificationConfig, appState.botConfig).runtime?.playback || {};
  const showIp = Boolean(runtime.showIp ?? true);
  const showIpGeo = Boolean(runtime.showIpGeo ?? true);
  const showOverview = Boolean(runtime.showOverview ?? true);
  if (!showIp) {
    payload.ip_line = "";
  } else if (!showIpGeo && typeof payload.ip_line === "string") {
    payload.ip_line = payload.ip_line
      .replace(/（[^）]*）/g, "")
      .replace(/\([^)]*\)/g, "")
      .replace(/^(.+?\b\d{1,3}(?:\.\d{1,3}){3}\b).*$/, "$1")
      .trim();
  }
  if (!showOverview) {
    payload.overview_block = "";
  }
  return payload;
}

function getNotificationSampleLabel(sampleKey) {
  const map = {
    default: "默认示例",
    singleMovie: "电影示例",
    singleEpisode: "单集示例",
    groupedSeries: "剧集聚合示例"
  };
  return map[sampleKey] || sampleKey;
}

function getNotificationChannelLabel(channel) {
  return channel === "wecom" ? "企业微信" : "Telegram";
}

function formatNotificationSamplePayload(payload) {
  try {
    return JSON.stringify(payload && typeof payload === "object" ? payload : {}, null, 2);
  } catch (_error) {
    return "{}";
  }
}

function renderNotificationTemplateText(template, context) {
  const source = String(template || "");
  const rendered = source.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, key) => {
    const value = context && Object.prototype.hasOwnProperty.call(context, key) ? context[key] : "";
    return String(value ?? "");
  });
  return rendered
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function readNotificationRouteValues(channel, fallback = {}) {
  const next = { ...fallback };
  document.querySelectorAll(`[data-notify-route-channel="${channel}"]`).forEach((input) => {
    if (input instanceof HTMLInputElement) {
      next[String(input.dataset.eventKey || "")] = Boolean(input.checked);
    }
  });
  return next;
}

function readNotificationTemplateValues(channel, fallback = {}) {
  const next = { ...fallback };
  document.querySelectorAll(`[data-notify-template-channel="${channel}"]`).forEach((textarea) => {
    if (textarea instanceof HTMLTextAreaElement) {
      next[String(textarea.dataset.eventKey || "")] = textarea.value;
    }
  });
  return next;
}

function readNotificationPlaybackUserScope(currentScope) {
  const fallbackScope = normalizePlaybackUserScope(currentScope);
  const knownUsers = Array.isArray(appState.notificationPlaybackUsers) ? appState.notificationPlaybackUsers : [];
  const knownKeys = getNotificationPlaybackKnownUserKeys(knownUsers);
  const mode = elements.notifyPlaybackUserScopeSelected?.checked ? "selected" : "all";
  const selectedUserNames = [];
  const seenNames = new Set();
  const selectedUsersMeta = [];
  const seenMeta = new Set();
  document.querySelectorAll("[data-notify-playback-user]").forEach((input) => {
    if (!(input instanceof HTMLInputElement) || !input.checked) {
      return;
    }
    const id = String(input.dataset.userId || "").trim();
    const name = String(input.dataset.userName || "").trim();
    if (name && !seenNames.has(name.toLowerCase())) {
      seenNames.add(name.toLowerCase());
      selectedUserNames.push(name);
    }
    if (id || name) {
      const metaKey = `${name.toLowerCase()}::${id.toLowerCase()}`;
      if (!seenMeta.has(metaKey)) {
        seenMeta.add(metaKey);
        selectedUsersMeta.push({ id, name });
      }
    }
  });

  (fallbackScope.selectedUsersMeta || []).forEach((row) => {
    if (!row || typeof row !== "object") {
      return;
    }
    const id = String(row.id || row.userId || "").trim();
    const name = String(row.name || row.userName || "").trim();
    const matchedKnown = (id && knownKeys.has(`id:${id.toLowerCase()}`)) || (name && knownKeys.has(`name:${name.toLowerCase()}`));
    if (matchedKnown) {
      return;
    }
    if (name && !seenNames.has(name.toLowerCase())) {
      seenNames.add(name.toLowerCase());
      selectedUserNames.push(name);
    }
    if (id || name) {
      const metaKey = `${name.toLowerCase()}::${id.toLowerCase()}`;
      if (!seenMeta.has(metaKey)) {
        seenMeta.add(metaKey);
        selectedUsersMeta.push({ id, name });
      }
    }
  });

  (fallbackScope.selectedUserNames || []).forEach((name) => {
    const safeName = String(name || "").trim();
    if (!safeName || knownKeys.has(`name:${safeName.toLowerCase()}`) || seenNames.has(safeName.toLowerCase())) {
      return;
    }
    seenNames.add(safeName.toLowerCase());
    selectedUserNames.push(safeName);
  });

  return {
    mode,
    selectedUserNames,
    selectedUsersMeta
  };
}

function readBotConfigFromInputs() {
  const current = normalizeNotificationConfig(appState.notificationConfig, appState.botConfig);
  return normalizeNotificationConfig(
    {
      enabled: Boolean(elements.botEnableCore?.checked),
      channels: {
        telegram: {
          enabled: Boolean(elements.notifyTelegramEnabled?.checked),
          botToken: elements.botTelegramToken?.value.trim() || "",
          chatId: elements.botTelegramChatId?.value.trim() || "",
          enableCommands: Boolean(elements.botEnableCommands?.checked),
          proxyUrl: elements.notifyTelegramProxyUrl?.value.trim() || ""
        },
        wecom: {
          enabled: Boolean(elements.notifyWecomEnabled?.checked),
          corpId: elements.botWechatCorpId?.value.trim() || "",
          agentId: elements.botWechatAgentId?.value.trim() || "",
          secret: elements.botWechatSecret?.value.trim() || "",
          toUser: elements.botWechatToUser?.value.trim() || "@all",
          callbackToken: elements.botWechatCallbackToken?.value.trim() || "",
          callbackAes: elements.botWechatCallbackAes?.value.trim() || "",
          callbackUrl: elements.botWechatCallbackUrl?.textContent?.trim() || "",
          proxyUrl: elements.notifyWecomProxyUrl?.value.trim() || ""
        }
      },
      routes: {
        telegram: readNotificationRouteValues("telegram", current.routes.telegram),
        wecom: readNotificationRouteValues("wecom", current.routes.wecom)
      },
      templates: {
        telegram: readNotificationTemplateValues("telegram", current.templates.telegram),
        wecom: readNotificationTemplateValues("wecom", current.templates.wecom)
      },
      display: {
        telegram: readNotificationDisplayValues("telegram", current.display.telegram),
        wecom: readNotificationDisplayValues("wecom", current.display.wecom)
      },
      runtime: {
        ...current.runtime,
        playback: {
          ...current.runtime.playback,
          userScope: readNotificationPlaybackUserScope(current.runtime?.playback?.userScope)
        }
      }
    },
    appState.botConfig
  );
}

function readNotificationDisplayValues(channel, defaults = {}) {
  const next = {};
  Object.keys(defaults || {}).forEach((eventKey) => {
    const fallback = defaults[eventKey] || { label: eventKey, description: "" };
    const labelInput = document.querySelector(
      `[data-notify-display-field="label"][data-notify-display-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
    );
    const descriptionInput = document.querySelector(
      `[data-notify-display-field="description"][data-notify-display-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
    );
    next[eventKey] = {
      label:
        labelInput instanceof HTMLInputElement && labelInput.value.trim()
          ? labelInput.value.trim()
          : String(fallback.label || eventKey),
      description:
        descriptionInput instanceof HTMLInputElement
          ? descriptionInput.value.trim()
          : String(fallback.description || "")
    };
  });
  return next;
}

function renderNotificationRouteList(channel, container, config) {
  if (!container) return;
  const events = getNotificationEventDefinitions();
  const groups = [
    { key: "playback", label: "播放通知", description: "开始、暂停、恢复和停止播放" },
    { key: "library", label: "媒体库通知", description: "单集、电影与剧集入库" }
  ];
  container.innerHTML = groups
    .map((group) => {
      const groupEvents = events.filter((eventDef) => String(eventDef.key || "").startsWith(`${group.key}.`));
      if (!groupEvents.length) return "";
      return `
        <section class="notify-route-category">
          <div class="notify-route-category-head"><strong>${escapeHtml(group.label)}</strong><span>${escapeHtml(group.description)}</span></div>
          ${groupEvents
            .map((eventDef) => {
      const checked = Boolean(config.routes?.[channel]?.[eventDef.key]);
      const presentation = getNotificationEventPresentation(channel, eventDef, config);
      return `
        <label class="notify-route-row">
          <div class="notify-route-copy">
            <strong data-notify-display-render="label" data-notify-display-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}" data-notify-display-fallback="${escapeHtml(eventDef.label || eventDef.key)}">${escapeHtml(presentation.label)}</strong>
            <p data-notify-display-render="description" data-notify-display-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}" data-notify-display-fallback="${escapeHtml(eventDef.description || "")}">${escapeHtml(presentation.description)}</p>
          </div>
          <input type="checkbox" data-notify-route-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}" ${checked ? "checked" : ""}>
        </label>
      `;
            })
            .join("")}
        </section>
      `;
    })
    .join("");
}

function renderNotificationUpcomingEvents() {
  if (!elements.notifyUpcomingEvents) return;
  const events = getNotificationUpcomingDefinitions();
  elements.notifyUpcomingEvents.innerHTML = events
    .map(
      (eventDef) => `
        <div class="notify-upcoming-item">
          <strong>${escapeHtml(eventDef.label || eventDef.key)}</strong>
          <span>未接入事件源</span>
        </div>
      `
    )
    .join("");
}

function buildNotificationTemplatePanel(channel, eventDef, config) {
  const template = String(config.templates?.[channel]?.[eventDef.key] || getNotificationDefaultTemplate(channel, eventDef.key));
  const presentation = getNotificationEventPresentation(channel, eventDef, config);
  const samplePayloads = eventDef?.samplePayloads && typeof eventDef.samplePayloads === "object" ? eventDef.samplePayloads : {};
  const sampleKeys = Object.keys(samplePayloads);
  const helperHtml = `
      <p class="notify-template-help">
        这里就是最终发给机器人的通知正文示例。标题、分组标题、每一行前面的中文前缀都可以直接改，
        只要保留 <code>{{...}}</code> 变量不删掉就行。像“📋 播放数据”“🛋️ 终端状态”“= 📦基础参数 =”这些文字也都能自定义。
        如果关闭了 IP 或剧情显示，对应那一行会自动隐藏。
      </p>
    `;
  const optionHtml = sampleKeys
    .map((key) => `<option value="${escapeHtml(key)}">${escapeHtml(getNotificationSampleLabel(key))}</option>`)
    .join("");
  const variableHtml = Array.isArray(eventDef.variables)
    ? eventDef.variables
      .map((item) => {
        const key = String(item?.key || "").trim();
        const label = String(item?.label || key).trim();
        const description = String(item?.description || "").trim();
        return `
          <div class="notify-template-var-chip">
            <strong>{{${escapeHtml(key)}}}</strong>
            <span>${escapeHtml(label)}</span>
            ${description ? `<small>${escapeHtml(description)}</small>` : ""}
          </div>
        `;
      })
      .join("")
    : "";
  const initialSampleKey = sampleKeys[0] || "default";
  const initialSamplePayload = getNotificationRuntimeAdjustedPayload(eventDef.key, initialSampleKey);
  return `
    <details class="notify-template-panel" data-notify-template-panel="${escapeHtml(channel)}:${escapeHtml(eventDef.key)}">
      <summary>
        <span class="notify-template-summary-copy">
          <strong data-notify-display-render="label" data-notify-display-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}" data-notify-display-fallback="${escapeHtml(eventDef.label || eventDef.key)}">${escapeHtml(presentation.label)}</strong>
          <small data-notify-display-render="description" data-notify-display-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}" data-notify-display-fallback="点击展开模板编辑区">${escapeHtml(presentation.description || "点击展开模板编辑区")}</small>
        </span>
        <span class="notify-template-summary-chevron" aria-hidden="true"></span>
      </summary>
      <div class="notify-template-panel-body">
        <div class="notify-template-display-grid">
          <label class="notify-template-display-field">
            <span>卡片标题</span>
            <input
              type="text"
              class="bot-template-input"
              value="${escapeHtml(presentation.label)}"
              data-notify-display-field="label"
              data-notify-display-channel="${escapeHtml(channel)}"
              data-event-key="${escapeHtml(eventDef.key)}"
              maxlength="40"
            >
          </label>
          <label class="notify-template-display-field">
            <span>卡片说明</span>
            <input
              type="text"
              class="bot-template-input"
              value="${escapeHtml(presentation.description)}"
              data-notify-display-field="description"
              data-notify-display-channel="${escapeHtml(channel)}"
              data-event-key="${escapeHtml(eventDef.key)}"
              maxlength="120"
            >
          </label>
        </div>
        ${helperHtml}
        <textarea class="bot-template-textarea" spellcheck="false" data-notify-template-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}">${escapeHtml(template)}</textarea>
        <details class="notify-template-subpanel">
          <summary>
            <span class="notify-template-subpanel-copy">
              <span class="bot-template-preview-label">可用变量</span>
              <span class="notify-template-meta-hint">${Array.isArray(eventDef.variables) ? eventDef.variables.length : 0} 个变量</span>
            </span>
            <span class="notify-template-subpanel-chevron" aria-hidden="true"></span>
          </summary>
          <div class="notify-template-subpanel-body">
            <div class="notify-template-vars">${variableHtml}</div>
          </div>
        </details>
        <details class="notify-template-subpanel">
          <summary>
            <span class="notify-template-subpanel-copy">
              <span class="bot-template-preview-label">示例数据</span>
              <span class="notify-template-meta-hint" data-notify-sample-label-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}">${escapeHtml(getNotificationSampleLabel(initialSampleKey))}</span>
            </span>
            <span class="notify-template-subpanel-actions">
              <select class="bot-template-select" data-notify-sample-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}" aria-label="选择示例数据">
                ${optionHtml}
              </select>
              <span class="notify-template-subpanel-chevron" aria-hidden="true"></span>
            </span>
          </summary>
          <div class="notify-template-subpanel-body">
            <pre class="notify-template-sample" data-notify-sample-preview-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}">${escapeHtml(formatNotificationSamplePayload(initialSamplePayload))}</pre>
          </div>
        </details>
        <div class="bot-template-preview-wrap">
          <div class="notify-template-meta-row">
            <span class="bot-template-preview-label">实时预览</span>
            <span class="notify-template-meta-hint" data-notify-template-meta-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}">${template.length} 字符</span>
          </div>
          <pre class="bot-template-preview" data-notify-preview-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}"></pre>
        </div>
        <p class="notify-template-preview-status" data-notify-preview-status-channel="${escapeHtml(channel)}" data-event-key="${escapeHtml(eventDef.key)}"></p>
      </div>
    </details>
  `;
}

function syncNotificationDisplayPresentation(channel, eventKey) {
  const labelInput = document.querySelector(
    `[data-notify-display-field="label"][data-notify-display-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const descriptionInput = document.querySelector(
    `[data-notify-display-field="description"][data-notify-display-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const definition = getNotificationEventDefinition(eventKey);
  const labelText =
    labelInput instanceof HTMLInputElement && labelInput.value.trim()
      ? labelInput.value.trim()
      : String(definition?.label || eventKey);
  const descriptionText =
    descriptionInput instanceof HTMLInputElement ? descriptionInput.value.trim() : String(definition?.description || "");
  document
    .querySelectorAll(`[data-notify-display-render="label"][data-notify-display-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`)
    .forEach((node) => {
      node.textContent = labelText;
    });
  document
    .querySelectorAll(`[data-notify-display-render="description"][data-notify-display-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`)
    .forEach((node) => {
      const fallback = node instanceof HTMLElement ? String(node.dataset.notifyDisplayFallback || "") : "";
      node.textContent = descriptionText || fallback;
    });
}

async function refreshNotificationTemplatePreview(channel, eventKey) {
  const textarea = document.querySelector(
    `[data-notify-template-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const select = document.querySelector(
    `[data-notify-sample-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const preview = document.querySelector(
    `[data-notify-preview-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const status = document.querySelector(
    `[data-notify-preview-status-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const meta = document.querySelector(
    `[data-notify-template-meta-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const samplePreview = document.querySelector(
    `[data-notify-sample-preview-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  const sampleLabel = document.querySelector(
    `[data-notify-sample-label-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventKey)}"]`
  );
  if (!(textarea instanceof HTMLTextAreaElement) || !(preview instanceof HTMLElement)) {
    return;
  }
  const sampleKey = select instanceof HTMLSelectElement ? select.value : "default";
  const previewKey = `${channel}:${eventKey}`;
  const requestId = `${Date.now()}:${Math.random()}`;
  textarea.dataset.previewRequestId = requestId;
  const samplePayload = getNotificationRuntimeAdjustedPayload(eventKey, sampleKey);
  if (samplePreview instanceof HTMLElement) {
    samplePreview.textContent = formatNotificationSamplePayload(samplePayload);
  }
  if (sampleLabel instanceof HTMLElement) {
    sampleLabel.textContent = getNotificationSampleLabel(sampleKey);
  }
  if (meta instanceof HTMLElement) {
    meta.textContent = `${textarea.value.length} 字符`;
  }
  if (status instanceof HTMLElement) {
    status.textContent = "正在生成预览...";
    status.classList.remove("is-warning", "is-error");
  }
  try {
    const result = await inviteApiFetch("/api/notifications/preview", {
      method: "POST",
      body: JSON.stringify({
        channel,
        eventKey,
        template: textarea.value,
        sampleKey,
        payloadOverrides: samplePayload
      })
    });
    if (textarea.dataset.previewRequestId !== requestId) {
      return;
    }
    preview.textContent = String(result?.previewText || "");
    const missing = Array.isArray(result?.missingVariables) ? result.missingVariables : [];
    if (status instanceof HTMLElement) {
      if (missing.length > 0) {
        status.textContent = `缺失变量：${missing.map((item) => `{{${item}}}`).join(" ")}`;
        status.classList.remove("is-error");
        status.classList.add("is-warning");
      } else {
        status.textContent = "预览已同步，当前模板变量完整。";
        status.classList.remove("is-warning", "is-error");
      }
    }
  } catch (error) {
    preview.textContent = renderNotificationTemplateText(textarea.value, samplePayload);
    if (status instanceof HTMLElement) {
      status.textContent = `预览接口暂不可用，已使用本地预览：${error.message || "未知错误"}`;
      status.classList.remove("is-warning");
      status.classList.add("is-error");
    }
  } finally {
    notificationPreviewTimers.delete(previewKey);
  }
}

function refreshAllNotificationTemplatePreviews() {
  getNotificationEventDefinitions().forEach((eventDef) => {
    refreshNotificationTemplatePreview("telegram", eventDef.key);
    refreshNotificationTemplatePreview("wecom", eventDef.key);
  });
}

function renderBotTemplatePreviews() {
  refreshAllNotificationTemplatePreviews();
}

function bindNotificationTemplateEditors() {
  document.querySelectorAll("[data-notify-display-field]").forEach((input) => {
    input.addEventListener("input", () => {
      const channel = String(input.dataset.notifyDisplayChannel || "");
      const eventKey = String(input.dataset.eventKey || "");
      syncNotificationDisplayPresentation(channel, eventKey);
    });
  });
  document.querySelectorAll("[data-notify-template-channel]").forEach((textarea) => {
    textarea.addEventListener("input", () => {
      const channel = String(textarea.dataset.notifyTemplateChannel || "");
      const eventKey = String(textarea.dataset.eventKey || "");
      const previewKey = `${channel}:${eventKey}`;
      window.clearTimeout(notificationPreviewTimers.get(previewKey));
      notificationPreviewTimers.set(
        previewKey,
        window.setTimeout(() => {
          refreshNotificationTemplatePreview(channel, eventKey);
        }, 180)
      );
    });
  });
  document.querySelectorAll("[data-notify-sample-channel]").forEach((select) => {
    select.addEventListener("change", () => {
      const channel = String(select.dataset.notifySampleChannel || "");
      const eventKey = String(select.dataset.eventKey || "");
      refreshNotificationTemplatePreview(channel, eventKey);
    });
  });
  document.querySelectorAll(".notify-template-subpanel-actions .bot-template-select").forEach((select) => {
    select.addEventListener("pointerdown", (event) => {
      event.stopPropagation();
    });
    select.addEventListener("mousedown", (event) => {
      event.stopPropagation();
    });
    select.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    select.addEventListener("keydown", (event) => {
      event.stopPropagation();
    });
  });
}

function getSupportedNotificationChannels() {
  return ["telegram", "wecom"];
}

function getNotificationChannelMeta(channel) {
  if (channel === "wecom") {
    return {
      key: "wecom",
      label: "企业微信",
      description: "企业通知与回调",
      iconClass: "bot-channel-icon-wechat"
    };
  }
  return {
    key: "telegram",
    label: "Telegram",
    description: "Bot 推送与命令",
    iconClass: "bot-channel-icon-telegram"
  };
}

function isNotificationChannelConfigured(channel, config) {
  const safeChannel = channel === "wecom" ? "wecom" : "telegram";
  const channelConfig = config?.channels?.[safeChannel] || {};
  if (safeChannel === "telegram") {
    return Boolean(String(channelConfig.botToken || "").trim() && String(channelConfig.chatId || "").trim());
  }
  return Boolean(
    String(channelConfig.corpId || "").trim() &&
    String(channelConfig.agentId || "").trim() &&
    String(channelConfig.secret || "").trim()
  );
}

function getNotificationChannelStatusText(channel, config) {
  const safeChannel = channel === "wecom" ? "wecom" : "telegram";
  const enabled = Boolean(config?.channels?.[safeChannel]?.enabled);
  const configured = isNotificationChannelConfigured(safeChannel, config);
  if (!configured) {
    return "待配置";
  }
  return enabled ? "已启用" : "已配置，未启用";
}

function getConfiguredNotificationChannels(config) {
  return getSupportedNotificationChannels().filter((channel) => isNotificationChannelConfigured(channel, config));
}

function getVisibleNotificationChannelCards(config) {
  const channels = getSupportedNotificationChannels();
  const visible = [];
  channels.forEach((channel) => {
    if (isNotificationChannelConfigured(channel, config) || appState.notificationChannelDraftChannel === channel) {
      visible.push(channel);
    }
  });
  return visible;
}

function renderNotificationChannelAddMenu(config) {
  if (!elements.notifyChannelAddMenu) {
    return;
  }
  const remainingChannels = getSupportedNotificationChannels().filter(
    (channel) => !getVisibleNotificationChannelCards(config).includes(channel)
  );
  if (remainingChannels.length === 0) {
    elements.notifyChannelAddMenu.hidden = true;
    elements.notifyChannelAddMenu.innerHTML = "";
    elements.notifyChannelAddToggle?.setAttribute("aria-expanded", "false");
    return;
  }
  elements.notifyChannelAddMenu.innerHTML = remainingChannels
    .map((channel) => {
      const meta = getNotificationChannelMeta(channel);
      return `
        <button class="notify-channel-add-item" type="button" role="menuitem" data-notify-channel-add="${escapeHtml(channel)}">
          <span class="bot-channel-icon ${escapeHtml(meta.iconClass)}" aria-hidden="true"></span>
          <span class="notify-channel-add-copy">
            <strong>${escapeHtml(meta.label)}</strong>
            <small>${escapeHtml(meta.description)}</small>
          </span>
        </button>
      `;
    })
    .join("");
  elements.notifyChannelAddMenu.hidden = !appState.notificationChannelMenuOpen;
  elements.notifyChannelAddToggle?.setAttribute("aria-expanded", appState.notificationChannelMenuOpen ? "true" : "false");
}

function renderNotificationChannelCards(config) {
  const visibleChannels = getVisibleNotificationChannelCards(config);
  const configuredChannels = getConfiguredNotificationChannels(config);
  if (elements.notifyChannelCardList) {
    elements.notifyChannelCardList.hidden = visibleChannels.length === 0;
    elements.notifyChannelCardList.innerHTML = visibleChannels
      .map((channel) => {
        const meta = getNotificationChannelMeta(channel);
        const configured = isNotificationChannelConfigured(channel, config);
        const enabled = Boolean(config?.channels?.[channel]?.enabled);
        const statusText = getNotificationChannelStatusText(channel, config);
        const routeCount = Object.values(config?.routes?.[channel] || {}).filter(Boolean).length;
        return `
          <article class="notify-channel-card ${configured ? "is-configured" : "is-draft"}" data-notify-channel-card-wrap="${escapeHtml(channel)}">
            <button class="notify-channel-card-main" type="button" data-notify-channel-open="${escapeHtml(channel)}">
              <span class="notify-channel-card-status ${enabled ? "is-online" : configured ? "is-idle" : "is-pending"}" aria-hidden="true"></span>
              <span class="notify-channel-card-copy">
                <strong>${escapeHtml(meta.label)}</strong>
                <small>${escapeHtml(statusText)} · 已开启 ${routeCount} 项事件</small>
              </span>
              <span class="bot-channel-icon ${escapeHtml(meta.iconClass)}" aria-hidden="true"></span>
            </button>
            <button class="notify-channel-card-remove" type="button" aria-label="移除 ${escapeHtml(meta.label)}" data-notify-channel-remove="${escapeHtml(channel)}">×</button>
          </article>
        `;
      })
      .join("");
  }
  if (elements.notifyChannelEmptyState) {
    elements.notifyChannelEmptyState.hidden = visibleChannels.length > 0;
  }
  if (elements.notifyChannelToolbar) {
    elements.notifyChannelToolbar.hidden = visibleChannels.length === 0 && !appState.notificationChannelMenuOpen;
  }
  if (elements.notifyChannelAddToggle) {
    const remainingChannels = getSupportedNotificationChannels().filter((channel) => !visibleChannels.includes(channel));
    elements.notifyChannelAddToggle.hidden = remainingChannels.length === 0;
  }
  renderNotificationChannelAddMenu(config);
}

function renderNotificationConfiguredSections(config) {
  const configuredChannels = getConfiguredNotificationChannels(config);
  const activeChannel = configuredChannels.includes(appState.notificationWorkspaceChannel)
    ? appState.notificationWorkspaceChannel
    : configuredChannels[0] || "telegram";
  appState.notificationWorkspaceChannel = activeChannel;
  document.querySelectorAll("[data-notify-route-pane]").forEach((node) => {
    const channel = String(node.getAttribute("data-notify-route-pane") || "");
    node.hidden = channel !== activeChannel || !configuredChannels.includes(channel);
  });
  const routeGrid = document.querySelector("#view-bot-assistant .notify-matrix-grid");
  const templateGrid = document.querySelector("#view-bot-assistant .notify-template-center-grid");
  if (elements.notifyRoutesEmpty) {
    elements.notifyRoutesEmpty.hidden = configuredChannels.length > 0;
  }
  if (elements.notifyTemplatesEmpty) {
    elements.notifyTemplatesEmpty.hidden = configuredChannels.length > 0;
  }
  if (routeGrid instanceof HTMLElement) {
    routeGrid.hidden = configuredChannels.length === 0;
    routeGrid.dataset.configuredCount = String(Math.min(Math.max(configuredChannels.length, 0), 2));
  }
  if (templateGrid instanceof HTMLElement) {
    templateGrid.hidden = configuredChannels.length === 0;
    templateGrid.dataset.configuredCount = String(Math.min(Math.max(configuredChannels.length, 0), 2));
  }
}

function renderNotificationWorkspaceControls(config) {
  const configuredChannels = getConfiguredNotificationChannels(config);
  const events = getNotificationEventDefinitions();
  if (!configuredChannels.includes(appState.notificationWorkspaceChannel)) {
    appState.notificationWorkspaceChannel = configuredChannels[0] || "telegram";
  }
  if (!events.some((eventDef) => eventDef.key === appState.notificationWorkspaceEvent)) {
    appState.notificationWorkspaceEvent = events[0]?.key || "";
  }
  const activeChannel = appState.notificationWorkspaceChannel;
  const activeEvent = appState.notificationWorkspaceEvent;
  const buildChannelTabs = (target) => {
    if (!(target instanceof HTMLElement)) return;
    target.innerHTML = configuredChannels
      .map((channel) => {
        const meta = getNotificationChannelMeta(channel);
        const active = channel === activeChannel;
        return `<button class="notify-workspace-tab ${active ? "is-active" : ""}" type="button" data-notify-workspace-channel="${escapeHtml(channel)}"><span class="bot-channel-icon ${escapeHtml(meta.iconClass)}"></span>${escapeHtml(meta.label)}</button>`;
      })
      .join("");
  };
  buildChannelTabs(elements.notifyRouteChannelTabs);
  buildChannelTabs(elements.notifyTemplateChannelTabs);
  if (elements.notifyTemplateEventList) {
    elements.notifyTemplateEventList.innerHTML = events
      .map((eventDef) => {
        const presentation = getNotificationEventPresentation(activeChannel, eventDef, config);
        const active = eventDef.key === activeEvent;
        return `<button class="notify-template-event-button ${active ? "is-active" : ""}" type="button" data-notify-workspace-event="${escapeHtml(eventDef.key)}"><strong>${escapeHtml(presentation.label)}</strong><small>${escapeHtml(presentation.description)}</small></button>`;
      })
      .join("");
  }
  ["telegram", "wecom"].forEach((channel) => {
    const list = channel === "telegram" ? elements.notifyTemplateListTelegram : elements.notifyTemplateListWecom;
    if (list instanceof HTMLElement) list.hidden = channel !== activeChannel || !configuredChannels.includes(channel);
  });
  document.querySelectorAll("[data-notify-template-panel]").forEach((panel) => {
    const key = String(panel.getAttribute("data-notify-template-panel") || "");
    const active = key === `${activeChannel}:${activeEvent}`;
    if (panel instanceof HTMLDetailsElement) panel.open = active;
    if (panel instanceof HTMLElement) panel.hidden = !active;
  });
}

function setNotificationWorkspaceChannel(channel) {
  appState.notificationWorkspaceChannel = channel === "wecom" ? "wecom" : "telegram";
  renderNotificationConfiguredSections(readBotConfigFromInputs());
  renderNotificationWorkspaceControls(readBotConfigFromInputs());
}

function setNotificationWorkspaceEvent(eventKey) {
  appState.notificationWorkspaceEvent = String(eventKey || "");
  renderNotificationWorkspaceControls(readBotConfigFromInputs());
}

function renderNotificationChannelModal(config) {
  const channel = appState.notificationChannelModalChannel;
  const meta = getNotificationChannelMeta(channel);
  const isTelegram = channel === "telegram";
  if (elements.notifyChannelModalTitle) {
    elements.notifyChannelModalTitle.textContent = channel ? `${meta.label} 配置` : "配置渠道";
  }
  if (elements.notifyChannelModalSubtitle) {
    elements.notifyChannelModalSubtitle.textContent = channel
      ? `填写 ${meta.label} 接入信息`
      : "填写通知渠道接入信息";
  }
  if (elements.notifyChannelModalIcon) {
    elements.notifyChannelModalIcon.className = `bot-channel-icon ${meta.iconClass}`;
  }
  // The modal is mounted under document.body. Set both visibility mechanisms
  // explicitly so the inactive channel cannot leak through page-level styles.
  const setChannelPaneVisibility = (pane, paneChannel) => {
    if (!(pane instanceof HTMLElement)) return;
    const active = Boolean(channel) && paneChannel === channel;
    pane.hidden = !active;
    pane.classList.toggle("is-active", active);
    pane.setAttribute("aria-hidden", active ? "false" : "true");
    pane.style.display = active ? "grid" : "none";
  };
  setChannelPaneVisibility(elements.notifyPaneTelegram, "telegram");
  setChannelPaneVisibility(elements.notifyPaneWecom, "wecom");
  const platformEnabled = Boolean(config?.enabled);
  if (elements.notifyChannelPlatformSummary) {
    const webhookWarning = appState.botWebhookWarning ? "Webhook 未就绪" : "Webhook 已就绪";
    elements.notifyChannelPlatformSummary.textContent =
      `平台${platformEnabled ? "已启用" : "未启用"} · ${webhookWarning}`;
  }
  if (elements.notifyChannelModalTest) {
    elements.notifyChannelModalTest.disabled = !channel || !isNotificationChannelConfigured(channel, config);
  }
  const activeSection = ["channel", "platform", "playback"].includes(appState.notificationModalSection)
    ? appState.notificationModalSection
    : "channel";
  document.querySelectorAll("[data-notify-modal-section]").forEach((node) => {
    const section = String(node.getAttribute("data-notify-modal-section") || "");
    node.hidden = section !== activeSection;
  });
  document.querySelectorAll("[data-notify-modal-section-target]").forEach((button) => {
    const section = String(button.getAttribute("data-notify-modal-section-target") || "");
    button.classList.toggle("is-active", section === activeSection);
    button.setAttribute("aria-selected", section === activeSection ? "true" : "false");
  });
}

function mountNotificationChannelModalToBody() {
  const modal = elements.notifyChannelModal;
  if (!(modal instanceof HTMLElement)) {
    return;
  }
  if (modal.parentElement !== document.body) {
    document.body.appendChild(modal);
  }
}

function openNotificationChannelModal(channel) {
  const safeChannel = channel === "wecom" ? "wecom" : "telegram";
  appState.notificationChannelModalChannel = safeChannel;
  appState.notificationModalSection = "channel";
  appState.notificationChannelMenuOpen = false;
  mountNotificationChannelModalToBody();
  if (elements.notifyChannelModal) {
    elements.notifyChannelModal.hidden = false;
  }
  renderBotAssistant();
  if (elements.notifyChannelModalContent instanceof HTMLElement) {
    elements.notifyChannelModalContent.scrollTop = 0;
  }
  loadNotificationPlaybackUsers({ silent: true });
}

function closeNotificationChannelModal() {
  const currentChannel = appState.notificationChannelModalChannel;
  appState.notificationChannelModalChannel = "";
  appState.notificationChannelMenuOpen = false;
  if (currentChannel && appState.notificationChannelDraftChannel === currentChannel) {
    const savedConfig = normalizeNotificationConfig(appState.notificationConfig, appState.botConfig);
    if (!isNotificationChannelConfigured(currentChannel, savedConfig)) {
      appState.notificationChannelDraftChannel = "";
    }
  }
  if (elements.notifyChannelModal) {
    elements.notifyChannelModal.hidden = true;
  }
  renderBotAssistant();
}

function toggleNotificationChannelMenu() {
  const config = readBotConfigFromInputs();
  const remainingChannels = getSupportedNotificationChannels().filter(
    (channel) => !getVisibleNotificationChannelCards(config).includes(channel)
  );
  if (!remainingChannels.length) {
    appState.notificationChannelMenuOpen = false;
    renderNotificationChannelCards(config);
    return;
  }
  appState.notificationChannelMenuOpen = !appState.notificationChannelMenuOpen;
  renderNotificationChannelCards(config);
}

function addNotificationChannel(channel) {
  const safeChannel = channel === "wecom" ? "wecom" : "telegram";
  appState.notificationChannelDraftChannel = safeChannel;
  appState.notificationChannelMenuOpen = false;
  renderBotAssistant();
  openNotificationChannelModal(safeChannel);
}

async function removeNotificationChannel(channel) {
  const safeChannel = channel === "wecom" ? "wecom" : "telegram";
  const nextConfig = normalizeNotificationConfig(appState.notificationConfig, appState.botConfig);
  if (safeChannel === "telegram") {
    nextConfig.channels.telegram = {
      ...nextConfig.channels.telegram,
      enabled: false,
      botToken: "",
      chatId: "",
      proxyUrl: ""
    };
  } else {
    nextConfig.channels.wecom = {
      ...nextConfig.channels.wecom,
      enabled: false,
      corpId: "",
      agentId: "",
      secret: "",
      toUser: "@all",
      callbackToken: "",
      callbackAes: "",
      callbackUrl: "",
      proxyUrl: ""
    };
  }
  try {
    await pushBotConfigToServer(nextConfig);
    if (appState.notificationChannelDraftChannel === safeChannel) {
      appState.notificationChannelDraftChannel = "";
    }
    if (appState.notificationChannelModalChannel === safeChannel) {
      appState.notificationChannelModalChannel = "";
      if (elements.notifyChannelModal) {
        elements.notifyChannelModal.hidden = true;
      }
    }
    if (elements.botFeedback) {
      elements.botFeedback.textContent = `${getNotificationChannelLabel(safeChannel)} 已移除。`;
      elements.botFeedback.classList.add("feedback-success");
    }
    showToast(`${getNotificationChannelLabel(safeChannel)} 已移除`, 1000);
    addSyncEvent(`${getNotificationChannelLabel(safeChannel)} 已移除`, "通知渠道已关闭并从顶部卡片区隐藏。", "success");
    renderBotAssistant();
  } catch (error) {
    if (elements.botFeedback) {
      elements.botFeedback.textContent = `移除失败：${error.message || "未知错误"}`;
      elements.botFeedback.classList.remove("feedback-success");
    }
    showToast("通知渠道移除失败", 1200);
  }
}

function renderNotificationTemplateCenter(config) {
  if (elements.notifyTemplateListTelegram) {
    elements.notifyTemplateListTelegram.innerHTML = getNotificationEventDefinitions()
      .map((eventDef) => buildNotificationTemplatePanel("telegram", eventDef, config))
      .join("");
  }
  if (elements.notifyTemplateListWecom) {
    elements.notifyTemplateListWecom.innerHTML = getNotificationEventDefinitions()
      .map((eventDef) => buildNotificationTemplatePanel("wecom", eventDef, config))
      .join("");
  }
  bindNotificationTemplateEditors();
  renderNotificationWorkspaceControls(config);
  const channel = appState.notificationWorkspaceChannel;
  const eventKey = appState.notificationWorkspaceEvent;
  if (eventKey) refreshNotificationTemplatePreview(channel, eventKey);
}

function renderBotAssistant() {
  if (!elements.botEnableCore || !elements.notifyRouteGridTelegram || !elements.notifyRouteGridWecom) {
    return;
  }
  mountNotificationChannelModalToBody();
  const config = normalizeNotificationConfig(appState.notificationConfig, appState.botConfig);
  appState.notificationConfig = config;
  elements.botEnableCore.checked = Boolean(config.enabled);
  if (elements.notifyTelegramEnabled) elements.notifyTelegramEnabled.checked = Boolean(config.channels?.telegram?.enabled);
  if (elements.notifyWecomEnabled) elements.notifyWecomEnabled.checked = Boolean(config.channels?.wecom?.enabled);
  if (elements.botEnableCommands) elements.botEnableCommands.checked = Boolean(config.channels?.telegram?.enableCommands);
  if (elements.botTelegramToken) elements.botTelegramToken.value = config.channels?.telegram?.botToken || "";
  if (elements.botTelegramChatId) elements.botTelegramChatId.value = config.channels?.telegram?.chatId || "";
  if (elements.notifyTelegramProxyUrl) elements.notifyTelegramProxyUrl.value = config.channels?.telegram?.proxyUrl || "";
  if (elements.botWechatCorpId) elements.botWechatCorpId.value = config.channels?.wecom?.corpId || "";
  if (elements.botWechatAgentId) elements.botWechatAgentId.value = config.channels?.wecom?.agentId || "";
  if (elements.botWechatSecret) elements.botWechatSecret.value = config.channels?.wecom?.secret || "";
  if (elements.botWechatToUser) elements.botWechatToUser.value = config.channels?.wecom?.toUser || "@all";
  if (elements.botWechatCallbackToken) elements.botWechatCallbackToken.value = config.channels?.wecom?.callbackToken || "";
  if (elements.botWechatCallbackAes) elements.botWechatCallbackAes.value = config.channels?.wecom?.callbackAes || "";
  if (elements.notifyWecomProxyUrl) elements.notifyWecomProxyUrl.value = config.channels?.wecom?.proxyUrl || "";
  if (elements.notifyTelegramStatus) {
    elements.notifyTelegramStatus.textContent = config.channels?.telegram?.enabled
      ? `Telegram 已启用 · 路由 ${Object.values(config.routes?.telegram || {}).filter(Boolean).length} 项`
      : "Telegram 未启用";
  }
  if (elements.notifyWecomStatus) {
    elements.notifyWecomStatus.textContent = config.channels?.wecom?.enabled
      ? `企业微信已启用 · 路由 ${Object.values(config.routes?.wecom || {}).filter(Boolean).length} 项`
      : "企业微信未启用";
  }
  const webhookUrl = appState.botWebhookUrl || getWebhookUrlForBot();
  appState.botWebhookWarning = getWebhookUrlWarning(webhookUrl);
  if (elements.botWebhookUrl) {
    elements.botWebhookUrl.textContent = webhookUrl;
    elements.botWebhookUrl.classList.toggle("route-copy-warning", Boolean(appState.botWebhookWarning));
  }
  renderBotWebhookStatus();
  renderNotificationPlaybackUserScope(config);
  if (elements.botWechatCallbackUrl) {
    elements.botWechatCallbackUrl.textContent = getWechatCallbackUrlForBot();
  }
  renderNotificationRouteList("telegram", elements.notifyRouteGridTelegram, config);
  renderNotificationRouteList("wecom", elements.notifyRouteGridWecom, config);
  renderNotificationUpcomingEvents();
  renderNotificationTemplateCenter(config);
  renderNotificationChannelCards(config);
  renderNotificationConfiguredSections(config);
  renderNotificationWorkspaceControls(config);
  renderNotificationChannelModal(config);
  if (shouldUseLocalProxy() && isAdminReady()) {
    refreshBotWebhookInfo({ silent: true });
  }
  if (!Array.isArray(appState.notificationPlaybackUsers) || appState.notificationPlaybackUsers.length === 0) {
    loadNotificationPlaybackUsers({ silent: true });
  }
  renderEnvControlledState();
}

async function loadBotConfigFromServer(options = {}) {
  const { silent = false } = options;
  try {
    const [configResult, capabilityResult] = await Promise.all([
      inviteApiFetch("/api/notifications/config"),
      inviteApiFetch("/api/notifications/capabilities")
    ]);
    appState.notificationCapabilities = normalizeNotificationCapabilities(capabilityResult?.capabilities || {});
    appState.notificationConfig = normalizeNotificationConfig(configResult?.notificationConfig || {}, configResult?.botConfig || appState.botConfig);
    appState.botConfig = normalizeBotConfig({
      ...DEFAULT_BOT_CONFIG,
      ...(configResult?.botConfig || appState.botConfig),
      enableCore: appState.notificationConfig.enabled,
      telegramToken: appState.notificationConfig.channels?.telegram?.botToken || "",
      telegramChatId: appState.notificationConfig.channels?.telegram?.chatId || "",
      enableCommands: Boolean(appState.notificationConfig.channels?.telegram?.enableCommands),
      wechatCorpId: appState.notificationConfig.channels?.wecom?.corpId || "",
      wechatAgentId: appState.notificationConfig.channels?.wecom?.agentId || "",
      wechatSecret: appState.notificationConfig.channels?.wecom?.secret || "",
      wechatToUser: appState.notificationConfig.channels?.wecom?.toUser || "@all",
      wechatCallbackToken: appState.notificationConfig.channels?.wecom?.callbackToken || "",
      wechatCallbackAes: appState.notificationConfig.channels?.wecom?.callbackAes || ""
    });
    appState.envControlledFields = mergeEnvControlledFields(configResult?.envControlledFields || capabilityResult?.envControlledFields, "notificationConfig");
    persistLocalState();
    renderBotAssistant();
    return true;
  } catch (error) {
    if (!silent) {
      if (elements.botFeedback) {
        elements.botFeedback.textContent = `读取通知配置失败：${error.message || "未知错误"}`;
        elements.botFeedback.classList.remove("feedback-success");
      }
      showToast("读取通知配置失败", 1200);
    }
    return false;
  }
}

async function pushBotConfigToServer(nextConfig) {
  const payloadConfig = normalizeNotificationConfig(nextConfig, appState.botConfig);
  const managed = appState?.envControlledFields?.notificationConfig || [];
  if (managed.includes("channels.telegram.botToken")) {
    delete payloadConfig.channels.telegram.botToken;
  }
  if (managed.includes("channels.telegram.chatId")) {
    delete payloadConfig.channels.telegram.chatId;
  }
  const result = await inviteApiFetch("/api/notifications/config", {
    method: "POST",
    body: JSON.stringify({ notificationConfig: payloadConfig })
  });
  appState.notificationConfig = normalizeNotificationConfig(result?.notificationConfig || payloadConfig, result?.botConfig || appState.botConfig);
  if (result?.botConfig) {
    appState.botConfig = normalizeBotConfig({ ...DEFAULT_BOT_CONFIG, ...result.botConfig });
  }
  appState.envControlledFields = mergeEnvControlledFields(result?.envControlledFields, "notificationConfig");
  persistLocalState();
  renderBotAssistant();
  return appState.notificationConfig;
}

function setBotActionSuccessState(button) {
  if (!button) {
    return;
  }
  const original = button.dataset.originalText || button.textContent || "保存";
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

async function saveBotConfig(options = {}) {
  const {
    button = null,
    successText = "配置已保存",
    feedbackText = "保存成功",
    eventTitle = "通知配置已保存",
    eventDetail = "通知平台配置、事件路由与模板已更新。"
  } = options;
  const nextConfig = readBotConfigFromInputs();
  try {
    await pushBotConfigToServer(nextConfig);
  } catch (error) {
    if (elements.botFeedback) {
      elements.botFeedback.textContent = `保存失败：${error.message || "未知错误"}`;
      elements.botFeedback.classList.remove("feedback-success");
    }
    showToast("通知配置保存失败", 1200);
    addSyncEvent("通知配置保存失败", error.message || "未知错误", "danger");
    return;
  }

  if (elements.botFeedback) {
    elements.botFeedback.textContent = feedbackText;
    elements.botFeedback.classList.add("feedback-success");
  }
  setBotActionSuccessState(button);
  showToast(successText, 1000);
  addSyncEvent(eventTitle, eventDetail, "success");
}

async function saveNotificationChannel(channel) {
  const label = getNotificationChannelLabel(channel);
  const button = elements.notifyChannelModalSave;
  await saveBotConfig({
    button,
    successText: `${label} 配置已保存`,
    feedbackText: `${label} 通道配置已保存`,
    eventTitle: `${label} 配置已保存`,
    eventDetail: `${label} 通道凭证、路由与模板改动已同步。`
  });
  if (isNotificationChannelConfigured(channel, appState.notificationConfig)) {
    if (appState.notificationChannelDraftChannel === channel) {
      appState.notificationChannelDraftChannel = "";
    }
    closeNotificationChannelModal();
  }
}

async function saveNotificationRoutesCard() {
  await saveBotConfig({
    button: elements.notifyRoutesSave,
    successText: "通知路由已保存",
    feedbackText: "通知类型矩阵已保存",
    eventTitle: "通知路由已保存",
    eventDetail: "按通道事件路由开关已同步到通知运行时。"
  });
}

async function saveNotificationTemplateCenter() {
  await saveBotConfig({
    button: elements.notifyTemplatesSave,
    successText: "模板中心已保存",
    feedbackText: "模板中心配置已保存",
    eventTitle: "通知模板已保存",
    eventDetail: "模板中心的通道模板改动已同步。"
  });
}

async function saveNotificationTemplate(channel, eventKey, button) {
  const label = getNotificationChannelLabel(channel);
  const eventDef = getNotificationEventDefinition(eventKey);
  const eventLabel = String(eventDef?.label || eventKey).trim();
  await saveBotConfig({
    button,
    successText: `${label} ${eventLabel} 模板已保存`,
    feedbackText: `${label} · ${eventLabel} 模板已保存`,
    eventTitle: `${label} 模板已保存`,
    eventDetail: `${eventLabel} 模板已更新并同步到通知运行时。`
  });
}

async function sendBotTest(channel) {
  const nextConfig = readBotConfigFromInputs();
  appState.notificationConfig = normalizeNotificationConfig(nextConfig, appState.botConfig);
  persistLocalState();
  renderBotAssistant();

  try {
    await pushBotConfigToServer(nextConfig);
    const result = await inviteApiFetch("/api/notifications/test", {
      method: "POST",
      body: JSON.stringify({ channel })
    });
    const label = getNotificationChannelLabel(channel);
    if (elements.botFeedback) {
      elements.botFeedback.textContent = result?.detail || `${label} 测试消息已发送。`;
      elements.botFeedback.classList.add("feedback-success");
    }
    showToast(`✅ 测试消息已成功发送至 ${label}！`, 1000);
    addSyncEvent(`${label} 测试发送`, result?.detail || "已发送真实测试消息。", "success");
  } catch (error) {
    const label = getNotificationChannelLabel(channel);
    if (elements.botFeedback) {
      elements.botFeedback.textContent = `${label} 测试发送失败：${error.message || "未知错误"}`;
      elements.botFeedback.classList.remove("feedback-success");
    }
    showToast("通知测试发送失败", 1400);
    addSyncEvent(`${label} 测试发送失败`, error.message || "未知错误", "danger");
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
  let dataReady = false;
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
    notifySessionsUpdated();
    appState.devices = normalizeDeviceList(devicesResult);
    appState.logs = logsResult.Items || [];
    try {
      await loadPlaybackHistory({ limit: 300, scanLimit: 2000, quiet: true });
    } catch (_error) {
      // 播放历史统一接口失败时，不中断主数据同步。
    }
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
    const authSummary = summarizeAuthSignalsFromActivityLogs(appState.logs);
    if (authSummary.loginSuccess || authSummary.loginFailed) {
      sendProjectLog({
        level: authSummary.loginFailed ? "warning" : "info",
        module: "auth",
        action: "login_activity_synced",
        message: `播放历史同步中发现登录事件：成功 ${authSummary.loginSuccess}，失败 ${authSummary.loginFailed}。`,
        detail: authSummary
      });
    }
    dataReady = true;
  } catch (error) {
    renderConnectionState(false, `连接失败：${error.message}`, "danger");
    addSyncEvent("连接 Emby 失败", error.message, "danger");
  }

  renderAll();
  if (dataReady) {
    window.dispatchEvent(new CustomEvent("vistamirror:emby-data-ready"));
  }
}

async function runQualityRescan() {
  if (!appState.config.serverUrl || !appState.config.apiKey) {
    if (elements.userActionFeedback) {
      elements.userActionFeedback.textContent = "请先在媒体库配置里连接 Emby，再执行重扫。";
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
  renderMissing();
  renderProjectLogs();
  renderBotAssistant();
  renderDrive115Page();
  renderHDHiveConfig();
  renderHDHiveResults();
  renderHDHiveRecords();
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

function shouldShowTopbar(view) {
  return TOPBAR_VISIBLE_VIEWS.has(String(view || "").trim());
}

function switchView(view) {
  const normalizedView = view === "settings" ? "media-config" : view;
  const targetView = normalizedView && VIEW_META[normalizedView] ? normalizedView : "";

  if (!targetView) {
    resetSettingsSaveFeedback();
    closeUserCenterInviteModal();
    closeUserCenterInviteManageModal();
    closeUserCenterInviteResultModal();
    closeCreateUserModal();
    closeUserConfigModal();
    appState.activeView = "";
    localStorage.removeItem(STORAGE_KEYS.activeView);
    document.title = "VistaMirror";
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
    if (elements.topbar) {
      elements.topbar.hidden = true;
    }
    if (elements.settingsSaveBtn) {
      elements.settingsSaveBtn.hidden = true;
    }
    if (elements.topbarUserCenterActions) {
      elements.topbarUserCenterActions.hidden = true;
    }
    document.dispatchEvent(new CustomEvent("adaptive:viewchange", { detail: { view: "" } }));
    closeProfileMenu();
    return;
  }

  if (!isConfigView(targetView)) {
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
  const showTopbar = shouldShowTopbar(targetView);

  appState.activeView = targetView;
  localStorage.setItem(STORAGE_KEYS.activeView, targetView);
  document.title = `${meta.title} - VistaMirror`;
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
  if (elements.topbar) {
    elements.topbar.hidden = !showTopbar;
  }
  swapLogsActionBlocks(targetView);
  if (elements.topbarLogsToolbarHost) {
    elements.topbarLogsToolbarHost.hidden = !showTopbar || targetView !== "logs";
  }
  if (elements.topbarActions) {
    elements.topbarActions.hidden = !showTopbar;
  }
  if (elements.settingsSaveBtn) {
    elements.settingsSaveBtn.hidden = !isConfigView(targetView);
  }
  if (elements.topbarUserCenterActions) {
    elements.topbarUserCenterActions.hidden = !showTopbar || targetView !== "user-center";
  }
  if (elements.topbarIcon) {
    elements.topbarIcon.textContent = meta.icon || "👥";
  }
  if (targetView === "missing" && !appState.missingLoading && !appState.missingScannedOnce) {
    loadMissingList({ quiet: true }).catch(() => {
      // 初次进入缺集页读取失败时保持静默，避免打断主流程
    });
  }
  if ((targetView === "media-config" || targetView === "workshop") && shouldUseLocalProxy()) {
    loadCoverStudioConfigFromServer({ silent: true });
    if (appState.config.serverUrl && appState.config.apiKey) {
      loadCoverStudioViews({ silent: true });
    }
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
    await copyTextToClipboard(text);
    showToast(successMessage, 1000);
  } catch {
    showToast("复制失败，请手动复制。", 1200);
  }
}

async function copyTextToClipboard(text) {
  const value = String(text || "");
  if (!value) {
    throw new Error("Nothing to copy");
  }

  if (copyTextWithTextarea(value)) {
    return;
  }

  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  throw new Error("Copy command failed");
}

function copyTextWithTextarea(value) {
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.top = "0";
  textarea.style.left = "0";
  textarea.style.width = "1px";
  textarea.style.height = "1px";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);

  const selection = document.getSelection();
  const selectedRange = selection && selection.rangeCount > 0 ? selection.getRangeAt(0) : null;

  textarea.focus();
  textarea.select();
  textarea.setSelectionRange(0, value.length);

  const copied = document.execCommand("copy");
  textarea.remove();

  if (selectedRange && selection) {
    selection.removeAllRanges();
    selection.addRange(selectedRange);
  }

  return copied;
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
          <span class="invite-result-url">${escapeHtml(link)}</span>
          <button class="invite-result-copy" type="button" data-copy-invite-link="${escapeHtml(link)}">复制</button>
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

function getActiveMediaServerType() {
  const selectedButtonType = document.querySelector("[data-media-server].selected")?.dataset?.mediaServer || "";
  if (MEDIA_SERVER_TYPES.includes(selectedButtonType)) {
    return selectedButtonType;
  }
  return MEDIA_SERVER_TYPES.includes(appState.config?.activeServerType) ? appState.config.activeServerType : "emby";
}

function getMediaServerConfig(type = getActiveMediaServerType()) {
  const safeType = MEDIA_SERVER_TYPES.includes(type) ? type : "emby";
  const mediaServers = appState.config?.mediaServers && typeof appState.config.mediaServers === "object"
    ? appState.config.mediaServers
    : {};
  return normalizeMediaServerConfig(mediaServers[safeType]);
}

function writeMediaServerConfig(type, config) {
  const safeType = MEDIA_SERVER_TYPES.includes(type) ? type : "emby";
  const mediaServers = appState.config?.mediaServers && typeof appState.config.mediaServers === "object"
    ? appState.config.mediaServers
    : {};
  appState.config.mediaServers = {
    emby: normalizeMediaServerConfig(mediaServers.emby),
    jellyfin: normalizeMediaServerConfig(mediaServers.jellyfin),
    [safeType]: normalizeMediaServerConfig(config)
  };
}

function syncActiveMediaServerFields() {
  const activeType = getActiveMediaServerType();
  const activeConfig = getMediaServerConfig(activeType);
  appState.config.serverUrl = activeConfig.serverUrl;
  appState.config.apiKey = activeConfig.apiKey;
  appState.config.clientName = DEFAULT_EMBY_CLIENT_NAME;
}

function renderMediaServerSelector() {
  const activeType = getActiveMediaServerType();
  const meta = MEDIA_SERVER_META[activeType] || MEDIA_SERVER_META.emby;
  elements.mediaServerButtons?.forEach((button) => {
    const selected = button.dataset.mediaServer === activeType;
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-pressed", selected ? "true" : "false");
  });
  if (elements.mediaServerConfigTitle) {
    elements.mediaServerConfigTitle.textContent = "媒体服务器核心";
  }
  if (elements.serverUrl) {
    elements.serverUrl.placeholder = meta.placeholder;
  }
  if (elements.apiKey) {
    elements.apiKey.placeholder = meta.keyPlaceholder;
  }
  if (elements.connectBtn) {
    elements.connectBtn.textContent = `连接 ${meta.label}`;
  }
}

function setActiveMediaServer(type, options = {}) {
  const { persist = false, captureCurrent = true } = options;
  const nextType = MEDIA_SERVER_TYPES.includes(type) ? type : "emby";
  if (captureCurrent) {
    applyConfigFromInputs({ persist: false });
  }
  appState.config.activeServerType = nextType;
  syncActiveMediaServerFields();
  hydrateInputs();
  if (persist) {
    persistLocalState();
  }
}

window.handleMediaServerSwitch = function handleMediaServerSwitch(type) {
  setActiveMediaServer(type, { persist: true });
  const label = MEDIA_SERVER_META[getActiveMediaServerType()]?.label || "媒体服务器";
  renderConnectionState(false, `已切换到 ${label} 配置。`);
};

function readConfigFromInputs() {
  const activeType = getActiveMediaServerType();
  const mediaServerConfig = {
    serverUrl: normalizeServerUrl(elements.serverUrl.value),
    apiKey: elements.apiKey.value.trim(),
    clientName: DEFAULT_EMBY_CLIENT_NAME
  };
  writeMediaServerConfig(activeType, mediaServerConfig);
  const mediaServers = appState.config.mediaServers || {};
  return {
    activeServerType: activeType,
    mediaServers,
    serverUrl: mediaServerConfig.serverUrl,
    apiKey: mediaServerConfig.apiKey,
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
  const activeType = getActiveMediaServerType();
  writeMediaServerConfig(activeType, {
    serverUrl: "",
    apiKey: "",
    clientName: DEFAULT_EMBY_CLIENT_NAME
  });
  appState.config = normalizeAppConfig({
    ...appState.config,
    activeServerType: activeType,
    mediaServers: appState.config.mediaServers
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
  hydrateInputs();
  renderConnectionState(false, `已清除 ${MEDIA_SERVER_META[activeType]?.label || "媒体服务器"} 配置。`);
  addSyncEvent("服务器配置已清除", `${MEDIA_SERVER_META[activeType]?.label || "媒体服务器"} 地址与 API Key 已清空。`, "warning");
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
      state.error = "请先在媒体库配置中连接 Emby。";
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

function isDesktopSidebarAvailable() {
  return window.matchMedia("(min-width: 1367px)").matches;
}

function syncSidebarCollapseState() {
  const canCollapse = isDesktopSidebarAvailable();
  const collapsed = canCollapse && Boolean(appState.sidebarCollapsed);

  document.body.classList.toggle("sidebar-collapsed", collapsed);

  if (elements.primarySidebar) {
    elements.primarySidebar.toggleAttribute("inert", collapsed);
    elements.primarySidebar.setAttribute("aria-hidden", collapsed ? "true" : "false");
  }

  if (elements.sidebarToggleBtn) {
    elements.sidebarToggleBtn.hidden = !canCollapse;
    elements.sidebarToggleBtn.textContent = collapsed ? "›" : "‹";
    elements.sidebarToggleBtn.setAttribute("aria-expanded", collapsed ? "false" : "true");
    elements.sidebarToggleBtn.setAttribute("aria-label", collapsed ? "展开侧边栏" : "隐藏侧边栏");
    elements.sidebarToggleBtn.title = collapsed ? "展开侧边栏" : "隐藏侧边栏";
  }
}

function setSidebarCollapsed(collapsed) {
  appState.sidebarCollapsed = Boolean(collapsed);
  localStorage.setItem(STORAGE_KEYS.sidebarCollapsed, appState.sidebarCollapsed ? "1" : "0");
  syncSidebarCollapseState();
}

function initEvents() {
  elements.authForm?.addEventListener("submit", handleAdminLoginSubmit);
  elements.adminCredentialForm?.addEventListener("submit", handleAdminCredentialSubmit);
  globalSearchModal.mount();
  syncSidebarCollapseState();
  elements.sidebarToggleBtn?.addEventListener("click", () => {
    setSidebarCollapsed(!appState.sidebarCollapsed);
  });
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

  elements.logQueryBtn?.addEventListener("click", async () => {
    appState.logSearch = elements.logSearch.value;
    try {
      await loadPlaybackHistory({ limit: 300, scanLimit: 2000, quiet: false });
    } catch (_error) {
      // 错误提示由 loadPlaybackHistory 内部记录。
    }
    renderLogs();
  });

  elements.missingScanBtn?.addEventListener("click", async () => {
    await scanMissingEpisodes();
  });

  elements.missingStatus?.addEventListener("change", async () => {
    await loadMissingList({ quiet: true });
    renderMissing();
  });

  elements.missingSearch?.addEventListener("input", async () => {
    await loadMissingList({ quiet: true });
    renderMissing();
  });

  elements.projectLogLevel?.addEventListener("change", refreshProjectLogs);
  elements.projectLogModule?.addEventListener("change", refreshProjectLogs);
  elements.projectLogSearch?.addEventListener("input", () => {
    window.clearTimeout(appState.projectLogSearchTimer);
    appState.projectLogSearchTimer = window.setTimeout(refreshProjectLogs, 250);
  });
  elements.projectLogRefresh?.addEventListener("click", refreshProjectLogs);
  elements.projectLogDownload?.addEventListener("click", downloadProjectLogs);
  elements.projectLogClear?.addEventListener("click", clearProjectLogs);

  elements.connectBtn?.addEventListener("click", async () => {
    applyConfigFromInputs();
    await loadEmbyData();
  });

  document.addEventListener("click", (event) => {
    const button = event.target?.closest?.("[data-media-server]");
    if (!button) {
      return;
    }
    const type = button.dataset.mediaServer || "emby";
    window.handleMediaServerSwitch(type);
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
  elements.tmdbTestBtn?.addEventListener("click", () => testTmdbConnection({ silent: false }));
  [
    elements.aiEnabled,
    elements.aiBaseUrl,
    elements.aiApiKey,
    elements.aiModel,
    elements.aiTemperature,
    elements.aiMaxTokens,
    elements.aiContextTokensK
  ].forEach((input) => {
    input?.addEventListener("input", () => updateAiFeedbackFromInputs({ saved: false }));
    input?.addEventListener("change", () => updateAiFeedbackFromInputs({ saved: false }));
  });
  [
    elements.libraryDirectoryRootPath,
    elements.libraryDirectoryRootName,
    elements.libraryDirectoryMaxDepth,
    elements.libraryDirectoryCategories
  ].forEach((input) => {
    input?.addEventListener("input", () => updateLibraryDirectoryFeedback({ saved: false }));
    input?.addEventListener("change", () => updateLibraryDirectoryFeedback({ saved: false }));
  });
  [
    elements.coverStudioViewSelect,
    elements.coverStudioTemplateKey,
    elements.coverStudioPickMode,
    elements.coverStudioPresetName,
    elements.coverStudioTitleText,
    elements.coverStudioSubtitleText,
    elements.coverStudioFontKey,
    elements.coverStudioTitleSize,
    elements.coverStudioSubtitleSize,
    elements.coverStudioTitleAlign,
    elements.coverStudioOverlayStrength,
    elements.coverStudioPosterCount,
    elements.coverStudioAccentTone,
    elements.coverStudioPosterRotation,
    elements.coverStudioTitleYOffset
  ].forEach((input) => {
    input?.addEventListener("input", () => {
      syncCoverStudioDraftFromInputs();
      renderCoverStudioStatus();
      renderCoverStudioPreview();
    });
    input?.addEventListener("change", () => {
      syncCoverStudioDraftFromInputs();
      renderCoverStudioModeControls();
      renderCoverStudioStatus();
      renderCoverStudioPreview();
    });
  });
  elements.coverStudioViewPicker?.addEventListener("change", () => {
    applyCoverStudioManualLibraryCopy();
    syncCoverStudioDraftFromInputs();
    updateCoverStudioTitleMode();
    renderCoverStudioStatus();
    renderCoverStudioPreview();
  });
  elements.coverStudioSelectAllBtn?.addEventListener("click", () => {
    elements.coverStudioViewPicker?.querySelectorAll('input[type="checkbox"]').forEach((input) => {
      input.checked = true;
    });
    syncCoverStudioDraftFromInputs();
    updateCoverStudioTitleMode();
    renderCoverStudioStatus();
    renderCoverStudioPreview();
  });
  elements.coverStudioClearSelectionBtn?.addEventListener("click", () => {
    elements.coverStudioViewPicker?.querySelectorAll('input[type="checkbox"]').forEach((input) => {
      input.checked = false;
    });
    syncCoverStudioDraftFromInputs();
    updateCoverStudioTitleMode();
    renderCoverStudioStatus();
    renderCoverStudioPreview();
  });
  elements.coverStudioTemplateKey?.addEventListener("change", () => {
    const selectedMode = getCoverStudioModeMeta(elements.coverStudioTemplateKey?.value || "fan_spread");
    const defaults = selectedMode?.defaults || {};
    appState.coverStudioConfig = normalizeCoverStudioConfig({
      ...appState.coverStudioConfig,
      draft: {
        ...readCoverStudioDraftFromInputs(),
        titleAlign: defaults.titleAlign,
        overlayStrength: defaults.overlayStrength,
        posterCount: defaults.posterCount,
        accentTone: defaults.accentTone,
        posterRotation: defaults.posterRotation,
        titleYOffset: defaults.titleYOffset
      }
    });
    clearCoverStudioPreview();
    persistLocalState();
    renderCoverStudioSettings();
  });
  elements.coverStudioSaveCurrentBtn?.addEventListener("click", () => {
    saveCoverStudioConfig({ cloneAsNew: false });
  });
  elements.coverStudioSaveAsBtn?.addEventListener("click", () => {
    saveCoverStudioConfig({ cloneAsNew: true });
  });
  elements.coverStudioPreviewBtn?.addEventListener("click", generateCoverStudioPreview);
  elements.coverStudioApplyBtn?.addEventListener("click", applyCoverStudioPreview);
  elements.coverStudioRestoreBtn?.addEventListener("click", restoreCoverStudioBackup);
  elements.coverStudioModeTabs?.addEventListener("click", (event) => {
    const button = event.target?.closest?.("[data-cover-studio-mode]");
    if (!button) {
      return;
    }
    setCoverStudioMode(button.dataset.coverStudioMode);
  });
  elements.coverStudioScheduleViewSelect?.addEventListener("change", applyCoverStudioScheduleLibraryCopy);
  elements.coverStudioSchedulePreviewBtn?.addEventListener("click", generateCoverStudioSchedulePreview);
  [
    elements.coverStudioScheduleTemplateKey,
    elements.coverStudioSchedulePickMode,
    elements.coverStudioScheduleTitleText,
    elements.coverStudioScheduleSubtitleText,
    elements.coverStudioScheduleFontKey,
    elements.coverStudioScheduleTitleAlign,
    elements.coverStudioScheduleTitleSize,
    elements.coverStudioScheduleSubtitleSize,
    elements.coverStudioSchedulePosterCount,
    elements.coverStudioScheduleAccentTone,
    elements.coverStudioSchedulePosterRotation,
    elements.coverStudioScheduleTitleYOffset
  ].forEach((input) => {
    input?.addEventListener("input", () => {
      appState.coverStudioScheduleDraft = readCoverStudioScheduleTemplateFromInputs();
      clearCoverStudioSchedulePreview();
    });
    input?.addEventListener("change", () => {
      appState.coverStudioScheduleDraft = readCoverStudioScheduleTemplateFromInputs();
      clearCoverStudioSchedulePreview();
    });
  });
  elements.coverStudioScheduleTemplateKey?.addEventListener("change", () => {
    const current = readCoverStudioScheduleTemplateFromInputs();
    const defaults = getCoverStudioModeMeta(current.templateKey)?.defaults || {};
    appState.coverStudioScheduleDraft = {
      ...current,
      titleAlign: defaults.titleAlign || current.titleAlign,
      posterCount: Number(defaults.posterCount ?? current.posterCount),
      accentTone: defaults.accentTone || current.accentTone,
      posterRotation: Number(defaults.posterRotation ?? current.posterRotation),
      titleYOffset: Number(defaults.titleYOffset ?? 0)
    };
    renderCoverStudioScheduleTemplateInputs();
    clearCoverStudioSchedulePreview();
  });
  elements.coverStudioScheduleAddBtn?.addEventListener("click", async () => {
    const viewId = String(elements.coverStudioScheduleViewSelect?.value || "").trim();
    if (!viewId) {
      showToast("请选择要自动更新封面的媒体库", 1500);
      return;
    }
    const cron = String(elements.coverStudioScheduleCron?.value || "").trim();
    if (!cron) {
      showToast("请填写计划时间 Cron 表达式", 1400);
      return;
    }
    const existing = (appState.coverStudioConfig?.schedules || []).some((plan) => plan.viewId === viewId);
    if (existing) {
      showToast("这个媒体库已有自动封面计划", 1400);
      return;
    }
    const view = (appState.coverStudioViews || []).find((item) => String(item?.id || "") === viewId) || {};
    const template = readCoverStudioScheduleTemplateFromInputs();
    const previousConfig = appState.coverStudioConfig;
    appState.coverStudioConfig = normalizeCoverStudioConfig({
      ...appState.coverStudioConfig,
      scheduleDraft: template,
      schedules: [
        ...(appState.coverStudioConfig?.schedules || []),
        {
          id: `schedule-${Date.now()}`,
          viewId,
          viewName: String(view.name || viewId),
          enabled: true,
          cron,
          template,
          fingerprint: {},
          initializedAt: "",
          lastCheckedAt: "",
          lastUpdatedAt: "",
          lastStatus: "idle",
          lastMessage: "尚未检查。"
        }
      ]
    });
    if (await saveCoverStudioSchedules({ feedback: "已添加独立自动封面计划" })) {
      appState.coverStudioScheduleDraft = appState.coverStudioConfig.scheduleDraft;
      renderCoverStudioSchedules();
    } else {
      appState.coverStudioConfig = previousConfig;
      renderCoverStudioSchedules();
    }
  });
  elements.coverStudioScheduleList?.addEventListener("click", async (event) => {
    const button = event.target?.closest?.("[data-schedule-action]");
    if (!button) {
      return;
    }
    const card = button.closest("[data-schedule-id]");
    const planId = String(card?.dataset.scheduleId || "");
    const plan = (appState.coverStudioConfig?.schedules || []).find((item) => item.id === planId);
    if (!plan || !card) {
      return;
    }
    const action = button.dataset.scheduleAction || "";
    if (action === "edit") {
      openCoverStudioScheduleEditor(planId);
      return;
    }
    if (action === "remove") {
      appState.coverStudioConfig = normalizeCoverStudioConfig({
        ...appState.coverStudioConfig,
        schedules: (appState.coverStudioConfig?.schedules || []).filter((item) => item.id !== planId)
      });
      await saveCoverStudioSchedules({ feedback: "已删除封面计划" });
      return;
    }
    if (action === "toggle") {
      button.disabled = true;
      updateCoverStudioSchedulePlan(planId, { enabled: !plan.enabled });
      const saved = await saveCoverStudioSchedules({ feedback: plan.enabled ? "已停用自动封面计划" : "已启用自动封面计划" });
      if (!saved) {
        updateCoverStudioSchedulePlan(planId, { enabled: plan.enabled });
        renderCoverStudioSchedules();
      }
      button.disabled = false;
      return;
    }
    if (action !== "force") {
      return;
    }
    button.disabled = true;
    try {
      const result = await inviteApiFetch("/api/cover-studio/schedule/run", {
        method: "POST",
        body: JSON.stringify({ planId, force: true })
      });
      await loadCoverStudioConfigFromServer({ silent: true });
      showToast(result?.message || "计划检查完成", 1600);
    } catch (error) {
      showToast(`执行失败：${error.message || "未知错误"}`, 1600);
    } finally {
      button.disabled = false;
    }
  });
  elements.coverStudioScheduleEditForm?.addEventListener("change", (event) => {
    if (event.target?.matches?.('[data-schedule-modal-field="templateKey"]')) {
      syncCoverStudioScheduleEditorCapabilities();
    }
  });
  [elements.coverStudioScheduleEditClose, elements.coverStudioScheduleEditCancel].forEach((button) => {
    button?.addEventListener("click", closeCoverStudioScheduleEditor);
  });
  elements.coverStudioScheduleEditModal?.addEventListener("click", (event) => {
    if (event.target === elements.coverStudioScheduleEditModal) {
      closeCoverStudioScheduleEditor();
    }
  });
  elements.coverStudioScheduleEditSave?.addEventListener("click", async () => {
    const form = elements.coverStudioScheduleEditForm;
    const planId = String(appState.coverStudioEditingScheduleId || form?.dataset.scheduleId || "");
    const plan = getCoverStudioSchedulePlan(planId);
    if (!plan || !form) {
      closeCoverStudioScheduleEditor();
      return;
    }
    const fieldValue = (name, fallback = "") => String(form.querySelector(`[data-schedule-modal-field="${name}"]`)?.value ?? fallback).trim();
    const fieldNumber = (name, fallback) => {
      const value = Number(form.querySelector(`[data-schedule-modal-field="${name}"]`)?.value);
      return Number.isFinite(value) ? value : fallback;
    };
    const currentTemplate = getCoverStudioScheduleTemplate(plan);
    const selectedTemplate = fieldValue("templateKey", currentTemplate.templateKey) || currentTemplate.templateKey;
    const { supports, posterLimit } = getCoverStudioModeCapabilities(selectedTemplate);
    const nextTemplate = {
      ...currentTemplate,
      templateKey: selectedTemplate,
      pickMode: fieldValue("pickMode", currentTemplate.pickMode) === "recent" ? "recent" : "random",
      titleText: fieldValue("titleText", currentTemplate.titleText),
      subtitleText: fieldValue("subtitleText", currentTemplate.subtitleText),
      fontKey: fieldValue("fontKey", currentTemplate.fontKey) || currentTemplate.fontKey,
      titleAlign: fieldValue("titleAlign", currentTemplate.titleAlign) || currentTemplate.titleAlign,
      titleFontSize: fieldNumber("titleFontSize", currentTemplate.titleFontSize),
      subtitleFontSize: fieldNumber("subtitleFontSize", currentTemplate.subtitleFontSize),
      posterCount: supports.has("posterCount")
        ? Math.min(posterLimit, Math.max(2, fieldNumber("posterCount", currentTemplate.posterCount)))
        : currentTemplate.posterCount,
      accentTone: fieldValue("accentTone", currentTemplate.accentTone) || currentTemplate.accentTone,
      posterRotation: supports.has("posterRotation") ? fieldNumber("posterRotation", currentTemplate.posterRotation) : 0,
      titleYOffset: fieldNumber("titleYOffset", currentTemplate.titleYOffset)
    };
    const cron = fieldValue("cron", plan.cron);
    if (!cron) {
      showToast("请填写计划时间 Cron 表达式", 1400);
      return;
    }
    updateCoverStudioSchedulePlan(planId, {
      cron,
      template: nextTemplate
    });
    elements.coverStudioScheduleEditSave.disabled = true;
    const saved = await saveCoverStudioSchedules({ feedback: "计划已保存" });
    elements.coverStudioScheduleEditSave.disabled = false;
    if (saved) {
      closeCoverStudioScheduleEditor();
    }
  });
  elements.aiTestBtn?.addEventListener("click", testAiConfig);
  [
    elements.drive115Enabled,
    elements.drive115Cookie,
    elements.drive115DefaultCid
  ].forEach((input) => {
    input?.addEventListener("input", () => {
      updateDrive115Feedback();
      updateDrive115Completion();
    });
    input?.addEventListener("change", () => {
      updateDrive115Feedback();
      updateDrive115Completion();
    });
  });
  elements.drive115SaveBtn?.addEventListener("click", saveDrive115Config);
  elements.drive115TestBtn?.addEventListener("click", testDrive115Config);
  elements.drive115QrStartBtn?.addEventListener("click", startDrive115QrLogin);
  elements.drive115QrStopBtn?.addEventListener("click", () => {
    stopDrive115QrPolling({ clearSession: true, notifyServer: true });
    setDrive115QrStatus("已停止二维码轮询。", "stopped");
  });
  elements.drive115LoginCollapseBtn?.addEventListener("click", () => {
    const collapsed = !elements.drive115LoginBody?.hidden;
    if (elements.drive115LoginBody) {
      elements.drive115LoginBody.hidden = collapsed;
    }
    if (elements.drive115LoginCollapseBtn) {
      elements.drive115LoginCollapseBtn.setAttribute("aria-expanded", collapsed ? "false" : "true");
      elements.drive115LoginCollapseBtn.textContent = collapsed ? "展开⌄" : "收起⌄";
    }
  });
  elements.drive115ParseBtn?.addEventListener("click", parseDrive115Link);
  elements.drive115TransferBtn?.addEventListener("click", transferDrive115Link);
  elements.hdhiveSaveBtn?.addEventListener("click", saveHDHiveConfig);
  elements.hdhiveTestBtn?.addEventListener("click", testHDHive);
  elements.hdhiveAuthorizeBtn?.addEventListener("click", authorizeHDHive);
  elements.hdhiveRefreshBtn?.addEventListener("click", refreshHDHiveStatus);
  elements.hdhiveDisconnectBtn?.addEventListener("click", disconnectHDHive);
  elements.hdhiveCheckinBtn?.addEventListener("click", checkinHDHive);
  elements.hdhiveAuthMode?.addEventListener("change", () => {
    appState.hdhiveConfig = normalizeHDHiveConfig({ ...appState.hdhiveConfig, authMode: elements.hdhiveAuthMode?.value });
    renderHDHiveConfig();
  });
  elements.hdhiveSearchBtn?.addEventListener("click", searchHDHive);
  elements.hdhiveOnly115?.addEventListener("change", renderHDHiveResults);
  elements.hdhiveSearchKeyword?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") searchHDHive();
  });
  elements.hdhiveSearchResults?.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest("[data-hdhive-transfer]") : null;
    if (button) transferHDHiveResource(String(button.getAttribute("data-hdhive-transfer") || ""));
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
  elements.notifyRoutesSave?.addEventListener("click", saveNotificationRoutesCard);
  elements.notifyTemplatesSave?.addEventListener("click", saveNotificationTemplateCenter);
  elements.notifyChannelAddToggle?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleNotificationChannelMenu();
  });
  elements.notifyChannelEmptyAdd?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleNotificationChannelMenu();
  });
  elements.notifyChannelAddMenu?.addEventListener("click", (event) => {
    const trigger = event.target instanceof HTMLElement ? event.target.closest("[data-notify-channel-add]") : null;
    if (!(trigger instanceof HTMLElement)) {
      return;
    }
    addNotificationChannel(String(trigger.dataset.notifyChannelAdd || ""));
  });
  elements.notifyChannelCardList?.addEventListener("click", (event) => {
    const target = event.target instanceof HTMLElement ? event.target : null;
    if (!target) {
      return;
    }
    const removeTrigger = target.closest("[data-notify-channel-remove]");
    if (removeTrigger instanceof HTMLElement) {
      removeNotificationChannel(String(removeTrigger.dataset.notifyChannelRemove || ""));
      return;
    }
    const openTrigger = target.closest("[data-notify-channel-open]");
    if (openTrigger instanceof HTMLElement) {
      openNotificationChannelModal(String(openTrigger.dataset.notifyChannelOpen || ""));
    }
  });
  elements.notifyChannelModalClose?.addEventListener("click", () => {
    closeNotificationChannelModal();
  });
  elements.notifyChannelModal?.addEventListener("click", (event) => {
    const target = event.target instanceof HTMLElement ? event.target : null;
    const sectionTrigger = target?.closest("[data-notify-modal-section-target]");
    if (sectionTrigger instanceof HTMLElement) {
      event.preventDefault();
      appState.notificationModalSection = String(sectionTrigger.dataset.notifyModalSectionTarget || "channel");
      renderNotificationChannelModal(readBotConfigFromInputs());
      return;
    }
    if (event.target === elements.notifyChannelModal) {
      closeNotificationChannelModal();
    }
  });
  elements.notifyChannelModalSave?.addEventListener("click", async () => {
    if (!appState.notificationChannelModalChannel) {
      return;
    }
    await saveNotificationChannel(appState.notificationChannelModalChannel);
  });
  elements.notifyChannelModalTest?.addEventListener("click", () => {
    if (!appState.notificationChannelModalChannel) {
      return;
    }
    sendBotTest(appState.notificationChannelModalChannel);
  });
  [elements.notifyPlaybackUserScopeAll, elements.notifyPlaybackUserScopeSelected].forEach((input) => {
    input?.addEventListener("change", () => {
      renderNotificationPlaybackUserScope(readBotConfigFromInputs());
    });
  });
  elements.notifyPlaybackUsersList?.addEventListener("change", (event) => {
    const target = event.target;
    if (target instanceof HTMLInputElement && target.matches("[data-notify-playback-user]")) {
      renderNotificationPlaybackUserScope(readBotConfigFromInputs());
    }
  });
  elements.notifyPlaybackUsersRefresh?.addEventListener("click", () => {
    loadNotificationPlaybackUsers({ silent: false });
  });
  [elements.notifyRouteChannelTabs, elements.notifyTemplateChannelTabs].forEach((tabs) => {
    tabs?.addEventListener("click", (event) => {
      const target = event.target instanceof HTMLElement ? event.target.closest("[data-notify-workspace-channel]") : null;
      if (target instanceof HTMLElement) setNotificationWorkspaceChannel(String(target.dataset.notifyWorkspaceChannel || ""));
    });
  });
  elements.notifyTemplateEventList?.addEventListener("click", (event) => {
    const target = event.target instanceof HTMLElement ? event.target.closest("[data-notify-workspace-event]") : null;
    if (target instanceof HTMLElement) setNotificationWorkspaceEvent(String(target.dataset.notifyWorkspaceEvent || ""));
  });
  elements.botTemplateReset?.addEventListener("click", () => {
    const events = getNotificationEventDefinitions();
    events.forEach((eventDef) => {
      ["telegram", "wecom"].forEach((channel) => {
        const textarea = document.querySelector(
          `[data-notify-template-channel="${CSS.escape(channel)}"][data-event-key="${CSS.escape(eventDef.key)}"]`
        );
        if (textarea instanceof HTMLTextAreaElement) {
          textarea.value = getNotificationDefaultTemplate(channel, eventDef.key);
        }
      });
    });
    refreshAllNotificationTemplatePreviews();
    if (elements.botFeedback) {
      elements.botFeedback.textContent = "已恢复所有默认模板，点击“保存”后生效。";
      elements.botFeedback.classList.add("feedback-success");
    }
  });
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

  document.addEventListener("click", (event) => {
    if (!appState.notificationChannelMenuOpen) {
      return;
    }
    const target = event.target instanceof HTMLElement ? event.target : null;
    if (!target) {
      return;
    }
    const insideToolbar = target.closest("#notify-channel-toolbar");
    if (insideToolbar) {
      return;
    }
    appState.notificationChannelMenuOpen = false;
    renderNotificationChannelAddMenu(readBotConfigFromInputs());
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

  elements.profileOpenLogs?.addEventListener("click", () => {
    openProjectLogModal();
  });

  elements.profileOpenSupport?.addEventListener("click", () => {
    switchView("about-support");
  });

  document.querySelector(".sidebar-exit")?.addEventListener("click", async (event) => {
    event.preventDefault();
    if (appState.authEnabled) {
      await triggerAdminLogout();
    }
  });

  elements.projectLogClose?.addEventListener("click", () => {
    closeProjectLogModal();
  });

  elements.projectLogModal?.addEventListener("click", (event) => {
    if (event.target === elements.projectLogModal) {
      closeProjectLogModal();
    }
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
      closeProjectLogModal();
      closeUserCenterInviteModal();
      closeUserCenterInviteManageModal();
      closeUserCenterInviteResultModal();
      closeCreateUserModal();
      closeUserConfigModal();
    }
  });
}

function hydrateInputs() {
  syncActiveMediaServerFields();
  const activeConfig = getMediaServerConfig();
  elements.serverUrl.value = activeConfig.serverUrl.replace(/\/emby$/i, "");
  elements.apiKey.value = activeConfig.apiKey;
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
  renderMediaServerSelector();
  refreshTmdbUiState();
  renderAiSettings();
  renderCoverStudioSettings();
  renderLibraryDirectorySettings();
  renderEnvControlledState();
}

initEvents();
hydrateInputs();
renderAdminCredentialForm();
renderConnectionState(false, "尚未连接 Emby。你可以先填地址和 API Key。");
const initialParams = new URLSearchParams(window.location.search || "");
if (initialParams.get("hdhive") === "authorized") {
  appState.activeView = "hdhive";
  localStorage.setItem(STORAGE_KEYS.activeView, "hdhive");
  window.history.replaceState({}, document.title, window.location.pathname);
}
const initialView = VIEW_META[appState.activeView] ? appState.activeView : "overview";
switchView(initialView);
renderAll();

async function startPostAuthBootstrap() {
  if (postAuthBootstrapPromise) {
    return postAuthBootstrapPromise;
  }
  postAuthBootstrapPromise = (async () => {
    if (shouldUseLocalProxy()) {
      setTimeout(() => {
        refreshInviteSyncStatus({ silent: true });
        loadBotConfigFromServer({ silent: true });
        loadAiConfigFromServer({ silent: true });
        loadCoverStudioConfigFromServer({ silent: true });
        loadDrive115ConfigFromServer({ silent: true });
        loadHDHiveConfigFromServer({ silent: true });
        refreshBotWebhookInfo({ silent: true });
        ensureBotWebhookStatusPolling();
      }, 80);
    }
    if (appState.config.serverUrl && appState.config.apiKey) {
      await loadEmbyData();
      await loadCoverStudioViews({ silent: true });
    }
  })();
  return postAuthBootstrapPromise;
}

(async () => {
  const ready = await bootstrapAdminAuth();
  if (ready) {
    await startPostAuthBootstrap();
  }
})();

/**
 * Adaptive responsive safety enhancer.
 * - Does NOT modify existing HTML structure/IDs/classes.
 * - Does NOT replace existing handlers/logic.
 * - Adds adaptive drawer navigation for compact/mobile widths.
 */
(function initAdaptiveResponsiveSafetyEnhancer() {
  const FORCE_DESKTOP_LAYOUT = false;
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
    syncSidebarCollapseState();
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
      return sessionHasPlayableItem(session);
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
  window.addEventListener("vistamirror:sessions-updated", renderLiveStatus);
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
    pendingForceRefresh: false,
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

  function renderUserViews(views, emptyMessage = "未读取到可展示的 Emby 媒体库视图。") {
    const grid = document.getElementById("my-library-grid");
    if (!grid) {
      return;
    }
    if (!Array.isArray(views) || !views.length) {
      grid.innerHTML = `<div class="my-library-empty">${escapeHtml(emptyMessage)}</div>`;
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
    const selectedUserId = String(appState?.selectedUserId || "").trim();

    if (selectedUserId) {
      try {
        const result = await embyFetch(`/Users/${encodeURIComponent(selectedUserId)}/Views`);
        const normalized = normalizeUserViews(result);
        if (normalized.length > 0) {
          return normalized;
        }
      } catch (error) {
        lastError = error;
      }
    }

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

    try {
      const libraries = await fetchAllLibraries();
      const normalizedLibraries = Array.isArray(libraries)
        ? libraries.map((item) => ({
          id: String(item?.id || "").trim(),
          name: String(item?.name || "").trim(),
          collectionType: normalizeCollectionType(item?.collectionType),
          type: "CollectionFolder",
          imageTags: {},
          backdropImageTags: [],
          recursiveItemCount: Number(item?.recursiveItemCount),
          childCount: Number(item?.childCount),
          itemCount: Number(item?.itemCount)
        })).filter((item) => item.id && item.name)
        : [];
      if (normalizedLibraries.length > 0) {
        return normalizedLibraries;
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
      if (force) {
        STATE.pendingForceRefresh = true;
      }
      return;
    }
    if (!force && now - STATE.lastSyncAt < LIBRARY_SYNC_INTERVAL_MS) {
      return;
    }
    if (!isAdminReady() || !appState?.config?.serverUrl || !appState?.config?.apiKey) {
      STATE.views = [];
      renderUserViews([]);
      return;
    }

    STATE.loading = true;
    try {
      const views = await fetchUserViews();
      STATE.views = views;
      renderUserViews(views, "已连接 Emby，但未读取到媒体库视图，请检查 API Key 权限或媒体库访问权限。");
      STATE.lastSyncAt = now;
    } catch (error) {
      renderUserViewsError(error?.message || "未知错误");
    } finally {
      STATE.loading = false;
      if (STATE.pendingForceRefresh) {
        STATE.pendingForceRefresh = false;
        window.setTimeout(() => refreshLibraryData(true), 0);
      }
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
    if (activeView === "overview" && isAdminReady()) {
      ensureAndRefresh(false);
    }
  }, LIBRARY_SYNC_INTERVAL_MS);

  window.addEventListener("vistamirror:auth-ready", () => {
    const activeView = document.querySelector(".main-content")?.dataset?.activeView || "";
    if (activeView === "overview") {
      ensureAndRefresh(true);
    }
  });

  window.addEventListener("vistamirror:emby-data-ready", () => {
    const activeView = document.querySelector(".main-content")?.dataset?.activeView || "";
    if (activeView === "overview") {
      ensureAndRefresh(true);
    }
  });
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
        <div class="radar-live-empty"><div><strong>当前暂无播放</strong></div></div>
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
      return sessionHasPlayableItem(session);
    });
  }

  function getSharedPlayableSessions() {
    return normalizeSessions(appState?.sessions);
  }

  function renderEmpty() {
    if (!els.list) {
      return;
    }
    els.list.innerHTML = `<div class="radar-live-empty"><div><strong>当前暂无播放</strong></div></div>`;
  }

  function renderPopover() {
    const sessions = state.sessions.length > 0 ? state.sessions : getSharedPlayableSessions();
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
      state.sessions = getSharedPlayableSessions();
      placePopover();
      renderPopover();
    }
  }

  function tick() {
    if (!isAdminReady()) {
      state.sessions = [];
      renderPopover();
      return;
    }
    ensureLiveSessionsPolling();
    state.sessions = getSharedPlayableSessions();
    if (!state.sessions.length) {
      state.sessions = getSharedPlayableSessions();
    }
    renderPopover();
    if (state.open) {
      placePopover();
    }
  }

  noticeBtn.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation?.();
    setOpen(!state.open);
  }, { capture: true });

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

  function startTicker() {
    if (state.timerId || !isAdminReady()) {
      return;
    }
    tick();
    state.timerId = setInterval(tick, 1000);
  }

  function stopTicker() {
    if (!state.timerId) {
      return;
    }
    clearInterval(state.timerId);
    state.timerId = null;
    state.sessions = [];
    renderPopover();
  }

  window.addEventListener("vistamirror:auth-ready", startTicker);
  window.addEventListener("vistamirror:auth-lock", stopTicker);
  window.addEventListener("vistamirror:sessions-updated", () => {
    state.sessions = getSharedPlayableSessions();
    renderPopover();
    if (state.open) {
      placePopover();
    }
  });
  startTicker();
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
    requireAdminReadyForRequest("/api/emby/ScheduledTasks");
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
      refs.groupsHost.innerHTML = `<div class="task-center-emby-empty">请先在“媒体库配置”中连接 Emby 服务器。</div>`;
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
    if (!isAdminReady() || !isEmbyConnected()) {
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
    if (state.timerId || !isAdminReady()) {
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
    if (!isAdminReady()) {
      stopPolling();
      state.activeView = "";
      return;
    }
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

  window.addEventListener("vistamirror:auth-ready", onViewChanged);
  window.addEventListener("vistamirror:auth-lock", stopPolling);
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
    requireAdminReadyForRequest("/api/ranking/annual");
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
        refs.top3Host.innerHTML = `<div class="annual-ranking-empty">请先在媒体库配置中连接 Emby。</div>`;
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
    if (!isAdminReady() || !isConnected()) {
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
    if (state.timerId || !isAdminReady()) {
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
    if (!isAdminReady()) {
      stopPolling();
      state.activeView = "";
      return;
    }
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
    onViewChange();
  }

  window.addEventListener("vistamirror:auth-ready", onViewChange);
  window.addEventListener("vistamirror:auth-lock", stopPolling);
  init();
})();
