
let currentJobId = null;
let pollTimer = null;
let jobsTimer = null;
let syncFeedbackTimer = null;
let completionTitleFlashTimer = null;
let completionTitleFlashText = '';
const defaultDocumentTitle = document.title;
let jobsCache = [];
let currentJobSnapshot = null;
let currentLogsCache = [];
let currentJobIdByMode = { seeding: null, booking: null, scan: null };
let currentProjectJobId = null;
let currentProjectModeFilter = 'all';
let currentProjectStatusFilter = 'all';
let currentSettingsCache = {};
let currentRunMode = 'seeding';
let currentMappingBlocksByMode = {};
let currentRunFlagsByMode = {};
let captureFivePerLink = false;
let sheetNameSuggestTimer = null;
let sheetNameSuggestKey = '';
const SHEET_NAME_CACHE_TTL_MS = 3 * 60 * 1000;
let sheetNameSuggestCache = {};
let sheetNameSuggestInflight = {};
const SHEET_COLUMN_CACHE_TTL_MS = 3 * 60 * 1000;
let sheetColumnSuggestTimer = null;
let sheetLinkSummaryTimer = null;
let sheetColumnSuggestKey = '';
let sheetColumnSuggestCache = {};
let sheetColumnSuggestInflight = {};
let currentSheetLinkColumns = [];
let sheetLinkSuggestPayloadByMode = {
  seeding: { columns: [], counts: {} },
  booking: { columns: [], counts: {} },
  scan: { columns: [], counts: {} },
};
let sheetLinkSuggestSourceKeyByMode = { seeding: '', booking: '', scan: '' };
let activeSheetColumnTarget = null;
let bulkSheetLinkSelectionMode = false;
let bulkSheetLinkSelectionsByMode = { seeding: [], booking: [], scan: [] };
let sheetLinkSuggestLoadedByMode = { seeding: false, booking: false, scan: false };
let sheetLinkSuggestLoadingByMode = { seeding: false, booking: false, scan: false };
let monitorIssueExpandState = { failed: false, unavailable: false };
let monitorIssueExpandJobId = '';
let pendingMappingScrollMode = '';
let pendingMappingHighlightIndex = -1;
let currentAccessPolicy = { allowed_emails: [], admin_emails: [], managed_emails: [], email_types: {}, updated_at: null };
let currentMailConfig = { sender_email: '', from_email: '', has_password: false, updated_at: null, source: 'env' };
let currentActivityEvents = [];
let accessDirectoryQuery = '';
let accessDirectoryRole = 'all';
let accessDirectoryScope = 'all';
let accessDirectoryType = 'all';
let accessMailEditorOpen = false;
let accessEntryEditorState = { open: false, originalEmail: '', email: '', role: 'user', type: 'internal' };
let jobStatusMemory = {};
let notifiedCompletedJobKeys = new Set();
const BROWSER_PORT_BY_MODE = { seeding: 9223, booking: 9423, scan: 9623 };
const DEFAULT_AUTO_LAUNCH_CHROME = true;
let currentLang = localStorage.getItem('ui_lang') || 'vi';
let currentTheme = localStorage.getItem('ui_theme') || 'light';
const authState = {
  email: '__AUTH_EMAIL__',
  role: '__AUTH_ROLE__',
  isAdmin: __AUTH_IS_ADMIN__,
};
const LOCAL_BROWSER_HOSTS = new Set(__LOCAL_BROWSER_HOSTS__);
const localAgentState = {
  origin: localStorage.getItem('toolEvidence.localAgentOrigin') || 'http://127.0.0.1:8765',
  enabled: false,
  checked: false,
  lastError: '',
};
const LOCAL_AGENT_RUNTIME_PREFIXES = [
  '/api/settings',
  '/api/sheets/names',
  '/api/sheets/column-suggestions',
  '/api/activity',
  '/api/chrome/',
  '/api/jobs',
];

function isConfiguredLocalBrowserHost(host) {
  return LOCAL_BROWSER_HOSTS.has(String(host || '').trim().toLowerCase());
}

function isLocalBrowserOrigin() {
  return isConfiguredLocalBrowserHost(window.location.hostname);
}

function isLocalAgentRuntimePath(url) {
  const path = String(url || '').trim();
  if (!path) return false;
  const lowered = path.toLowerCase();
  if (lowered.startsWith('http://') || lowered.startsWith('https://')) return false;
  return LOCAL_AGENT_RUNTIME_PREFIXES.some(prefix => path === prefix || path.startsWith(prefix));
}

function shouldUseLocalAgent(url) {
  return !isLocalBrowserOrigin() && !!localAgentState.enabled && isLocalAgentRuntimePath(url);
}

function runtimeHref(url) {
  return shouldUseLocalAgent(url) ? `${localAgentState.origin}${url}` : url;
}

async function detectLocalAgent() {
  if (isLocalBrowserOrigin()) {
    localAgentState.enabled = false;
    localAgentState.checked = true;
    localAgentState.lastError = '';
    return false;
  }
  try {
    const res = await fetch(`${localAgentState.origin}/health`, {
      method: 'GET',
      cache: 'no-store',
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data?.ok) throw new Error(data.detail || ('HTTP ' + res.status));
    localAgentState.enabled = true;
    localAgentState.checked = true;
    localAgentState.lastError = '';
    return true;
  } catch (e) {
    localAgentState.enabled = false;
    localAgentState.checked = true;
    localAgentState.lastError = String(e?.message || e || 'Local agent unavailable');
    return false;
  }
}

async function agentReq(url, opts = {}) {
  if (!authState.email) throw new Error('Thiếu email đăng nhập để gọi local agent');
  const headers = {
    'Content-Type': 'application/json',
    'X-Tool-Evidence-User': authState.email,
    ...(opts.headers || {}),
  };
  let res = null;
  try {
    res = await fetch(`${localAgentState.origin}${url}`, { ...opts, headers });
  } catch (e) {
    localAgentState.enabled = false;
    localAgentState.lastError = String(e?.message || e || 'Local agent unavailable');
    throw new Error('Không kết nối được local agent trên máy này');
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

window.toolEvidenceSetLocalAgentOrigin = function(origin) {
  let raw = String(origin || '').trim();
  while (raw.endsWith('/')) raw = raw.slice(0, -1);
  if (!raw) {
    localStorage.removeItem('toolEvidence.localAgentOrigin');
    localAgentState.origin = 'http://127.0.0.1:8765';
    return localAgentState.origin;
  }
  localStorage.setItem('toolEvidence.localAgentOrigin', raw);
  localAgentState.origin = raw;
  return raw;
};

const I18N = {
  vi: {
    searchPlaceholder: 'Tìm job hoặc trạng thái...',
    launchChrome: 'Mở Chrome',
    refresh: 'Làm mới',
    light: 'Sáng',
    dark: 'Tối',
    logout: 'Đăng xuất',
    roleAdmin: 'Admin',
    roleUser: 'User',
    adminOnly: 'Chỉ admin mới dùng được phần này',
    overview: 'Tổng quan',
    runs: 'Chạy tác vụ',
    projects: 'Dự án',
    tasks: 'Tác vụ',
    activities: 'Hoạt động',
    access: 'Quản lý người dùng',
    settings: 'Cài đặt',
    state: 'Trạng thái',
    readyState: 'Sẵn sàng',
    openRuns: 'Mở Runs',
    view: 'Xem',
    sync: 'Đồng bộ',
    syncing: 'Đang đồng bộ',
    synced: 'Đã đồng bộ',
    syncFailed: 'Lỗi',
    goToRuns: 'Mở Run Center',
    selectedJob: 'Job đang chọn',
    storedJobs: 'Job đã lưu',
    successFailed: 'Thành công / Lỗi',
    overallProgress: 'Tiến độ tổng',
    overviewModeSplit: 'Tỉ lệ theo mode',
    overviewModeSplitSub: 'Phân bổ job đang theo dõi theo từng mode.',
    overviewModeShareFmt: (count, pct) => `${count} job · ${pct}%`,
    overviewModeSplitEmpty: 'Chưa có dữ liệu mode để thống kê.',
    overviewGreetingLabel: 'Lời chào hôm nay',
    overviewGreetingMorning: 'Chào buổi sáng',
    overviewGreetingAfternoon: 'Chào buổi chiều',
    overviewGreetingEvening: 'Chào buổi tối',
    overviewGreetingFallbackName: 'bạn',
    overviewGreetingSub: 'Tiếp tục theo dõi job và giữ nhịp công việc hôm nay.',
    jobsToday: 'Tổng số job hôm nay',
    avgSuccess: 'Tỉ lệ success trung bình',
    latestJob: 'Job chạy gần nhất',
    topError: 'Top lỗi gặp nhiều nhất',
    overviewTimeline: 'Kết quả theo ngày',
    overviewTimelineEmpty: 'Chưa có lịch sử chạy theo ngày',
    overviewDateFmt: label => `Ngày ${label}`,
    overviewTimelineJobsBadgeFmt: count => `${count} job`,
    overviewTimelineSuccessBadgeFmt: count => `${count} ok`,
    overviewTimelineFailedBadgeFmt: count => `${count} lỗi`,
    overviewTimelineUnavailableBadgeFmt: count => `${count} không khả dụng`,
    overviewCompletedLegend: 'Hoàn thành',
    overviewFailedLegend: 'Lỗi',
    overviewUnavailableLegend: 'Không khả dụng',
    createdLast24h: 'được tạo trong 24h gần nhất',
    acrossTracked: 'trên toàn bộ job đã theo dõi',
    noRecentRun: 'chưa có job gần đây',
    noRecurring: 'chưa có lỗi lặp lại',
    runSummary: 'Tóm tắt job',
    overviewClean: 'Overview chỉ để xem số liệu. Khu chạy nằm ở tab Runs.',
    runConfig: 'Cấu hình chạy',
    runConfigHelp: 'Chia sẻ quyền Editor cho Sheet và Drive trước khi chạy.',
    runShareLabel: 'Chia sẻ Sheet & Drive folder cho (quyền Editor):',
    overwriteRunHelp: 'Luôn chạy lại và ghi đè kết quả cũ.',
    highlightSheetErrors: 'Tô màu lỗi trên sheet',
    highlightSheetErrorsHelp: 'Khi row chạy xong, tô màu ngay các ô kết quả trên Sheet: trắng cho Thành công, vàng cho Không khả dụng, đỏ cho Lỗi.',
    scanNegativeFilter: 'Lọc từ ngữ tiêu cực',
    scanNegativeFilterHelp: 'Khi bật, Scan sẽ đánh dấu lỗi nếu text hoặc OCR chứa các từ tiêu cực đã cấu hình trong Cài đặt.',
    runMode: 'Chế độ chạy',
    columnMapping: 'Column Mapping',
    seeding: 'Seeding',
    booking: 'Booking',
    scan: 'Scan',
    runModeSeedingHelp: 'Seeding dùng luồng chụp và upload ảnh tiêu chuẩn cho bài đăng.',
    runModeBookingHelp: 'Booking phù hợp cho job cần multi-capture và theo dõi lịch booking.',
    runModeScanHelp: 'Scan bỏ qua Chrome nếu chỉ quét dữ liệu và dùng bộ cột scan mặc định.',
    addBlock: '+ Thêm Block',
    captureFive: 'Chụp 5 tấm / 1 link',
    captureFiveHelp: 'Bật để mỗi link chụp đủ 5 ảnh và giữ nhịp booking ổn định.',
    chrome: 'Chrome',
    postName: 'Tên Post',
    textColumn: 'Text Column',
    imageColumn: 'Image Column',
    resultColumn: 'Result Column',
    profileColumn: 'Profile',
    contentColumn: 'Content',
    linkUrl: 'Link URL',
    driveUrl: 'Drive URL',
    screenshotColumn: 'Screenshot',
    airDate: 'Air Date',
    sheetUrl: 'Sheet URL',
    sheetName: 'Tên Sheet',
    driveFolder: 'Drive Folder ID',
    sheetUrlHintLoading: 'Đang tải danh sách sheet...',
    sheetUrlHintEmpty: 'Không tìm thấy sheet nào trong file này',
    sheetUrlHintCountFmt: count => `Tìm thấy ${count} sheet`,
    sheetNameInvalidFmt: name => `Không tìm thấy sheet: ${name}`,
    sheetLinkCellHintLoading: 'Đang quét ô có URL...',
    sheetLinkCellHintCountFmt: count => `Phát hiện ${count} cột có URL`,
    sheetLinkSuggestTitle: 'Cột có link',
    sheetLinkSuggestSheetFmt: sheet => `Sheet: ${sheet}`,
    sheetLinkSuggestFind: 'Tìm cột',
    sheetLinkSuggestLoading: 'Đang quét cột có link...',
    sheetLinkSuggestHelp: 'Bấm vào ô cột liên quan đến link trước, rồi chọn một cột bên dưới.',
    sheetLinkSuggestReady: 'Bấm Quét nhanh để quét các cột đang chứa link trong sheet này.',
    sheetLinkSuggestNeedSheet: 'Chọn đúng Sheet Name để hệ thống có thể quét cột link.',
    sheetLinkSuggestActiveFmt: (field, block) => `Đang chọn cho ${field}${block ? ` · ${block}` : ''}`,
    sheetLinkSuggestEmpty: 'Chưa phát hiện cột nào có link trong sheet này.',
    sheetLinkSuggestCountFmt: count => `Phát hiện ${count} cột có link`,
    sheetLinkBulkToggle: 'Chọn nhiều cột',
    sheetLinkBulkAdd: 'Tạo block',
    sheetLinkQuickScan: 'Quét nhanh',
    sheetLinkQuickCreate: 'Tạo cột nhanh',
    sheetLinkBulkClear: 'Bỏ chọn',
    sheetLinkReload: 'Load lại',
    sheetLinkBulkSelectedFmt: count => `Đã chọn ${count} cột`,
    sheetLinkBulkHelp: 'Bật chọn nhiều để bấm nhiều cột link và tự thêm block.',
    sheetLinkBulkUnsupported: 'Chọn nhiều cột hiện hỗ trợ cho Seeding và Booking.',
    sheetLinkBulkNone: 'Chưa chọn cột nào',
    sheetLinkBulkAddedFmt: count => `Đã tạo ${count} block và thêm 2 cột mới cho mỗi cột đã chọn`,
    sheetLinkBulkNoNew: 'Các cột đã chọn đã có block rồi',
    sheetLinkQuickScanNoSelection: 'Chưa chọn cột nào để quét nhanh',
    sheetLinkQuickScanDoneFmt: count => `Đã tạo ${count} block quét nhanh và thêm 2 cột mới cho mỗi cột đã chọn`,
    browserPort: 'Browser Port',
    startLine: 'Dòng bắt đầu',
    autoLaunchChrome: 'Tự mở Chrome',
    startJob: 'Chạy job',
    overwriteRun: 'Chạy đè',
    pauseJob: 'Tạm dừng',
    stopJob: 'Dừng',
    resumeJob: 'Tiếp tục',
    continueJob: 'Chạy tiếp',
    errorOnlyJob: 'Chạy lỗi',
    refreshJobs: 'Làm mới job',
    runQueue: 'Hàng đợi job',
    runQueueHelp: 'Chọn job để theo dõi. Mỗi mode được chạy 1 job cùng lúc.',
    liveLogs: 'Live log',
    errorRows: 'Lỗi',
    selectedJobMeta: 'Job đang chọn',
    monitorKicker: '4. Kết quả & Theo dõi',
    monitorTitle: 'Theo dõi tiến độ và lỗi',
    monitorJob: 'Job',
    monitorProgress: 'Tiến độ',
    monitorErrors: 'Thống kê',
    monitorTable: 'Bảng log xử lý',
    monitorNoJob: 'Chưa chọn job',
    monitorNoErrors: 'Không có lỗi',
    monitorIssueSummaryLabel: 'Tóm tắt',
    monitorIssueRowsLabel: 'Lỗi',
    monitorIssueUnavailableRowsLabel: 'Không khả dụng',
    monitorIssueExpandFmt: count => `+${count}`,
    monitorIssueCollapse: 'Thu gọn',
    monitorIssueStatsLabel: 'Thống kê',
    monitorIssueSummaryNone: 'Không có lỗi cần tổng hợp',
    monitorIssueSummaryTopFmt: (label, count) => `Chủ yếu: ${label} (${count})`,
    monitorIssueSummaryTopMoreFmt: (label, count, more) => `Chủ yếu: ${label} (${count}) · +${more} loại lỗi khác`,
    jobFinishedTitle: 'Hoàn tất',
    jobFinishedToastFmt: (name, done, total) => `${name} đã chạy xong ${done}/${total} dòng.`,
    jobFinishedBannerTitle: 'Dự án đã hoàn tất',
    jobFinishedBannerDismiss: 'Đã thấy',
    monitorNoLogs: 'Chưa có dữ liệu',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Thành công ${ok} · Lỗi ${fail} · Không khả dụng ${unavailable}`,
    monitorIssueCellCountFmt: count => `${count} ô`,
    unavailableLabel: 'Không khả dụng',
    time: 'Time',
    post: 'Post',
    result: 'Kết quả',
    message: 'Thông điệp',
    replay: 'Replay',
    exportLog: 'Xuất log Excel',
    noLogsToExport: 'Chưa có log để xuất',
    replayStartedFmt: row => `Đã tạo replay cho dòng ${row}`,
    continueStarted: 'Đã tạo job chạy tiếp',
    errorOnlyStarted: 'Đã tạo job chạy lại các dòng lỗi',
    noData: 'Chưa có dữ liệu',
    projectsState: 'Lưu các run hoàn tất và xem lại chi tiết',
    groupedProjects: 'Dự án đã lưu',
    completedGroups: 'Sheet đã lưu',
    largestGroup: 'Dự án đang chọn',
    groupedRegistry: 'Thư viện dự án',
    groupSnapshot: 'Chi tiết dự án',
    projectLogs: 'Log dự án',
    projectLogsSub: 'Lưu theo dự án đang chọn',
    projectModeLabel: 'Mode',
    projectStatusLabel: 'Trạng thái',
    allProjects: 'Tất cả',
    projectStatusAll: 'Tất cả',
    projectStatusRunning: 'Đang chạy',
    projectStatusCompleted: 'Hoàn tất',
    projectStatusStopped: 'Đã dừng',
    projectStatusFailed: 'Lỗi',
    projectOwner: 'Người chạy',
    noProjectsInFilter: 'Chưa có dự án trong nhóm này',
    projectNoLogs: 'Chưa có log cho dự án này',
    tasksState: 'Phân rã khối lượng xử lý',
    done: 'Hoàn thành',
    pending: 'Chờ xử lý',
    success: 'Thành công',
    failed: 'Lỗi',
    rowsProcessed: 'số dòng đã xử lý',
    rowsRemaining: 'số dòng còn lại',
    rowsPassed: 'số dòng thành công',
    rowsNeedRetry: 'số dòng cần chạy lại',
    taskDistribution: 'Phân bố tác vụ',
    progressOverTime: 'Tiến độ theo thời gian',
    errorQueue: 'Hàng đợi lỗi',
    currentProgress: 'Tiến độ hiện tại',
    activitiesState: 'Dòng thời gian runtime có phân loại',
    recentTimeline: 'Lịch sử hoạt động',
    activityLevel: 'Hoạt động',
    accessState: 'Admin quản lý người dùng được đăng nhập và mail admin',
    accessMailTitle: 'Mail gửi OTP',
    accessMailHelp: 'Đổi Gmail gửi mã xác nhận ngay trên giao diện admin. App password cũ sẽ được giữ kín và chỉ thay khi bạn nhập mới.',
    accessMailSenderLabel: 'Gmail gửi OTP',
    accessMailFromLabel: 'From email',
    accessMailPasswordLabel: 'App password mới',
    accessMailSave: 'Lưu mail OTP',
    accessMailEdit: 'Chỉnh sửa',
    accessMailHide: 'Ẩn',
    accessMailCurrentFmt: email => `Đang dùng: ${email || 'Chưa cấu hình'}`,
    accessMailPasswordSaved: 'Đã có app password',
    accessMailPasswordMissing: 'Chưa có app password',
    accessMailSourceEnv: 'Đang lấy từ .env',
    accessMailSourceFile: 'Đang lấy từ giao diện',
    accessMailSaved: 'Đã lưu mail gửi OTP',
    accessMailReloaded: 'Đã tải lại cấu hình mail OTP',
    accessEntryTitle: 'Chỉnh sửa Gmail',
    accessEntryHelp: 'Đổi địa chỉ Gmail hoặc role của dòng đang chọn rồi lưu lại.',
    accessEntryEmailLabel: 'Địa chỉ Gmail',
    accessEntryRoleLabel: 'Role',
    accessEntryTypeLabel: 'Loại',
    accessEntryCurrentFmt: email => `Đang sửa: ${email || '-'}`,
    accessEntrySave: 'Lưu chỉnh sửa',
    accessEntryCancel: 'Hủy',
    accessEntrySaved: 'Đã lưu chỉnh sửa Gmail',
    accessEntryInvalid: 'Nhập đúng địa chỉ Gmail hợp lệ',
    accessDirectoryTitle: 'Danh sách người dùng',
    accessDirectoryHelp: 'Lọc nhanh mail theo quyền, trạng thái truy cập và chỉnh role trực tiếp trên từng dòng.',
    accessDirectorySearchPlaceholder: 'Tìm Gmail hoặc trạng thái...',
    accessQuickAdd: '+ Thêm Gmail',
    accessFilterRole: 'Role',
    accessFilterScope: 'Truy cập',
    accessFilterType: 'Loại',
    accessFilterAll: 'Tất cả',
    accessFilterAdmin: 'Admin',
    accessFilterUser: 'User',
    accessFilterInternal: 'Nội bộ',
    accessFilterExternal: 'Ngoại bộ',
    accessYouTag: 'You',
    accessScopeAllowed: 'Được phép',
    accessScopeAdmin: 'Admin',
    accessScopeOpen: 'OTP',
    accessTableEmail: 'Gmail',
    accessTableAccess: 'Truy cập',
    accessTableRole: 'Quyền',
    accessTableType: 'Loại',
    accessTableStatus: 'Trạng thái',
    accessTableUpdated: 'Cập nhật',
    accessTableActions: 'Thao tác',
    accessDirectoryNoMatch: 'Không có mail nào khớp bộ lọc hiện tại',
    accessOpenEntryTitle: 'Cấu hình OTP',
    accessOpenEntrySub: 'Chỉ mail trong danh sách mới được nhập OTP',
    accessOpenEntryMailFmt: email => `Mail gửi OTP: ${email || 'Chưa cấu hình'}`,
    accessAllowedEntrySub: 'Được phép nhập OTP',
    accessAdminEntrySub: 'Giữ quyền quản trị',
    accessStatusActive: 'Đang được phép',
    accessStatusAdmin: 'Toàn quyền quản trị',
    accessStatusOpen: 'OTP giới hạn theo danh sách',
    accessTypeInternal: 'Nội bộ',
    accessTypeExternal: 'Ngoại bộ',
    accessMakeAdmin: 'Lên admin',
    accessMakeUser: 'Hạ user',
    accessRemove: 'Gỡ',
    accessQuickAddInvalid: 'Nhập đúng địa chỉ Gmail để thêm nhanh',
    accessQuickAddDoneFmt: email => `Đã thêm ${email} vào danh sách người dùng`,
    accessSummaryTitle: 'Tóm tắt phân quyền',
    accessSummaryAllowed: 'Danh sách được phép',
    accessSummaryAdmins: 'Danh sách admin',
    accessSummaryUpdated: 'Cập nhật gần nhất',
    accessSummaryCurrentMail: 'Mail đang đăng nhập',
    accessSummaryCurrentRole: 'Role hiện tại',
    accessSummaryOpen: 'Chưa có mail nào trong danh sách',
    accessSummaryEmptyAdmins: 'Chưa có admin nào',
    settingsState: 'Cấu hình đã lưu',
    settingsTitle: 'Thông số screenshot & credentials',
    settingsHelp: 'Các giá trị này sẽ được áp dụng cho các job mới. Bạn cũng có thể dán JSON service account để lưu một lần.',
    accessPolicyTitle: 'Phân quyền truy cập',
    accessPolicyHelp: 'Admin quản lý mail nào được đăng nhập và mail nào có quyền admin.',
    accessAllowedLabel: 'Mail được phép đăng nhập',
    accessAllowedHelp: 'Chỉ mail nằm trong danh sách mới được nhập OTP.',
    accessAdminLabel: 'Mail admin',
    accessAdminHelp: 'Mail admin luôn giữ quyền quản trị và cũng có quyền nhập OTP.',
    saveAccessPolicy: 'Lưu phân quyền',
    reloadAccessPolicy: 'Tải lại phân quyền',
    accessPolicySaved: 'Đã lưu phân quyền',
    accessNotifySentFmt: count => `Đã gửi mail thông báo cho ${count} người dùng`,
    accessNotifyPartialFmt: (sent, failed) => `Đã lưu phân quyền. Gửi mail thành công ${sent}, lỗi ${failed}`,
    accessPolicySelfProtect: 'Không thể tự gỡ quyền admin của chính bạn trong phiên này',
    viewportWidth: 'Viewport width',
    viewportHeight: 'Viewport height',
    pageTimeout: 'Timeout tải trang (ms)',
    scanNegativeTermsLabel: 'Từ ngữ tiêu cực cho Scan',
    scanNegativeTermsHelp: 'Mỗi dòng một từ hoặc cụm từ. Khi Scan bật lọc từ tiêu cực, row chứa từ này sẽ bị đánh dấu lỗi.',
    scanNegativeTermsPlaceholder: 'spam\nlừa đảo\nchửi bới',
    waitReadyState: 'Chờ trang ở trạng thái',
    fullPageCapture: 'Chụp full page',
    fullPageHelp: 'Bật nếu bạn muốn giữ toàn bộ chiều dài trang thay vì chỉ phần đang thấy.',
    jsonServiceAccount: 'JSON service account',
    jsonHelp: 'Chọn file service account .json hoặc dán JSON trực tiếp để lưu cục bộ và tự cập nhật credentials path.',
    serviceJsonLabel: 'Chọn file JSON',
    serviceJsonPasteLabel: 'Hoặc dán JSON trực tiếp',
    serviceJsonNoFile: 'Chưa chọn file',
    serviceJsonSelectedFmt: name => `Đã chọn: ${name}`,
    serviceJsonReadError: 'Không đọc được file JSON đã chọn',
    saveSettings: 'Lưu cài đặt',
    reloadSettings: 'Tải lại cài đặt',
    currentConfigSummary: 'Tóm tắt cấu hình hiện tại',
    viewport: 'Viewport',
    timeout: 'Timeout',
    waitMode: 'Chế độ chờ',
    output: 'Ảnh đầu ra',
    serviceAccount: 'Service account',
    sharingNote: 'Cách share quyền',
    sharingHelp: 'Share Google Sheets và thư mục Google Drive cho email service account ở trên với quyền Editor.',
    notSaved: 'Chưa lưu',
    saved: 'Đã lưu',
    fullPage: 'Chụp toàn bộ trang',
    viewportOnly: 'Chỉ chụp phần nhìn thấy',
    noServiceEmail: 'Chưa có email service account',
    fixedCredentials: 'Đã dùng credentials cố định',
    persistent: 'Lưu bền',
    noRunSelected: 'Chưa có job được chọn.',
    noGroupsYet: 'Chưa có dự án nào được lưu',
    noProjectGroup: 'Chưa chọn dự án',
    noErrors: 'Không có lỗi',
    clear: 'sạch',
    noProgressHistory: 'Chưa có lịch sử tiến độ',
    noActivity: 'Chưa có hoạt động nào',
    startOrSelect: 'Hãy chạy hoặc chọn một job để xem sự kiện.',
    latestUpdate: 'Cập nhật gần nhất',
    jobs: 'Jobs',
    detailLabel: 'Chi tiết',
    summaryLabel: 'Tóm tắt',
    openProjectRun: 'Mở trong chạy tác vụ',
    openProjectRunDone: 'Đã mở dự án trong Chạy tác vụ',
    deleteLabel: 'Xóa',
    deleteProjectConfirm: 'Xóa dự án đã lưu này?',
    deleteProjectDone: 'Đã xóa dự án',
    totalScope: 'Tổng phạm vi',
    processed: 'Đã xử lý',
    succeeded: 'Thành công',
    failedLabel: 'Thất bại',
    pendingFailed: 'Chờ / Lỗi',
    eta: 'ETA',
    group: 'Nhóm',
    latestJobMetaFmt: (status, stamp) => `${status} · ${stamp}`,
    overviewTextFmt: (id, done, total) => `Job ${id} đang theo dõi ${done}/${total} tác vụ.`,
    jobsLoadedFmt: count => `${count} job đã tải`,
    rowFmt: row => `Dòng ${row}`,
    jobsCountFmt: count => `${count} jobs`,
  },
  en: {
    searchPlaceholder: 'Search jobs or status...',
    launchChrome: 'Launch Chrome',
    refresh: 'Refresh',
    light: 'Light',
    dark: 'Dark',
    logout: 'Logout',
    roleAdmin: 'Admin',
    roleUser: 'User',
    adminOnly: 'Only admins can use this section',
    overview: 'Overview',
    runs: 'Runs',
    projects: 'Projects',
    tasks: 'Tasks',
    activities: 'Activities',
    access: 'User Management',
    settings: 'Settings',
    state: 'State',
    readyState: 'Ready',
    openRuns: 'Open Runs',
    view: 'View',
    sync: 'Sync',
    syncing: 'Syncing',
    synced: 'Synced',
    syncFailed: 'Failed',
    goToRuns: 'Open Run Center',
    selectedJob: 'Selected job',
    storedJobs: 'Stored jobs',
    successFailed: 'Success / Failed',
    overallProgress: 'Overall progress',
    overviewModeSplit: 'Mode split',
    overviewModeSplitSub: 'Tracked job distribution by mode.',
    overviewModeShareFmt: (count, pct) => `${count} jobs · ${pct}%`,
    overviewModeSplitEmpty: 'No mode data available yet.',
    overviewGreetingLabel: 'Daily greeting',
    overviewGreetingMorning: 'Good morning',
    overviewGreetingAfternoon: 'Good afternoon',
    overviewGreetingEvening: 'Good evening',
    overviewGreetingFallbackName: 'there',
    overviewGreetingSub: 'Keep your runs on track and continue today’s workflow.',
    jobsToday: 'Jobs today',
    avgSuccess: 'Average success rate',
    latestJob: 'Latest job',
    topError: 'Top error',
    overviewTimeline: 'Results by Date',
    overviewTimelineEmpty: 'No date-based run history yet',
    overviewDateFmt: label => `Date ${label}`,
    overviewTimelineJobsBadgeFmt: count => `${count} jobs`,
    overviewTimelineSuccessBadgeFmt: count => `${count} success`,
    overviewTimelineFailedBadgeFmt: count => `${count} failed`,
    overviewTimelineUnavailableBadgeFmt: count => `${count} unavailable`,
    overviewCompletedLegend: 'Completed',
    overviewFailedLegend: 'Errors',
    overviewUnavailableLegend: 'Unavailable',
    createdLast24h: 'created in the last 24h',
    acrossTracked: 'across tracked jobs',
    noRecentRun: 'no recent run',
    noRecurring: 'no recurring issues',
    runSummary: 'Run Summary',
    overviewClean: 'Overview stays clean. Running tools live in the Runs tab.',
    runConfig: 'Run Config',
    runConfigHelp: 'Share Editor access for the Sheet and Drive folder before running.',
    runShareLabel: 'Share Sheet & Drive folder with (Editor permission):',
    overwriteRunHelp: 'Always rerun and replace previous results.',
    highlightSheetErrors: 'Highlight errors on sheet',
    highlightSheetErrorsHelp: 'Color the sheet output cells after each row finishes: white for success, yellow for unavailable, and red for failed.',
    scanNegativeFilter: 'Negative word filter',
    scanNegativeFilterHelp: 'When enabled, Scan marks a row as failed if the OCR or text contains negative terms configured in Settings.',
    runMode: 'Run mode',
    columnMapping: 'Column Mapping',
    seeding: 'Seeding',
    booking: 'Booking',
    scan: 'Scan',
    runModeSeedingHelp: 'Seeding uses the standard posting flow and screenshot upload columns.',
    runModeBookingHelp: 'Booking is tuned for booking runs and repeated capture workflows.',
    runModeScanHelp: 'Scan skips Chrome when possible and uses the default scan columns.',
    addBlock: '+ Add Block',
    captureFive: 'Capture 5 images / link',
    captureFiveHelp: 'Enable this to capture all 5 images per link for booking runs.',
    chrome: 'Chrome',
    postName: 'Post Name',
    textColumn: 'Text Column',
    imageColumn: 'Image Column',
    resultColumn: 'Result Column',
    profileColumn: 'Profile',
    contentColumn: 'Content',
    linkUrl: 'Link URL',
    driveUrl: 'Drive URL',
    screenshotColumn: 'Screenshot',
    airDate: 'Air Date',
    sheetUrl: 'Sheet URL',
    sheetName: 'Sheet Name',
    driveFolder: 'Drive Folder ID',
    sheetUrlHintLoading: 'Loading sheet names...',
    sheetUrlHintEmpty: 'No sheets found in this spreadsheet',
    sheetUrlHintCountFmt: count => `${count} sheets found`,
    sheetNameInvalidFmt: name => `Sheet not found: ${name}`,
    sheetLinkCellHintLoading: 'Scanning URL cells...',
    sheetLinkCellHintCountFmt: count => `${count} URL columns found`,
    sheetLinkSuggestTitle: 'Detected link columns',
    sheetLinkSuggestSheetFmt: sheet => `Sheet: ${sheet}`,
    sheetLinkSuggestFind: 'Find columns',
    sheetLinkSuggestLoading: 'Scanning link columns...',
    sheetLinkSuggestHelp: 'Click a link-related column field, then pick a column below.',
    sheetLinkSuggestReady: 'Click Quick scan to detect link-bearing columns in this sheet.',
    sheetLinkSuggestNeedSheet: 'Choose a valid sheet name so the app can scan link columns.',
    sheetLinkSuggestActiveFmt: (field, block) => `Selecting for ${field}${block ? ` · ${block}` : ''}`,
    sheetLinkSuggestEmpty: 'No link columns detected in this sheet yet.',
    sheetLinkSuggestCountFmt: count => `${count} link columns found`,
    sheetLinkBulkToggle: 'Select multiple columns',
    sheetLinkBulkAdd: 'Create blocks',
    sheetLinkQuickScan: 'Quick scan',
    sheetLinkQuickCreate: 'Quick create',
    sheetLinkBulkClear: 'Clear',
    sheetLinkReload: 'Reload',
    sheetLinkBulkSelectedFmt: count => `${count} columns selected`,
    sheetLinkBulkHelp: 'Enable multi-select to pick several link columns and create blocks automatically.',
    sheetLinkBulkUnsupported: 'Multi-select is currently available for Seeding and Booking only.',
    sheetLinkBulkNone: 'No columns selected yet',
    sheetLinkBulkAddedFmt: count => `Created ${count} blocks and added 2 new columns for each selected column`,
    sheetLinkBulkNoNew: 'All selected columns already have blocks',
    sheetLinkQuickScanNoSelection: 'No columns selected for quick scan',
    sheetLinkQuickScanDoneFmt: count => `Created ${count} quick-scan blocks and added 2 new columns for each selection`,
    browserPort: 'Browser Port',
    startLine: 'Start Line',
    autoLaunchChrome: 'Auto Launch Chrome',
    startJob: 'Start Job',
    overwriteRun: 'Overwrite',
    pauseJob: 'Pause',
    stopJob: 'Stop',
    resumeJob: 'Resume',
    continueJob: 'Continue',
    errorOnlyJob: 'Run errors only',
    refreshJobs: 'Refresh Jobs',
    runQueue: 'Run Queue',
    runQueueHelp: 'Select a job to monitor. One active job is allowed per mode.',
    liveLogs: 'Live Logs',
    errorRows: 'Error Rows',
    selectedJobMeta: 'Selected Job',
    monitorKicker: '4. Result & Monitor',
    monitorTitle: 'Track progress and errors',
    monitorJob: 'Job',
    monitorProgress: 'Progress',
    monitorErrors: 'Summary',
    monitorTable: 'Processing log table',
    monitorNoJob: 'No job selected',
    monitorNoErrors: 'No errors',
    monitorIssueSummaryLabel: 'Summary',
    monitorIssueRowsLabel: 'Failed',
    monitorIssueUnavailableRowsLabel: 'Unavailable',
    monitorIssueExpandFmt: count => `+${count}`,
    monitorIssueCollapse: 'Collapse',
    monitorIssueStatsLabel: 'Stats',
    monitorIssueSummaryNone: 'No issue summary',
    monitorIssueSummaryTopFmt: (label, count) => `Top issue: ${label} (${count})`,
    monitorIssueSummaryTopMoreFmt: (label, count, more) => `Top issue: ${label} (${count}) · +${more} more issue types`,
    jobFinishedTitle: 'Completed',
    jobFinishedToastFmt: (name, done, total) => `${name} finished ${done}/${total} rows.`,
    jobFinishedBannerTitle: 'Project completed',
    jobFinishedBannerDismiss: 'Dismiss',
    monitorNoLogs: 'No data yet',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Success ${ok} · Failed ${fail} · Unavailable ${unavailable}`,
    monitorIssueCellCountFmt: count => `${count} cells`,
    unavailableLabel: 'Unavailable',
    time: 'Time',
    post: 'Post',
    result: 'Result',
    message: 'Message',
    replay: 'Replay',
    exportLog: 'Export Excel Log',
    noLogsToExport: 'No logs to export',
    replayStartedFmt: row => `Replay job queued for row ${row}`,
    continueStarted: 'Continue job queued',
    errorOnlyStarted: 'Error-only job queued',
    noData: 'No data',
    projectsState: 'Store completed runs and reopen their details',
    groupedProjects: 'Saved Projects',
    completedGroups: 'Saved Sheets',
    largestGroup: 'Selected Project',
    groupedRegistry: 'Project Library',
    groupSnapshot: 'Project Detail',
    projectLogs: 'Project logs',
    projectLogsSub: 'Saved with the selected project',
    projectModeLabel: 'Mode',
    projectStatusLabel: 'Status',
    allProjects: 'All',
    projectStatusAll: 'All',
    projectStatusRunning: 'Running',
    projectStatusCompleted: 'Completed',
    projectStatusStopped: 'Stopped',
    projectStatusFailed: 'Failed',
    projectOwner: 'Owner',
    noProjectsInFilter: 'No projects in this category',
    projectNoLogs: 'No logs for this project yet',
    tasksState: 'Workload breakdown',
    done: 'Done',
    pending: 'Pending',
    success: 'Success',
    failed: 'Failed',
    rowsProcessed: 'rows processed',
    rowsRemaining: 'remaining rows',
    rowsPassed: 'rows passed',
    rowsNeedRetry: 'rows need retry',
    taskDistribution: 'Task Distribution',
    progressOverTime: 'Progress Over Time',
    errorQueue: 'Error Queue',
    currentProgress: 'Current Progress',
    activitiesState: 'Latest runtime events with severity',
    recentTimeline: 'Activity History',
    activityLevel: 'Activity',
    accessState: 'Admins manage user access and admin emails',
    accessMailTitle: 'OTP Sender',
    accessMailHelp: 'Change the Gmail account that sends login codes from the admin UI. The old app password stays hidden and is only replaced when you enter a new one.',
    accessMailSenderLabel: 'Gmail sender',
    accessMailFromLabel: 'From email',
    accessMailPasswordLabel: 'New app password',
    accessMailSave: 'Save OTP Mail',
    accessMailEdit: 'Edit',
    accessMailHide: 'Hide',
    accessMailCurrentFmt: email => `Current sender: ${email || 'Not configured'}`,
    accessMailPasswordSaved: 'App password saved',
    accessMailPasswordMissing: 'App password missing',
    accessMailSourceEnv: 'Using .env source',
    accessMailSourceFile: 'Using UI override',
    accessMailSaved: 'OTP sender saved',
    accessMailReloaded: 'OTP sender reloaded',
    accessEntryTitle: 'Edit Gmail',
    accessEntryHelp: 'Change the selected Gmail address or role, then save it.',
    accessEntryEmailLabel: 'Gmail address',
    accessEntryRoleLabel: 'Role',
    accessEntryTypeLabel: 'Type',
    accessEntryCurrentFmt: email => `Editing: ${email || '-'}`,
    accessEntrySave: 'Save changes',
    accessEntryCancel: 'Cancel',
    accessEntrySaved: 'Gmail changes saved',
    accessEntryInvalid: 'Enter a valid Gmail address',
    accessDirectoryTitle: 'User Directory',
    accessDirectoryHelp: 'Filter Gmail accounts by role and access state, then change permission per row.',
    accessDirectorySearchPlaceholder: 'Search Gmail or state...',
    accessQuickAdd: '+ Add Gmail',
    accessFilterRole: 'Role',
    accessFilterScope: 'Access',
    accessFilterType: 'Type',
    accessFilterAll: 'All',
    accessFilterAdmin: 'Admin',
    accessFilterUser: 'User',
    accessFilterInternal: 'Internal',
    accessFilterExternal: 'External',
    accessYouTag: 'You',
    accessScopeAllowed: 'Allowed',
    accessScopeAdmin: 'Admin',
    accessScopeOpen: 'OTP',
    accessTableEmail: 'Gmail',
    accessTableAccess: 'Access',
    accessTableRole: 'Permission',
    accessTableType: 'Type',
    accessTableStatus: 'Status',
    accessTableUpdated: 'Updated',
    accessTableActions: 'Actions',
    accessDirectoryNoMatch: 'No Gmail matches the current filters',
    accessOpenEntryTitle: 'OTP Settings',
    accessOpenEntrySub: 'Only listed emails can request OTP',
    accessOpenEntryMailFmt: email => `OTP sender: ${email || 'Not configured'}`,
    accessAllowedEntrySub: 'Can request OTP',
    accessAdminEntrySub: 'Keeps admin control',
    accessStatusActive: 'Allowed',
    accessStatusAdmin: 'Admin control',
    accessStatusOpen: 'OTP restricted by list',
    accessTypeInternal: 'Internal',
    accessTypeExternal: 'External',
    accessMakeAdmin: 'Make admin',
    accessMakeUser: 'Make user',
    accessRemove: 'Remove',
    accessQuickAddInvalid: 'Enter a valid Gmail address to quick-add',
    accessQuickAddDoneFmt: email => `Added ${email} to the user list`,
    accessSummaryTitle: 'Access summary',
    accessSummaryAllowed: 'Allowed list',
    accessSummaryAdmins: 'Admin list',
    accessSummaryUpdated: 'Last updated',
    accessSummaryCurrentMail: 'Current signed-in email',
    accessSummaryCurrentRole: 'Current role',
    accessSummaryOpen: 'No email has been added yet',
    accessSummaryEmptyAdmins: 'No admin email yet',
    settingsState: 'Saved configuration',
    settingsTitle: 'Screenshot & credentials',
    settingsHelp: 'These values are reused by future jobs. You can also paste service account JSON here and save it once.',
    accessPolicyTitle: 'Access control',
    accessPolicyHelp: 'Admins manage which emails can log in and which emails keep admin permission.',
    accessAllowedLabel: 'Allowed emails',
    accessAllowedHelp: 'Only emails in the list can request OTP.',
    accessAdminLabel: 'Admin emails',
    accessAdminHelp: 'Admin emails always keep admin permission and can request OTP.',
    saveAccessPolicy: 'Save Access',
    reloadAccessPolicy: 'Reload Access',
    accessPolicySaved: 'Access control saved',
    accessNotifySentFmt: count => `Notification email sent to ${count} users`,
    accessNotifyPartialFmt: (sent, failed) => `Access control saved. Email sent: ${sent}, failed: ${failed}`,
    accessPolicySelfProtect: 'You cannot remove your own admin right in this session',
    viewportWidth: 'Viewport width',
    viewportHeight: 'Viewport height',
    pageTimeout: 'Page timeout (ms)',
    scanNegativeTermsLabel: 'Negative words for Scan',
    scanNegativeTermsHelp: 'Use one word or phrase per line. When Scan negative filtering is enabled, rows containing these terms are flagged as failed.',
    scanNegativeTermsPlaceholder: 'spam\nscam\nabuse',
    waitReadyState: 'Wait ready state',
    fullPageCapture: 'Full page capture',
    fullPageHelp: 'Enable this if you want to keep the entire page length instead of only the visible area.',
    jsonServiceAccount: 'JSON service account',
    jsonHelp: 'Upload a service account .json file or paste the JSON directly to save it locally and update the credentials path automatically.',
    serviceJsonLabel: 'Choose JSON file',
    serviceJsonPasteLabel: 'Or paste JSON directly',
    serviceJsonNoFile: 'No file selected',
    serviceJsonSelectedFmt: name => `Selected: ${name}`,
    serviceJsonReadError: 'Unable to read the selected JSON file',
    saveSettings: 'Save Settings',
    reloadSettings: 'Reload Settings',
    currentConfigSummary: 'Current config summary',
    viewport: 'Viewport',
    timeout: 'Timeout',
    waitMode: 'Wait mode',
    output: 'Output',
    serviceAccount: 'Service account',
    sharingNote: 'Sharing note',
    sharingHelp: 'Share Google Sheets and Drive folder with the service account email above using Editor permission.',
    notSaved: 'Not saved',
    saved: 'Saved',
    fullPage: 'Full page',
    viewportOnly: 'Viewport only',
    noServiceEmail: 'No service account email',
    fixedCredentials: 'Using fixed credentials',
    persistent: 'Persistent',
    noRunSelected: 'No run selected.',
    noGroupsYet: 'No saved projects yet',
    noProjectGroup: 'No project selected',
    noErrors: 'No errors',
    clear: 'clear',
    noProgressHistory: 'No progress history yet',
    noActivity: 'No activity yet',
    startOrSelect: 'Start or select a job to see events.',
    latestUpdate: 'Latest update',
    jobs: 'Jobs',
    detailLabel: 'Detail',
    summaryLabel: 'Summary',
    openProjectRun: 'Open in Runs',
    openProjectRunDone: 'Project opened in Runs',
    deleteLabel: 'Delete',
    deleteProjectConfirm: 'Delete this saved project?',
    deleteProjectDone: 'Project deleted',
    totalScope: 'Total scope',
    processed: 'Processed',
    succeeded: 'Succeeded',
    failedLabel: 'Failed',
    pendingFailed: 'Pending / Failed',
    eta: 'ETA',
    group: 'Group',
    latestJobMetaFmt: (status, stamp) => `${status} · ${stamp}`,
    overviewTextFmt: (id, done, total) => `Job ${id} is tracking ${done}/${total} tasks.`,
    jobsLoadedFmt: count => `${count} jobs loaded`,
    rowFmt: row => `Row ${row}`,
    jobsCountFmt: count => `${count} jobs`,
  }
};

function t(key) {
  return (I18N[currentLang] && I18N[currentLang][key]) || (I18N.en[key] ?? key);
}

function getRoleLabel(role = authState.role) {
  return String(role || '').toLowerCase() === 'admin' ? t('roleAdmin') : t('roleUser');
}

function deriveGreetingName(email = authState.email) {
  const local = String(email || '').split('@')[0] || '';
  const parts = local.split(/[._-]+/).map(part => part.replace(/\\d+/g, '').trim()).filter(Boolean);
  const base = parts[0] || '';
  if (!base) return t('overviewGreetingFallbackName');
  return base.charAt(0).toUpperCase() + base.slice(1);
}

function deriveGreetingInitials(email = authState.email) {
  const local = String(email || '').split('@')[0] || '';
  const parts = local.split(/[._-]+/).map(part => part.replace(/\\d+/g, '').trim()).filter(Boolean);
  const initials = (parts.slice(0, 2).map(part => part.charAt(0).toUpperCase()).join('') || 'EV').slice(0, 2);
  return initials || 'EV';
}

function getGreetingTextByHour(date = new Date()) {
  const hour = Number(date.getHours());
  if (hour < 12) return t('overviewGreetingMorning');
  if (hour < 18) return t('overviewGreetingAfternoon');
  return t('overviewGreetingEvening');
}

function renderOverviewGreeting() {
  const kicker = document.getElementById('ovGreetingKicker');
  if (kicker) kicker.textContent = t('overviewGreetingLabel');
  const title = document.getElementById('ovGreetingTitle');
  if (title) title.textContent = `${getGreetingTextByHour()}, ${deriveGreetingName()}`;
  const sub = document.getElementById('ovGreetingSub');
  if (sub) sub.textContent = t('overviewGreetingSub');
  const avatar = document.getElementById('ovGreetingAvatar');
  if (avatar) avatar.textContent = deriveGreetingInitials();
  const email = document.getElementById('ovGreetingEmail');
  if (email) email.textContent = authState.email || '-';
  const role = document.getElementById('ovGreetingRole');
  if (role) {
    role.textContent = getRoleLabel();
    role.className = `auth-role auth-role-${authState.role || 'user'} overview-greeting-role`;
  }
}

function isAdminUser() {
  return !!authState.isAdmin;
}

function getRunModeLabel(mode) {
  return t(String(mode || 'seeding').toLowerCase());
}

function formatRunTitle(mode = currentRunMode) {
  return getRunModeLabel(mode);
}

function formatRunConfigTitle(mode = currentRunMode) {
  return t('runConfig');
}

function sanitizeMappingBlockForMode(mode, block, index = 1) {
  const key = String(mode || 'seeding').toLowerCase();
  const next = {
    ...defaultMappingBlock(key, index),
    ...(block || {}),
    start_line: Number(block?.start_line || 4),
    mode: key,
  };
  if (key === 'seeding') {
    next.col_profile = '';
    next.col_content = '';
  } else if (key === 'scan') {
    next.col_profile = '';
    next.col_screenshot = '';
    next.col_air_date = '';
  }
  return next;
}

function normalizeMappingsByModeForClient(raw = {}) {
  const next = {};
  ['seeding', 'booking', 'scan'].forEach(mode => {
    const items = Array.isArray(raw?.[mode]) ? raw[mode] : [];
    if (!items.length) return;
    next[mode] = items.map((block, index) => sanitizeMappingBlockForMode(mode, block, index + 1));
  });
  return next;
}

function defaultRunFlagsForMode(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  return {
    force_run_all: true,
    highlight_sheet_errors: true,
    capture_five_per_link: key === 'booking' ? false : false,
    scan_negative_filter: false,
  };
}

function normalizeRunFlagsByModeForClient(raw = {}) {
  const next = {};
  ['seeding', 'booking', 'scan'].forEach(mode => {
    next[mode] = {
      ...defaultRunFlagsForMode(mode),
      ...((raw && typeof raw === 'object' && raw[mode] && typeof raw[mode] === 'object') ? raw[mode] : {}),
    };
    if (mode !== 'booking') next[mode].capture_five_per_link = false;
    if (mode !== 'scan') next[mode].scan_negative_filter = false;
    next[mode].force_run_all = next[mode].force_run_all !== false;
    next[mode].highlight_sheet_errors = !!next[mode].highlight_sheet_errors;
    next[mode].capture_five_per_link = !!next[mode].capture_five_per_link;
    next[mode].scan_negative_filter = !!next[mode].scan_negative_filter;
  });
  return next;
}

function ensureRunFlagsForMode(mode = currentRunMode) {
  const key = String(mode || 'seeding').toLowerCase();
  if (!currentRunFlagsByMode || typeof currentRunFlagsByMode !== 'object') currentRunFlagsByMode = {};
  const normalized = normalizeRunFlagsByModeForClient(currentRunFlagsByMode);
  currentRunFlagsByMode = normalized;
  return currentRunFlagsByMode[key] || defaultRunFlagsForMode(key);
}

function rememberCurrentRunFlags(mode = currentRunMode) {
  const key = String(mode || currentRunMode || 'seeding').toLowerCase();
  const flags = ensureRunFlagsForMode(key);
  const overwriteNode = document.getElementById('force_run_all');
  const highlightNode = document.getElementById('highlight_sheet_errors');
  const negativeNode = document.getElementById('scan_negative_filter');
  flags.force_run_all = overwriteNode ? !!overwriteNode.checked : flags.force_run_all !== false;
  flags.highlight_sheet_errors = highlightNode ? !!highlightNode.checked : !!flags.highlight_sheet_errors;
  flags.capture_five_per_link = key === 'booking' ? !!captureFivePerLink : false;
  flags.scan_negative_filter = key === 'scan' ? !!negativeNode?.checked : false;
  currentRunFlagsByMode[key] = flags;
  return flags;
}

function applyRunFlagsForMode(mode = currentRunMode) {
  const key = String(mode || currentRunMode || 'seeding').toLowerCase();
  const flags = ensureRunFlagsForMode(key);
  const overwriteNode = document.getElementById('force_run_all');
  const highlightNode = document.getElementById('highlight_sheet_errors');
  const negativeNode = document.getElementById('scan_negative_filter');
  if (overwriteNode) overwriteNode.checked = flags.force_run_all !== false;
  if (highlightNode) highlightNode.checked = !!flags.highlight_sheet_errors;
  if (negativeNode) negativeNode.checked = key === 'scan' ? !!flags.scan_negative_filter : false;
  captureFivePerLink = key === 'booking' ? !!flags.capture_five_per_link : false;
}

function serializeMappingsByModeForSave() {
  const payload = {};
  Object.entries(currentMappingBlocksByMode || {}).forEach(([mode, items]) => {
    const key = String(mode || '').toLowerCase();
    if (!['seeding', 'booking', 'scan'].includes(key)) return;
    const blocks = Array.isArray(items) ? items : [];
    if (!blocks.length) return;
    payload[key] = blocks.map((block, index) => sanitizeMappingBlockForMode(key, block, index + 1));
  });
  return payload;
}

function getRunModeHelp(mode) {
  if (mode === 'booking') return t('runModeBookingHelp');
  if (mode === 'scan') return t('runModeScanHelp');
  return t('runModeSeedingHelp');
}

function defaultMappingBlock(mode, index = 1) {
  const blockIndex = Number(index || 1);
  if (mode === 'scan') {
    return {
      name: `Scan ${blockIndex}`,
      start_line: 4,
      col_profile: '',
      col_content: 'E',
      col_url: 'F',
      col_drive: 'G',
      col_screenshot: '',
      col_air_date: '',
      fixed_air_date: '',
      manual_link: '',
      mode: 'scan'
    };
  }
  const isBooking = mode === 'booking';
  return {
    name: `Post ${blockIndex}`,
    start_line: 4,
    col_profile: isBooking ? 'B' : '',
    col_content: isBooking ? 'I' : '',
    col_url: 'K',
    col_drive: 'L',
    col_screenshot: 'J',
    col_air_date: '',
    fixed_air_date: '',
    manual_link: '',
    mode: isBooking ? 'booking' : 'seeding'
  };
}

function ensureMappingBlocks(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  if (!Array.isArray(currentMappingBlocksByMode[key]) || !currentMappingBlocksByMode[key].length) {
    currentMappingBlocksByMode[key] = [defaultMappingBlock(key, 1)];
  } else {
    currentMappingBlocksByMode[key] = currentMappingBlocksByMode[key].map((block, index) => sanitizeMappingBlockForMode(key, block, index + 1));
  }
  return currentMappingBlocksByMode[key];
}

function mappingFieldsForMode(mode) {
  if (mode === 'scan') {
    return [
      { key: 'name', label: t('postName') },
      { key: 'col_content', label: t('textColumn') },
      { key: 'col_url', label: t('imageColumn') },
      { key: 'col_drive', label: t('resultColumn') },
      { key: 'start_line', label: t('startLine'), type: 'number' },
    ];
  }
  if (mode === 'seeding') {
    return [
      { key: 'name', label: t('postName') },
      { key: 'col_air_date', label: t('airDate') },
      { key: 'col_url', label: t('linkUrl') },
      { key: 'col_drive', label: t('driveUrl') },
      { key: 'col_screenshot', label: t('screenshotColumn') },
      { key: 'start_line', label: t('startLine'), type: 'number' },
    ];
  }
  return [
    { key: 'name', label: t('postName') },
    { key: 'col_air_date', label: t('airDate') },
    { key: 'col_profile', label: t('profileColumn') },
    { key: 'col_content', label: t('contentColumn') },
    { key: 'col_url', label: t('linkUrl') },
    { key: 'col_drive', label: t('driveUrl') },
    { key: 'col_screenshot', label: t('screenshotColumn') },
    { key: 'start_line', label: t('startLine'), type: 'number' },
  ];
}

function getMappingFieldInputId(mode, index, key) {
  return `mapping_${String(mode || 'seeding')}_${Number(index) || 0}_${String(key || '')}`;
}

function isLinkSuggestionField(mode, key) {
  const normalizedMode = String(mode || '').toLowerCase();
  const normalizedKey = String(key || '').toLowerCase();
  if (normalizedMode === 'scan') return normalizedKey === 'col_url';
  return ['col_url', 'col_drive', 'col_screenshot'].includes(normalizedKey);
}

function supportsBulkSheetLinkMode(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || '').toLowerCase();
  return ['seeding', 'booking'].includes(normalizedMode);
}

function getBulkSheetLinkSelections(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  if (!Array.isArray(bulkSheetLinkSelectionsByMode[normalizedMode])) {
    bulkSheetLinkSelectionsByMode[normalizedMode] = [];
  }
  return bulkSheetLinkSelectionsByMode[normalizedMode];
}

function clearBulkSheetLinkSelections(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  bulkSheetLinkSelectionsByMode[normalizedMode] = [];
}

function toggleBulkSheetLinkMode(nextEnabled = null) {
  if (!supportsBulkSheetLinkMode(currentRunMode)) {
    alert(t('sheetLinkBulkUnsupported'));
    return;
  }
  bulkSheetLinkSelectionMode = nextEnabled == null ? !bulkSheetLinkSelectionMode : !!nextEnabled;
  if (!bulkSheetLinkSelectionMode) {
    clearBulkSheetLinkSelections(currentRunMode);
  }
  renderSheetLinkSuggestions();
}

function toggleBulkSheetLinkSelection(column) {
  if (!supportsBulkSheetLinkMode(currentRunMode)) {
    alert(t('sheetLinkBulkUnsupported'));
    return;
  }
  const normalized = String(column || '').trim().toUpperCase();
  if (!normalized) return;
  const selected = getBulkSheetLinkSelections(currentRunMode);
  const index = selected.indexOf(normalized);
  if (index >= 0) selected.splice(index, 1);
  else selected.push(normalized);
  renderSheetLinkSuggestions();
}

function sheetColumnLetterToIndex(column) {
  const normalized = String(column || '').trim().toUpperCase();
  if (!normalized) return 0;
  let value = 0;
  for (const ch of normalized) {
    const code = ch.charCodeAt(0);
    if (code < 65 || code > 90) return 0;
    value = (value * 26) + (code - 64);
  }
  return value;
}

function sheetColumnIndexToLetter(index) {
  let n = Number(index || 0);
  if (!Number.isFinite(n) || n <= 0) return '';
  let out = '';
  while (n > 0) {
    const remainder = (n - 1) % 26;
    out = String.fromCharCode(65 + remainder) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
}

function shiftSheetColumn(column, delta = 0) {
  const base = sheetColumnLetterToIndex(column);
  if (!base) return '';
  return sheetColumnIndexToLetter(base + Number(delta || 0));
}

function buildBulkSuggestedBlock(mode, column, index, template = null) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  const base = sanitizeMappingBlockForMode(normalizedMode, template || defaultMappingBlock(normalizedMode, index), index);
  const normalizedColumn = String(column || '').trim().toUpperCase();
  const next1 = shiftSheetColumn(normalizedColumn, 1);
  const next2 = shiftSheetColumn(normalizedColumn, 2);
  base.name = normalizedMode === 'scan' ? `Scan ${index}` : `Post ${index}`;
  if (normalizedMode === 'booking') {
    base.col_url = normalizedColumn;
    if (next1) base.col_screenshot = next1;
    if (next2) base.col_drive = next2;
  } else if (normalizedMode === 'seeding') {
    base.col_url = normalizedColumn;
    if (next1) base.col_screenshot = next1;
    if (next2) base.col_drive = next2;
  }
  return base;
}

function buildSuggestedBlockFromColumns(mode, linkColumn, driveColumn, screenshotColumn, index, template = null) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  const base = sanitizeMappingBlockForMode(normalizedMode, template || defaultMappingBlock(normalizedMode, index), index);
  const normalizedLink = String(linkColumn || '').trim().toUpperCase();
  const normalizedDrive = String(driveColumn || '').trim().toUpperCase();
  const normalizedScreenshot = String(screenshotColumn || '').trim().toUpperCase();
  base.name = normalizedMode === 'scan' ? `Scan ${index}` : `Post ${index}`;
  base.col_url = normalizedLink;
  if (normalizedMode === 'booking' || normalizedMode === 'seeding') {
    if (normalizedDrive) base.col_drive = normalizedDrive;
    if (normalizedScreenshot) base.col_screenshot = normalizedScreenshot;
  }
  return base;
}

async function addBlocksFromSelectedLinkColumns() {
  if (!supportsBulkSheetLinkMode(currentRunMode)) {
    alert(t('sheetLinkBulkUnsupported'));
    return;
  }
  const selected = getBulkSheetLinkSelections(currentRunMode).slice();
  if (!selected.length) {
    alert(t('sheetLinkBulkNone'));
    return;
  }
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  if (!rawUrl || !rawName) {
    alert(t('sheetLinkQuickScanNoSelection'));
    return;
  }
  try {
    const out = await req('/api/sheets/quick-block-columns', {
      method: 'POST',
      body: JSON.stringify({
        sheet_url: rawUrl,
        sheet_name: rawName,
        mode: currentRunMode,
        columns: selected,
      }),
    });
    const items = Array.isArray(out?.items) ? out.items : [];
    if (!items.length) {
      alert(t('sheetLinkBulkNoNew'));
      return;
    }
    const blocks = ensureMappingBlocks(currentRunMode);
    const template = blocks.length ? blocks[blocks.length - 1] : defaultMappingBlock(currentRunMode, 1);
    items.forEach(item => {
      const nextIndex = blocks.length + 1;
      blocks.push(
        buildSuggestedBlockFromColumns(
          currentRunMode,
          item.link_column || item.source_column || '',
          item.drive_column || '',
          item.screenshot_column || '',
          nextIndex,
          template,
        )
      );
    });
    pendingMappingScrollMode = currentRunMode;
    pendingMappingHighlightIndex = Math.max(0, blocks.length - items.length);
    clearBulkSheetLinkSelections(currentRunMode);
    bulkSheetLinkSelectionMode = false;
    renderMappingEditor();
    await fetchSheetLinkSuggestions(true);
    setStatus(t('sheetLinkBulkAddedFmt')(items.length), 'done');
  } catch (e) {
    alert(e.message);
  }
}

async function quickScanSelectedLinkColumns() {
  await addBlocksFromSelectedLinkColumns();
}

function setSheetColumnTarget(mode, index, key) {
  if (!isLinkSuggestionField(mode, key)) return;
  activeSheetColumnTarget = {
    mode: String(mode || currentRunMode || 'seeding').toLowerCase(),
    index: Number(index) || 0,
    key: String(key || '').trim(),
  };
  renderSheetLinkSuggestions();
}

function getActiveSheetColumnTargetValue() {
  if (!activeSheetColumnTarget) return '';
  const blocks = ensureMappingBlocks(activeSheetColumnTarget.mode);
  const block = blocks[activeSheetColumnTarget.index];
  if (!block) return '';
  return String(block[activeSheetColumnTarget.key] || '').trim().toUpperCase();
}

function currentSheetColumnStartRow() {
  const blocks = ensureMappingBlocks(currentRunMode);
  const rows = blocks
    .map(block => Number(block?.start_line || 4))
    .filter(value => Number.isFinite(value) && value > 0);
  return rows.length ? Math.max(1, Math.min(...rows)) : 4;
}

function getCachedSheetLinkColumns(rawUrl, rawName, startRow, allowStale = false) {
  const key = `${String(rawUrl || '').trim()}|${String(rawName || '').trim()}|${Math.max(1, Number(startRow || 4) || 4)}`;
  const entry = sheetColumnSuggestCache[key];
  if (!entry || !Array.isArray(entry.columns)) return null;
  if (allowStale) return entry;
  if ((Date.now() - Number(entry.ts || 0)) > SHEET_COLUMN_CACHE_TTL_MS) return null;
  return entry;
}

function getSheetLinkSuggestPayload(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  if (!sheetLinkSuggestPayloadByMode[normalizedMode]) {
    sheetLinkSuggestPayloadByMode[normalizedMode] = { columns: [], counts: {} };
  }
  return sheetLinkSuggestPayloadByMode[normalizedMode];
}

function resetSheetLinkSuggestions(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  sheetLinkSuggestLoadedByMode[normalizedMode] = false;
  sheetLinkSuggestLoadingByMode[normalizedMode] = false;
  sheetLinkSuggestPayloadByMode[normalizedMode] = { columns: [], counts: {} };
  sheetLinkSuggestSourceKeyByMode[normalizedMode] = '';
  if (String(normalizedMode) === String(currentRunMode || '').toLowerCase()) {
    currentSheetLinkColumns = [];
    setSheetNameHint('');
    renderSheetLinkSuggestions();
  }
}

async function handleSheetLinkQuickAction() {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  if (modeKey === 'scan') return;
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  const startRow = currentSheetColumnStartRow();
  const currentKey = `${rawUrl}|${rawName}|${startRow}`;
  const sourceKey = String(sheetLinkSuggestSourceKeyByMode[modeKey] || '');
  const loadedForCurrentSheet = !!sheetLinkSuggestLoadedByMode[modeKey] && sourceKey === currentKey;
  if (!loadedForCurrentSheet) {
    const cached = getCachedSheetLinkColumns(rawUrl, rawName, startRow, false);
    if (cached) {
      sheetLinkSuggestLoadedByMode[modeKey] = true;
      sheetLinkSuggestPayloadByMode[modeKey] = cached;
      sheetLinkSuggestSourceKeyByMode[modeKey] = currentKey;
      renderSheetLinkSuggestions(cached);
      return;
    }
    await findSheetLinkSuggestions(true);
    return;
  }
  await quickScanSelectedLinkColumns();
}

async function reloadSheetLinkSuggestions() {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  if (modeKey === 'scan') return;
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  const startRow = currentSheetColumnStartRow();
  const cacheKey = `${rawUrl}|${rawName}|${startRow}`;
  delete sheetColumnSuggestCache[cacheKey];
  delete sheetColumnSuggestInflight[cacheKey];
  sheetLinkSuggestLoadedByMode[modeKey] = false;
  sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {} };
  sheetLinkSuggestSourceKeyByMode[modeKey] = '';
  clearBulkSheetLinkSelections(modeKey);
  activeSheetColumnTarget = null;
  await findSheetLinkSuggestions(true);
}

async function findSheetLinkSuggestions(force = true) {
  const normalizedMode = String(currentRunMode || 'seeding').toLowerCase();
  sheetLinkSuggestLoadingByMode[normalizedMode] = true;
  renderSheetLinkSuggestions();
  try {
    await fetchSheetLinkSuggestions(force);
  } finally {
    sheetLinkSuggestLoadingByMode[normalizedMode] = false;
    renderSheetLinkSuggestions();
    requestAnimationFrame(() => {
      document.getElementById('sheet_link_suggest')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });
  }
}

function renderSheetLinkSuggestions(payload = null) {
  const host = document.getElementById('sheet_link_suggest');
  const titleNode = document.getElementById('sheet_link_suggest_title');
  const metaNode = document.getElementById('sheet_link_suggest_meta');
  const actionsNode = document.getElementById('sheet_link_suggest_actions');
  const rowsNode = document.getElementById('sheet_link_suggest_rows');
  const datalist = document.getElementById('sheet_link_column_datalist');
  if (!host || !titleNode || !metaNode || !actionsNode || !rowsNode || !datalist) return;

  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  if (modeKey === 'scan') {
    host.style.display = 'none';
    titleNode.textContent = t('sheetLinkSuggestTitle');
    metaNode.textContent = '';
    actionsNode.innerHTML = '';
    rowsNode.innerHTML = '';
    datalist.innerHTML = '';
    currentSheetLinkColumns = [];
    setSheetNameHint('');
    return;
  }
  host.style.display = '';
  const data = payload || getSheetLinkSuggestPayload(modeKey) || { columns: currentSheetLinkColumns || [] };
  const columns = Array.isArray(data.columns) ? data.columns.map(value => String(value || '').trim().toUpperCase()).filter(Boolean) : [];
  const counts = data && typeof data.counts === 'object' && data.counts ? data.counts : {};
  const totalUrlColumns = columns.length || Object.keys(counts).length;
  const rawSheetName = String(document.getElementById('sheet_name')?.value || '').trim();
  const rawSheetUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const startRow = currentSheetColumnStartRow();
  const currentKey = `${rawSheetUrl}|${rawSheetName}|${startRow}`;
  const loaded = !!sheetLinkSuggestLoadedByMode[modeKey] && String(sheetLinkSuggestSourceKeyByMode[modeKey] || '') === currentKey;
  const loading = !!sheetLinkSuggestLoadingByMode[modeKey];
  const idle = !loaded && !loading;
  currentSheetLinkColumns = columns;
  titleNode.textContent = t('sheetLinkSuggestTitle');
  host.classList.toggle('idle', idle);
  const bulkSupported = supportsBulkSheetLinkMode(currentRunMode);
  const bulkSelections = getBulkSheetLinkSelections(currentRunMode);

  const active = activeSheetColumnTarget;
  let helperText = '';
  if (loading) {
    helperText = t('sheetLinkSuggestLoading');
  } else if (loaded && active && ensureMappingBlocks(active.mode)[active.index]) {
    const fields = mappingFieldsForMode(active.mode);
    const field = fields.find(item => item.key === active.key);
    const block = ensureMappingBlocks(active.mode)[active.index] || {};
    helperText = t('sheetLinkSuggestActiveFmt')(field?.label || active.key, String(block?.name || '').trim());
  } else if (loaded) {
    helperText = columns.length ? t('sheetLinkSuggestHelp') : '';
  }
  const sheetLabel = loaded && rawSheetName ? t('sheetLinkSuggestSheetFmt')(rawSheetName) : '';
  const metaText = [sheetLabel, helperText].filter(Boolean).join(' · ');
  metaNode.textContent = '';
  if (loading) {
    setSheetNameHint(t('sheetLinkCellHintLoading'));
  } else if (loaded && rawSheetName) {
    setSheetNameHint(t('sheetLinkCellHintCountFmt')(totalUrlColumns));
  } else {
    setSheetNameHint('');
  }

  const quickScanDisabled = loading;
  if (bulkSupported) {
    actionsNode.innerHTML = loaded ? `
      <div class="sheet-link-suggest-action-group meta">
        <span class="sheet-link-suggest-action-meta">${esc(metaText)}</span>
      </div>
      <div class="sheet-link-suggest-action-group buttons">
        <button class="btn sheet-link-suggest-action-btn icon-only" type="button" title="${esc(t('sheetLinkReload'))}" aria-label="${esc(t('sheetLinkReload'))}" onclick="reloadSheetLinkSuggestions()" ${quickScanDisabled ? 'disabled' : ''}>
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 12a9 9 0 1 1-2.64-6.36"></path><path d="M21 3v6h-6"></path></svg>
        </button>
        <button class="btn blue sheet-link-suggest-action-btn" type="button" onclick="addBlocksFromSelectedLinkColumns()" ${quickScanDisabled ? 'disabled' : ''}>${esc(t('sheetLinkBulkAdd'))}</button>
      </div>
    ` : `
      <div class="sheet-link-suggest-action-group buttons" style="width:100%;justify-content:center">
        <button class="btn sheet-link-suggest-action-btn" type="button" onclick="handleSheetLinkQuickAction()" ${quickScanDisabled ? 'disabled' : ''}>${esc(t('sheetLinkQuickScan'))}</button>
      </div>
    `;
  } else {
    actionsNode.innerHTML = `
      <div class="sheet-link-suggest-action-group meta">
        <span class="sheet-link-suggest-action-meta">${esc(loading ? t('sheetLinkSuggestLoading') : metaText)}</span>
      </div>
      <div class="sheet-link-suggest-action-group buttons">
        <button class="btn sheet-link-suggest-action-btn" type="button" onclick="handleSheetLinkQuickAction()" ${quickScanDisabled ? 'disabled' : ''}>${esc(t('sheetLinkQuickScan'))}</button>
      </div>
    `;
  }

  datalist.innerHTML = columns.map(col => `<option value="${esc(col)}"></option>`).join('');
  host.classList.add('open');
  if (!loaded) {
    rowsNode.innerHTML = '';
    return;
  }
  if (!columns.length) {
    rowsNode.innerHTML = `<div class="sheet-link-suggest-empty">${esc(String(document.getElementById('sheet_name')?.value || '').trim() ? t('sheetLinkSuggestEmpty') : '')}</div>`;
    return;
  }
  const activeValue = getActiveSheetColumnTargetValue();
  rowsNode.innerHTML = columns.map(col => {
    const count = Number(counts?.[col] || 0);
    const activeClass = !bulkSupported && activeValue === col ? ' active' : '';
    const selectedClass = bulkSupported && bulkSelections.includes(col) ? ' selected' : '';
    const title = count > 0 ? `${col} · ${count}` : col;
    const clickHandler = bulkSupported
      ? `toggleBulkSheetLinkSelection('${esc(col)}')`
      : `applySuggestedSheetColumn('${esc(col)}')`;
    return `<button class="sheet-link-suggest-chip${activeClass}${selectedClass}" type="button" title="${esc(title)}" onclick="${clickHandler}">${esc(col)}</button>`;
  }).join('');
}

async function fetchSheetLinkSuggestions(force = false) {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  if (modeKey === 'scan') {
    resetSheetLinkSuggestions(modeKey);
    return;
  }
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  const host = document.getElementById('sheet_link_suggest');
  const rowsNode = document.getElementById('sheet_link_suggest_rows');
  const metaNode = document.getElementById('sheet_link_suggest_meta');
  if (!rawUrl || !rawName) {
    currentSheetLinkColumns = [];
    sheetLinkSuggestLoadedByMode[modeKey] = false;
    sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {} };
    if (rowsNode) rowsNode.innerHTML = '';
    if (metaNode) metaNode.textContent = '';
    return;
  }
  if (!force && !isKnownSheetName(rawUrl, rawName)) {
    currentSheetLinkColumns = [];
    sheetLinkSuggestLoadedByMode[modeKey] = false;
    sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {} };
    if (rowsNode) rowsNode.innerHTML = '';
    if (metaNode) metaNode.textContent = '';
    return;
  }
  const startRow = currentSheetColumnStartRow();
  const cacheKey = `${rawUrl}|${rawName}|${startRow}`;
  const cached = getCachedSheetLinkColumns(rawUrl, rawName, startRow, false);
  if (cached && !force) {
    sheetColumnSuggestKey = cacheKey;
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = cached;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    renderSheetLinkSuggestions(cached);
    return;
  }
  if (sheetColumnSuggestInflight[cacheKey]) {
    const pending = await sheetColumnSuggestInflight[cacheKey];
    sheetColumnSuggestKey = cacheKey;
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = pending;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    renderSheetLinkSuggestions(pending);
    return;
  }
  if (host) host.classList.add('open');
  if (rowsNode) rowsNode.innerHTML = `<div class="sheet-link-suggest-empty">${esc(t('sheetLinkSuggestLoading'))}</div>`;
  if (metaNode) metaNode.textContent = '';
  try {
    sheetColumnSuggestInflight[cacheKey] = (async () => {
      const qs = new URLSearchParams({
        sheet_url: rawUrl,
        sheet_name: rawName,
        start_row: String(startRow),
      });
      if (force) qs.set('force', '1');
      if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
      const out = await req('/api/sheets/column-suggestions?' + qs.toString());
      if (String(out.sheet_name || '').trim() && String(out.sheet_name || '').trim() !== rawName) {
        sheet_name.value = String(out.sheet_name || '').trim();
        rememberResolvedSheetName(rawUrl, out.sheet_name);
      }
      return {
        columns: Array.isArray(out.columns) ? out.columns : [],
        counts: out.counts && typeof out.counts === 'object' ? out.counts : {},
        drive_columns: Array.isArray(out.drive_columns) ? out.drive_columns : [],
        samples: out.samples && typeof out.samples === 'object' ? out.samples : {},
      };
    })();
    const payload = await sheetColumnSuggestInflight[cacheKey];
    sheetColumnSuggestCache[cacheKey] = { ...payload, ts: Date.now() };
    sheetColumnSuggestKey = cacheKey;
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = payload;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    renderSheetLinkSuggestions(payload);
  } catch (e) {
    const stale = getCachedSheetLinkColumns(rawUrl, rawName, startRow, true);
    if (stale) {
      sheetLinkSuggestLoadedByMode[modeKey] = true;
      sheetLinkSuggestPayloadByMode[modeKey] = stale;
      sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
      renderSheetLinkSuggestions(stale);
    } else if (rowsNode) {
      sheetLinkSuggestLoadedByMode[modeKey] = true;
      sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {}, error: e.message };
      sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
      if (host) host.classList.add('open');
      rowsNode.innerHTML = `<div class="sheet-link-suggest-empty">${esc(e.message)}</div>`;
    }
  } finally {
    delete sheetColumnSuggestInflight[cacheKey];
  }
}

async function refreshSheetLinkCountSummary(force = false) {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  if (modeKey === 'scan') {
    setSheetNameHint('');
    return;
  }
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  if (!rawUrl || !rawName) {
    setSheetNameHint('');
    return;
  }
  let knownSheet = isKnownSheetName(rawUrl, rawName);
  if (!knownSheet && force) {
    try {
      await fetchSheetNameSuggestions(true);
    } catch (_) {}
    knownSheet = isKnownSheetName(rawUrl, rawName);
  }
  if (!knownSheet) {
    setSheetNameHint(rawName ? t('sheetNameInvalidFmt')(rawName) : '', !!rawName);
    return;
  }
  const startRow = currentSheetColumnStartRow();
  const cached = getCachedSheetLinkColumns(rawUrl, rawName, startRow, false);
  if (cached && !force) {
    const totalCached = Array.isArray(cached.columns) ? cached.columns.length : Object.keys(cached.counts || {}).length;
    setSheetNameHint(t('sheetLinkCellHintCountFmt')(totalCached));
    return;
  }
  setSheetNameHint(t('sheetLinkCellHintLoading'));
  const cacheKey = `${rawUrl}|${rawName}|${startRow}`;
  try {
    if (!sheetColumnSuggestInflight[cacheKey]) {
      sheetColumnSuggestInflight[cacheKey] = (async () => {
        const qs = new URLSearchParams({
          sheet_url: rawUrl,
          sheet_name: rawName,
          start_row: String(startRow),
        });
        if (force) qs.set('force', '1');
        if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
        const out = await req('/api/sheets/column-suggestions?' + qs.toString());
        if (String(out.sheet_name || '').trim() && String(out.sheet_name || '').trim() !== rawName) {
          sheet_name.value = String(out.sheet_name || '').trim();
          rememberResolvedSheetName(rawUrl, out.sheet_name);
        }
        return {
          columns: Array.isArray(out.columns) ? out.columns : [],
          counts: out.counts && typeof out.counts === 'object' ? out.counts : {},
          drive_columns: Array.isArray(out.drive_columns) ? out.drive_columns : [],
          samples: out.samples && typeof out.samples === 'object' ? out.samples : {},
        };
      })();
    }
    const payload = await sheetColumnSuggestInflight[cacheKey];
    sheetColumnSuggestCache[cacheKey] = { ...payload, ts: Date.now() };
    const total = Array.isArray(payload.columns) ? payload.columns.length : Object.keys(payload.counts || {}).length;
    setSheetNameHint(t('sheetLinkCellHintCountFmt')(total));
  } catch (e) {
    const stale = getCachedSheetLinkColumns(rawUrl, rawName, startRow, true);
    if (stale) {
      const totalStale = Array.isArray(stale.columns) ? stale.columns.length : Object.keys(stale.counts || {}).length;
      setSheetNameHint(t('sheetLinkCellHintCountFmt')(totalStale));
    } else {
      setSheetNameHint(e.message, true);
    }
  } finally {
    delete sheetColumnSuggestInflight[cacheKey];
  }
}

function scheduleSheetLinkSuggestions(force = false) {
  if (String(currentRunMode || '').toLowerCase() === 'scan') {
    renderSheetLinkSuggestions();
    return;
  }
  if (!sheetLinkSuggestLoadedByMode[String(currentRunMode || 'seeding').toLowerCase()]) {
    renderSheetLinkSuggestions();
    return;
  }
  if (sheetColumnSuggestTimer) clearTimeout(sheetColumnSuggestTimer);
  sheetColumnSuggestTimer = setTimeout(() => {
    fetchSheetLinkSuggestions(force);
  }, force ? 0 : 800);
}

function scheduleSheetLinkCountSummary(force = false) {
  if (sheetLinkSummaryTimer) clearTimeout(sheetLinkSummaryTimer);
  sheetLinkSummaryTimer = setTimeout(() => {
    refreshSheetLinkCountSummary(force);
  }, force ? 0 : 500);
}

function applySuggestedSheetColumn(column) {
  const target = activeSheetColumnTarget;
  if (!target) {
    alert(t('sheetLinkSuggestHelp'));
    return;
  }
  const blocks = ensureMappingBlocks(target.mode);
  if (!blocks[target.index]) {
    activeSheetColumnTarget = null;
    renderSheetLinkSuggestions();
    return;
  }
  blocks[target.index][target.key] = String(column || '').trim().toUpperCase();
  renderMappingEditor();
  renderSheetLinkSuggestions();
  requestAnimationFrame(() => {
    const input = document.getElementById(getMappingFieldInputId(target.mode, target.index, target.key));
    if (input) input.focus();
  });
}

function updateMappingBlock(mode, index, key, value) {
  const blocks = ensureMappingBlocks(mode);
  if (!blocks[index]) return;
  blocks[index][key] = key === 'start_line' ? Number(value || 4) : String(value || '');
  if (String(mode || '').toLowerCase() === currentRunMode && String(key || '').toLowerCase() === 'start_line') {
    resetSheetLinkSuggestions();
  }
}

function removeMappingBlock(index) {
  const blocks = ensureMappingBlocks(currentRunMode);
  if (blocks.length <= 1) return;
  blocks.splice(index, 1);
  renderMappingEditor();
}

function addMappingBlock() {
  const blocks = ensureMappingBlocks(currentRunMode);
  blocks.push(defaultMappingBlock(currentRunMode, blocks.length + 1));
  pendingMappingScrollMode = currentRunMode;
  pendingMappingHighlightIndex = blocks.length - 1;
  renderMappingEditor();
}

function toggleCaptureFivePerLink(checked) {
  captureFivePerLink = !!checked;
  rememberCurrentRunFlags(currentRunMode);
}

function getModeBasePort(mode = currentRunMode) {
  return Number(BROWSER_PORT_BY_MODE[String(mode || 'seeding').toLowerCase()] || BROWSER_PORT_BY_MODE.seeding);
}

function getChromePortForBlock(index, mode = currentRunMode) {
  const basePort = getModeBasePort(mode);
  return Number(index) <= 0 ? basePort : basePort + 100 + Number(index);
}

function openAirDatePicker(mode, index) {
  const picker = document.getElementById(`air_date_picker_${mode}_${index}`);
  if (!picker) return;
  if (typeof picker.showPicker === 'function') picker.showPicker();
  else picker.click();
}

function applyAirDate(mode, index, value) {
  updateMappingBlock(mode, index, 'col_air_date', value || '');
  renderMappingEditor();
}

function isLocalWebHost() {
  return isConfiguredLocalBrowserHost(window.location.hostname);
}

function launchChromeViaLocalProtocol(mode, index, port) {
  const runMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  const blockIndex = Number(index) || 0;
  const resolvedPort = Number(port) || getChromePortForBlock(blockIndex, runMode);
  const href = `tool-evidence://launch?mode=${encodeURIComponent(runMode)}&block=${blockIndex}&port=${resolvedPort}`;
  const frame = document.createElement('iframe');
  frame.style.display = 'none';
  frame.src = href;
  document.body.appendChild(frame);
  window.setTimeout(() => frame.remove(), 1500);
  return { href, port: resolvedPort };
}

async function launchChromeBlock(index, mode = currentRunMode, explicitPort = null) {
  try {
    const runMode = String(mode || currentRunMode || 'seeding').toLowerCase();
    const blockIndex = Number(index) || 0;
    const port = Number(explicitPort) || getChromePortForBlock(blockIndex, runMode);
    const previousMode = currentRunMode;
    currentRunMode = runMode;
    const blockName = getBlockActivityName(blockIndex);
    currentRunMode = previousMode;
    if (!isLocalWebHost()) {
      await logActivityEvent({
        kind: 'login',
        level: 'info',
        run_mode: runMode,
        block_name: blockName,
        browser_port: port,
        message: `${blockName}: đã gửi lệnh mở Chrome ${port} trên máy cục bộ`,
      });
      const local = launchChromeViaLocalProtocol(runMode, blockIndex, port);
      setStatus(`Đã gửi lệnh mở Chrome ${local.port} tới máy của bạn`, 'running');
      return;
    }
    const out = await req(`/api/chrome/launch-block/${blockIndex}?run_mode=${encodeURIComponent(runMode)}&browser_port=${port}`, { method: 'POST' });
    await logActivityEvent({
      kind: 'login',
      level: 'info',
      run_mode: runMode,
      block_name: blockName,
      browser_port: Number(out?.browser_port || port),
      message: `${blockName}: đã mở Chrome ${Number(out?.browser_port || port)} để đăng nhập`,
    });
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) {
    alert(e.message);
  }
}

function renderMappingEditor() {
  const blocks = ensureMappingBlocks(currentRunMode);
  const fields = mappingFieldsForMode(currentRunMode);
  const host = document.getElementById('mappingBlocks');
  const addButton = document.getElementById('mappingAddButton');
  if (addButton) addButton.textContent = t('addBlock');
  if (!host) return;
  if (currentRunMode === 'scan') {
    host.innerHTML = `<div class="mapping-seeding-row">${blocks.map((block, index) => {
      const blockClass = pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex === index
        ? 'mapping-block mapping-block-new'
        : 'mapping-block';
      const title = block.name || `Scan ${index + 1}`;
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        const inputType = field.type === 'number' ? 'number' : 'text';
        const inputId = getMappingFieldInputId(currentRunMode, index, field.key);
        const listAttr = isLinkSuggestionField(currentRunMode, field.key) ? ' list="sheet_link_column_datalist"' : '';
        const focusAttr = isLinkSuggestionField(currentRunMode, field.key) ? ` onfocus="setSheetColumnTarget('${currentRunMode}', ${index}, '${field.key}')"` : '';
        return `<div class="mapping-label">${esc(field.label)}</div><div><input id="${esc(inputId)}" class="mapping-input" type="${inputType}" value="${esc(value)}"${listAttr}${focusAttr} oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      return `<section class="${blockClass}">
        <div class="mapping-block-head">
          <div class="mapping-block-title">${esc(title)}</div>
          ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
        </div>
        <div class="mapping-block-grid">${rows}</div>
      </section>`;
    }).join('')}</div>`;
  } else if (currentRunMode === 'seeding' || currentRunMode === 'booking') {
    host.innerHTML = `<div class="mapping-seeding-row">${blocks.map((block, index) => {
      const blockClass = pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex === index
        ? 'mapping-block mapping-block-new'
        : 'mapping-block';
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        const inputId = getMappingFieldInputId(currentRunMode, index, field.key);
        const listAttr = isLinkSuggestionField(currentRunMode, field.key) ? ' list="sheet_link_column_datalist"' : '';
        const focusAttr = isLinkSuggestionField(currentRunMode, field.key) ? ` onfocus="setSheetColumnTarget('${currentRunMode}', ${index}, '${field.key}')"` : '';
        if (field.key === 'col_air_date') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input id="${esc(inputId)}" class="mapping-input" type="text" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" onclick="openAirDatePicker('${currentRunMode}', ${index})">...</button><input id="air_date_picker_${currentRunMode}_${index}" type="date" style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px" onchange="applyAirDate('${currentRunMode}', ${index}, this.value)" /></div>`;
        }
        const inputType = field.type === 'number' ? 'number' : 'text';
        if (field.key === 'name') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input id="${esc(inputId)}" class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" />${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}</div>`;
        }
        return `<div class="mapping-label">${esc(field.label)}</div><div><input id="${esc(inputId)}" class="mapping-input" type="${inputType}" value="${esc(value)}"${listAttr}${focusAttr} oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      const chromePort = getChromePortForBlock(index, currentRunMode);
        const chromeRow = `<div class="mapping-label">${esc(t('chrome'))}</div><div><button class="btn mapping-chrome-btn" type="button" onclick="launchChromeBlock(${index}, '${currentRunMode}', ${chromePort})">${esc(`${t('chrome')} ${chromePort}`)}</button></div>`;
      return `<section class="${blockClass}"><div class="mapping-block-grid">${rows}${chromeRow}</div></section>`;
    }).join('')}</div>`;
  }
  const addRow = document.querySelector('.mapping-add-row');
  if (addRow) {
    addRow.classList.toggle('booking', currentRunMode === 'booking');
    const bookingExtra = currentRunMode === 'booking'
      ? `<label class="mapping-toggle-card">
          <span class="mapping-toggle-copy">
            <span class="mapping-toggle-title">${esc(t('captureFive'))}</span>
            <span class="mapping-toggle-help">${esc(t('captureFiveHelp'))}</span>
          </span>
          <span class="mapping-toggle-switch">
            <input type="checkbox" ${captureFivePerLink ? 'checked' : ''} onchange="toggleCaptureFivePerLink(this.checked)" />
            <span class="mapping-toggle-slider"></span>
          </span>
        </label>`
      : '';
    addRow.innerHTML = `<button id="mappingAddButton" class="btn" type="button" onclick="addMappingBlock()">${esc(t('addBlock'))}</button>${bookingExtra}`;
  }
  if (pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex >= 0) {
    const row = host.querySelector('.mapping-seeding-row');
    const target = row && row.children ? row.children[pendingMappingHighlightIndex] : null;
    requestAnimationFrame(() => {
      if (row && target) {
        row.scrollTo({ left: target.offsetLeft - 8, behavior: 'smooth' });
      }
      pendingMappingScrollMode = '';
      pendingMappingHighlightIndex = -1;
    });
  } else {
    pendingMappingScrollMode = '';
    pendingMappingHighlightIndex = -1;
  }
  renderSheetLinkSuggestions();
}

function applyRunModeUI() {
  ['seeding', 'booking', 'scan'].forEach(mode => {
    const node = document.getElementById('run_mode_' + mode);
    if (node) {
      node.classList.toggle('active', currentRunMode === mode);
      node.textContent = t(mode);
    }
  });
  const runTitle = document.getElementById('runTitleText');
  if (runTitle) runTitle.textContent = formatRunTitle(currentRunMode);
  const scanNegativeFilterCard = document.getElementById('scanNegativeFilterCard');
  if (scanNegativeFilterCard) scanNegativeFilterCard.style.display = currentRunMode === 'scan' ? '' : 'none';
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', document.getElementById('view-runs')?.classList.contains('active'));
  renderMappingEditor();
  renderSheetLinkSuggestions();
}

function applyLanguage() {
  document.documentElement.lang = currentLang === 'vi' ? 'vi' : 'en';
  const langToggle = document.getElementById('lang_toggle');
  if (langToggle) {
    langToggle.textContent = currentLang === 'vi' ? 'VN' : 'EN';
    langToggle.title = currentLang === 'vi' ? 'Switch to English' : 'Chuyen sang tieng Viet';
    langToggle.setAttribute('aria-label', currentLang === 'vi' ? 'Switch to English' : 'Chuyen sang tieng Viet');
  }
  const themeToggle = document.getElementById('theme_toggle');
  if (themeToggle) {
    const nextLabel = currentTheme === 'dark' ? t('light') : t('dark');
    themeToggle.title = `${t('light')} / ${t('dark')}`;
    themeToggle.setAttribute('aria-label', `${t('light')} / ${t('dark')} (${nextLabel})`);
  }
  const topSearch = document.getElementById('top_search');
  if (topSearch) topSearch.placeholder = t('searchPlaceholder');
  const launchChromeBtn = document.getElementById('btn_launch_chrome');
  if (launchChromeBtn) launchChromeBtn.textContent = t('launchChrome');
  const refreshJobsBtn = document.getElementById('btn_refresh_jobs');
  if (refreshJobsBtn) refreshJobsBtn.textContent = t('refresh');

  const menuMap = { runs: 'runs', projects: 'projects', tasks: 'tasks', activities: 'activities', access: 'access', settings: 'settings' };
  Object.entries(menuMap).forEach(([view, key]) => {
    const node = document.querySelector(`.side-btn[data-view="${view}"] span:last-child`);
    if (node) node.textContent = t(key);
  });

  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value;
  };
  const setNthText = (selector, index, value) => {
    const nodes = document.querySelectorAll(selector);
    if (nodes[index]) nodes[index].textContent = value;
  };
  const setFirstChildText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el && el.childNodes && el.childNodes[0]) el.childNodes[0].textContent = value;
  };
  setText('#logoutLabel', t('logout'));
  setText('#authRoleBadge', getRoleLabel());
  setText('#view-overview .h1', t('overview'));
  setText('#runTitleText', formatRunTitle());
  setText('#view-projects .h1', t('projects'));
  setText('#view-activities .h1', t('activities'));
  setText('#view-access .h1', t('access'));
  setText('#view-settings .h1', t('settings'));
  setText('#view-projects .state', t('projectsState'));
  setText('#view-activities .state', t('activitiesState'));
  setText('#view-access .state', t('accessState'));
  setText('#view-settings .state', t('settingsState'));
  setText('#view-runs .state', t('runConfigHelp'));

  setText('#ovSavedProjectsLabel', t('groupedProjects'));
  setText('#ovSavedSheetsLabel', t('completedGroups'));
  setText('#ovSelectedProjectLabel', t('largestGroup'));
  setText('#ovHistoryTitle', t('overviewTimeline'));
  setText('#ovLegendSuccess', t('overviewCompletedLegend'));
  setText('#ovLegendFailed', t('overviewFailedLegend'));
  setText('#ovLegendUnavailable', t('overviewUnavailableLegend'));
  setText('#ovModeSplitTitle', t('overviewModeSplit'));
  setText('#ovModeSplitSub', t('overviewModeSplitSub'));
  setText('#overviewRunCtaLabel', t('goToRuns'));
  setText('#runSummaryTitle', t('runSummary'));
  setText('#runSummarySub', t('overviewClean'));
  setText('#view-overview .item:nth-child(1) .t', t('selectedJob'));
  setText('#view-overview .item:nth-child(1) .btn', t('openRuns'));
  setText('#view-overview .item:nth-child(2) .t', t('storedJobs'));
  setText('#overviewSyncLabel', t('sync'));
  setText('#view-overview .item:nth-child(3) .t', t('successFailed'));
  setText('#view-overview .item:nth-child(3) .btn', t('view'));
  setText('#view-overview .mini > div span:first-child', t('overallProgress'));
  renderOverviewGreeting();
  setNthText('#view-overview .day', 0, t('totalScope'));
  setNthText('#view-overview .day', 1, t('done'));
  setNthText('#view-overview .day', 2, t('success'));
  setNthText('#view-overview .day', 3, t('failed'));
  setNthText('#view-overview .day', 4, t('jobs'));

  setText('#view-runs .headline .state', t('runConfigHelp'));
  setText('#runShareLabel', t('runShareLabel'));
  applyRunModeUI();
  setText('label[for="sheet_url"]', t('sheetUrl'));
  setText('label[for="sheet_name"]', t('sheetName'));
  setText('label[for="drive_id"]', t('driveFolder'));
  setText('#startJobLabel', t('startJob'));
  setText('#pauseJobLabel', t('stopJob'));
  setText('#continueJobLabel', t('continueJob'));
  setText('#errorOnlyJobLabel', t('errorOnlyJob'));
  setText('#overwriteRunLabel', t('overwriteRun'));
  setText('#overwriteRunHelp', t('overwriteRunHelp'));
  setText('#highlightSheetErrorsLabel', t('highlightSheetErrors'));
  setText('#highlightSheetErrorsHelp', t('highlightSheetErrorsHelp'));
  setText('#scanNegativeFilterLabel', t('scanNegativeFilter'));
  setText('#scanNegativeFilterHelp', t('scanNegativeFilterHelp'));
  setText('#sheet_link_suggest_title', t('sheetLinkSuggestTitle'));
  setText('#runMonitorKicker', t('monitorKicker'));
  setText('#runMonitorJobLabel', t('monitorJob'));
  setText('#runMonitorProgressLabel', t('monitorProgress'));
  setText('#runMonitorErrorLabel', t('monitorErrors'));
  setText('#runMonitorIssueRowsLabel', t('monitorIssueRowsLabel'));
  setText('#runMonitorUnavailableRowsLabel', t('monitorIssueUnavailableRowsLabel'));
  setText('#runMonitorErrorRows', '-');
  setText('#runMonitorUnavailableRows', '-');
  setText('#runMonitorErrorMeta', t('monitorSuccessFailedFmt')(0, 0, 0));
  setText('#runMonitorTableTitle', t('monitorTable'));
  setText('#runMonitorHeadTime', t('time'));
  setText('#runMonitorHeadPost', t('post'));
  setText('#runMonitorHeadResult', t('result'));
  setText('#runMonitorHeadMessage', t('message'));
  setText('#runMonitorHeadReplay', t('replay'));
  setText('#exportLogLabel', t('exportLog'));
  updateRunActionButtons();

  setText('#view-projects .cards-3 .card:nth-child(1) .k', t('groupedProjects'));
  setText('#view-projects .cards-3 .card:nth-child(2) .k', t('completedGroups'));
  setText('#view-projects .cards-3 .card:nth-child(3) .k', t('largestGroup'));
  setText('#projectsListTitle', t('groupedRegistry'));
  setText('#projectsSnapshotTitle', t('groupSnapshot'));
  setText('#view-activities .card > div:first-child', t('recentTimeline'));

  setText('#accessMailTitle', t('accessMailTitle'));
  setText('#accessMailHelp', t('accessMailHelp'));
  setText('#accessMailSenderLabel', t('accessMailSenderLabel'));
  setText('#accessMailFromLabel', t('accessMailFromLabel'));
  setText('#accessMailPasswordLabel', t('accessMailPasswordLabel'));
  setText('#saveMailConfigButton', t('accessMailSave'));
  setText('#hideMailConfigButton', t('accessMailHide'));
  setText('#accessEntryTitle', t('accessEntryTitle'));
  setText('#accessEntryHelp', t('accessEntryHelp'));
  setText('#accessEntryEmailLabel', t('accessEntryEmailLabel'));
  setText('#accessEntryRoleLabel', t('accessEntryRoleLabel'));
  setText('#accessEntryTypeLabel', t('accessEntryTypeLabel'));
  setText('#accessEntryCancelTop', t('accessEntryCancel'));
  setText('#accessEntryCancelButton', t('accessEntryCancel'));
  setText('#accessEntrySaveButton', t('accessEntrySave'));
  renderSheetLinkSuggestions();
  const accessEntryRole = document.getElementById('access_entry_role');
  if (accessEntryRole?.options?.[0]) accessEntryRole.options[0].text = t('roleUser');
  if (accessEntryRole?.options?.[1]) accessEntryRole.options[1].text = t('roleAdmin');
  const accessEntryType = document.getElementById('access_entry_type');
  if (accessEntryType?.options?.[0]) accessEntryType.options[0].text = t('accessTypeInternal');
  if (accessEntryType?.options?.[1]) accessEntryType.options[1].text = t('accessTypeExternal');
  setText('#accessDirectoryTitle', t('accessDirectoryTitle'));
  setText('#accessDirectoryHelp', t('accessDirectoryHelp'));
  setText('#accessFilterRoleLabel', t('accessFilterRole'));
  setText('#accessFilterScopeLabel', t('accessFilterScope'));
  setText('#accessFilterTypeLabel', t('accessFilterType'));
  setText('#accessRoleFilterAll', t('accessFilterAll'));
  setText('#accessRoleFilterAdmin', t('accessFilterAdmin'));
  setText('#accessRoleFilterUser', t('accessFilterUser'));
  setText('#accessScopeFilterAll', t('accessFilterAll'));
  setText('#accessScopeFilterAllowed', t('accessScopeAllowed'));
  setText('#accessScopeFilterAdmin', t('accessScopeAdmin'));
  setText('#accessScopeFilterOpen', t('accessScopeOpen'));
  setText('#accessTypeFilterAll', t('accessFilterAll'));
  setText('#accessTypeFilterInternal', t('accessFilterInternal'));
  setText('#accessTypeFilterExternal', t('accessFilterExternal'));
  setText('#accessTableHeadEmail', t('accessTableEmail'));
  setText('#accessTableHeadAccess', t('accessTableAccess'));
  setText('#accessTableHeadRole', t('accessTableRole'));
  setText('#accessTableHeadType', t('accessTableType'));
  setText('#accessTableHeadStatus', t('accessTableStatus'));
  setText('#accessTableHeadUpdated', t('accessTableUpdated'));
  setText('#accessTableHeadActions', t('accessTableActions'));
  setText('#accessQuickAddButton', t('accessQuickAdd'));
  setText('#accessSummaryTitle', t('accessSummaryTitle'));
  const accessSearchInput = document.getElementById('accessDirectorySearch');
  if (accessSearchInput) accessSearchInput.placeholder = t('accessDirectorySearchPlaceholder');
  renderMailConfig(currentMailConfig);
  renderAccessEntryEditor();

  setText('#view-settings .settings-layout .card:first-child > div:first-child', t('settingsTitle'));
  setText('#view-settings .settings-layout .card:first-child > div:nth-child(2)', t('settingsHelp'));
  setText('label[for="settings_viewport_width"]', t('viewportWidth'));
  setText('label[for="settings_viewport_height"]', t('viewportHeight'));
  setText('label[for="settings_page_timeout_ms"]', t('pageTimeout'));
  setText('#settingsScanNegativeTermsLabel', t('scanNegativeTermsLabel'));
  setText('#settings_scan_negative_terms_help', t('scanNegativeTermsHelp'));
  const settingsNegativeTerms = document.getElementById('settings_scan_negative_terms');
  if (settingsNegativeTerms) settingsNegativeTerms.placeholder = t('scanNegativeTermsPlaceholder');
  setText('#view-settings .list-row div div:first-child', t('fullPageCapture'));
  setText('#view-settings .list-row .muted', t('fullPageHelp'));
  setText('#view-settings .settings-layout .card:first-child .card > div:first-child', t('jsonServiceAccount'));
  setText('#view-settings .settings-layout .card:first-child .card > div:nth-child(2)', t('jsonHelp'));
  setText('#settingsServiceAccountFileLabel', t('serviceJsonLabel'));
  setText('#settingsServiceAccountJsonLabel', t('serviceJsonPasteLabel'));
  const serviceFileHint = document.getElementById('settings_service_account_file_hint');
  if (serviceFileHint && !serviceFileHint.dataset.fileName) serviceFileHint.textContent = t('serviceJsonNoFile');
  setText('#saveSettingsButton', t('saveSettings'));
  setText('#accessPolicyTitle', t('accessPolicyTitle'));
  setText('#accessPolicyHelp', t('accessPolicyHelp'));
  setText('#accessAllowedLabel', t('accessAllowedLabel'));
  setText('#accessAllowedHelp', t('accessAllowedHelp'));
  setText('#accessAdminLabel', t('accessAdminLabel'));
  setText('#accessAdminHelp', t('accessAdminHelp'));
  setText('#saveAccessButton', t('saveAccessPolicy'));
  setText('#reloadAccessButton', t('reloadAccessPolicy'));
  setText('#view-settings .settings-layout aside > div:first-child', t('currentConfigSummary'));
  const summaryTitles = document.querySelectorAll('#view-settings .settings-layout aside .timeline-item strong');
  if (summaryTitles[0]) summaryTitles[0].textContent = t('viewport');
  if (summaryTitles[1]) summaryTitles[1].textContent = t('timeout');
  if (summaryTitles[2]) summaryTitles[2].textContent = t('output');
  if (summaryTitles[3]) summaryTitles[3].textContent = t('serviceAccount');
  if (summaryTitles[4]) summaryTitles[4].textContent = t('sharingNote');
  const shareHelp = document.querySelector('#view-settings .settings-layout aside .timeline-item:last-child div');
  if (shareHelp) shareHelp.textContent = t('sharingHelp');
  renderRunShareInfo(currentSettingsCache);
  renderAccessDirectory(currentAccessPolicy);
  renderAccessPolicySummary(currentAccessPolicy);
  syncAuthUI();
}

function applyTheme() {
  document.documentElement.setAttribute('data-theme', currentTheme);
  const themeToggle = document.getElementById('theme_toggle');
  if (themeToggle) {
    themeToggle.setAttribute('data-mode', currentTheme);
    const nextLabel = currentTheme === 'dark' ? t('light') : t('dark');
    themeToggle.title = `${t('light')} / ${t('dark')}`;
    themeToggle.setAttribute('aria-label', `${t('light')} / ${t('dark')} (${nextLabel})`);
  }
}

function setTheme(theme) {
  currentTheme = theme === 'dark' ? 'dark' : 'light';
  localStorage.setItem('ui_theme', currentTheme);
  applyTheme();
}

function toggleTheme() {
  setTheme(currentTheme === 'dark' ? 'light' : 'dark');
}

function setRunMode(mode) {
  rememberCurrentRunFlags(currentRunMode);
  const nextMode = String(mode || 'seeding').toLowerCase();
  currentRunMode = ['seeding', 'booking', 'scan'].includes(nextMode) ? nextMode : 'seeding';
  applyRunFlagsForMode(currentRunMode);
  resetSheetLinkSuggestions(currentRunMode);
  currentJobId = resolveModeJobId(currentRunMode);
  applyRunModeUI();
}

function openRunMode(mode) {
  switchView('runs');
  setRunMode(mode);
  if (currentJobId) {
    pollCurrent();
  } else {
    currentJobSnapshot = null;
    currentLogsCache = [];
    renderRunMonitor(null, []);
  }
}

function setLanguage(lang) {
  currentLang = lang === 'en' ? 'en' : 'vi';
  localStorage.setItem('ui_lang', currentLang);
  applyLanguage();
  renderOverview();
  renderProjects();
  renderActivities(getCombinedActivities());
  renderRunMonitor(currentJobSnapshot, currentLogsCache);
  if (String(document.getElementById('sheet_url')?.value || '').trim()) scheduleSheetNameSuggestions(false);
}

function toggleLanguage() {
  setLanguage(currentLang === 'vi' ? 'en' : 'vi');
}

async function req(url, opts = {}) {
  const useLocalAgent = shouldUseLocalAgent(url);
  if (useLocalAgent) return agentReq(url, opts);
  const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
  const res = await fetch(url, { ...opts, headers });
  const data = await res.json().catch(() => ({}));
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error(data.detail || 'Authentication required');
  }
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

async function loadAuthState() {
  const data = await req('/api/auth/me');
  authState.email = String(data.email || '').trim();
  authState.role = String(data.role || 'user').trim().toLowerCase() === 'admin' ? 'admin' : 'user';
  authState.isAdmin = !!data.is_admin || authState.role === 'admin';
  return data;
}

async function logActivityEvent(payload = {}) {
  try {
    const out = await req('/api/activity', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    if (out?.item) {
      currentActivityEvents = [out.item, ...(currentActivityEvents || [])];
      renderActivities(getCombinedActivities());
    }
    return out?.item || null;
  } catch (_e) {
    return null;
  }
}

function getBlockActivityName(index) {
  const block = ensureMappingBlocks(currentRunMode)[Number(index) || 0] || {};
  return String(block?.name || '').trim() || `Post ${Number(index) + 1}`;
}

async function logoutAuth() {
  try {
    await fetch('/api/auth/logout', { method: 'POST' });
  } finally {
    window.location.href = '/login';
  }
}

function esc(s) {
  return String(s || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
}

function toLocalStamp(iso) {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit'
  }).format(d);
}

function toCalendarDayKey(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function toLocalDayLabel(value) {
  if (!value) return '-';
  let d = null;
  if (/^\\d{4}-\\d{2}-\\d{2}$/.test(String(value))) {
    const [year, month, day] = String(value).split('-').map(Number);
    d = new Date(year, month - 1, day);
  } else {
    d = new Date(value);
  }
  if (Number.isNaN(d.getTime())) return String(value);
  return new Intl.DateTimeFormat(currentLang === 'vi' ? 'vi-VN' : 'en-GB', {
    day: '2-digit',
    month: '2-digit'
  }).format(d);
}

function getJobTimelineStamp(job) {
  return job?.finished_at || job?.created_at || '';
}

function getTerminalLogStats(job) {
  const logs = Array.isArray(job?.logs) ? job.logs : [];
  if (!logs.length) {
    const summary = getJobSummary(job);
    return {
      success: Number(summary.success || 0),
      failed: Number(summary.failed || 0),
      unavailable: Number(summary.unavailable || 0),
    };
  }
  let success = 0;
  let failed = 0;
  let unavailable = 0;
  logs.forEach(log => {
    const tag = String(log?.tag || '').toLowerCase();
    const state = String(log?.state || '').toLowerCase();
    const result = String(log?.result || '').toLowerCase();
    const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
    if (tag.includes('unavailable') || raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) {
      unavailable += 1;
      return;
    }
    if (state === 'fail' || result === 'fail' || tag.includes('fail')) {
      failed += 1;
      return;
    }
    if (state === 'ok' || result === 'ok' || tag.includes('ok')) {
      success += 1;
    }
  });
  if (!success && !failed && !unavailable) {
    const summary = getJobSummary(job);
    success = Number(summary.success || 0);
    failed = Number(summary.failed || 0);
    unavailable = Number(summary.unavailable || 0);
  }
  return { success, failed, unavailable };
}

function buildOverviewDateBuckets(jobs, limit = 7) {
  const buckets = new Map();
  (jobs || []).forEach(job => {
    const stamp = getJobTimelineStamp(job);
    const key = toCalendarDayKey(stamp);
    if (!key) return;
    const stats = getTerminalLogStats(job);
    const existing = buckets.get(key) || { key, jobs: 0, success: 0, failed: 0, unavailable: 0 };
    existing.jobs += 1;
    existing.success += Number(stats.success || 0);
    existing.failed += Number(stats.failed || 0);
    existing.unavailable += Number(stats.unavailable || 0);
    buckets.set(key, existing);
  });
  return [...buckets.values()].sort((a, b) => a.key.localeCompare(b.key)).slice(-limit);
}

function toDateKeyFromDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getJobSummary(job) {
  const base = job?.summary || {};
  const logs = Array.isArray(job?.logs) ? job.logs : (Array.isArray(job?.recent_logs) ? job.recent_logs : []);
  const touchedRows = new Set();
  const successRows = new Set();
  const failedRows = new Set();
  const unavailableRows = new Set();
  logs.forEach(item => {
    const row = Number(item?.row || 0);
    if (!Number.isFinite(row) || row <= 0) return;
    touchedRows.add(row);
    if (isUnavailableLog(item)) unavailableRows.add(row);
    else if (isFailedLog(item)) failedRows.add(row);
    else if (isSuccessLog(item)) successRows.add(row);
  });
  const done = Math.max(Number(base.done || 0), touchedRows.size);
  const success = Math.max(Number(base.success || 0), successRows.size);
  const failed = Math.max(Number(base.failed || 0), failedRows.size, Object.keys(job?.error_rows || {}).length);
  const unavailable = Math.max(Number(base.unavailable || 0), unavailableRows.size);
  const total = Math.max(
    Number(base.total || 0),
    done,
    success + failed + unavailable
  );
  return {
    done,
    total,
    success,
    failed,
    unavailable,
    eta: String(base.eta || '---'),
  };
}

function getJobSheetLabel(job) {
  const req = job?.request || {};
  return req.sheet_name || req.sheet_url || 'Unknown sheet';
}

function getJobMode(job) {
  return String(job?.mode || job?.request?.mode || job?.request?.mappings?.[0]?.mode || 'seeding').toLowerCase();
}

function getJobOwnerEmail(job) {
  return String(job?.owner_email || job?.request?.owner_email || '').trim().toLowerCase();
}

function isJobOwnedByCurrentUser(job) {
  const viewer = String(authState.email || '').trim().toLowerCase();
  const owner = getJobOwnerEmail(job);
  return !!viewer && !!owner && viewer === owner;
}

function getJobOwnerBadge(job) {
  const owner = getJobOwnerEmail(job);
  if (!owner) return '';
  if (!isAdminUser()) return '';
  if (isJobOwnedByCurrentUser(job)) return '';
  return owner;
}

function getJobRootId(job) {
  const req = job?.request || {};
  return String(req.root_job_id || job?.id || '').trim();
}

function getJobLineageJobs(job) {
  const rootId = getJobRootId(job);
  if (!rootId) return job ? [job] : [];
  return (jobsCache || [])
    .filter(item => getJobRootId(item) === rootId)
    .sort((a, b) => {
      const at = Date.parse(String(a?.created_at || '')) || 0;
      const bt = Date.parse(String(b?.created_at || '')) || 0;
      return at - bt;
    });
}

function getLineageDisplayLogs(snapshot, logs) {
  const currentLogs = Array.isArray(logs) ? logs : [];
  if (!snapshot) return currentLogs;
  const lineageJobs = getJobLineageJobs(snapshot);
  if (lineageJobs.length <= 1) return currentLogs;
  const out = [];
  const seen = new Set();
  lineageJobs.forEach(job => {
    const sourceLogs = job?.id === snapshot?.id
      ? currentLogs
      : (Array.isArray(job?.logs) ? job.logs : (Array.isArray(job?.recent_logs) ? job.recent_logs : []));
    sourceLogs.forEach(item => {
      const key = [
        String(job?.id || ''),
        String(item?.ts || ''),
        String(item?.row || ''),
        String(item?.state || ''),
        String(item?.result || ''),
        String(item?.tag || ''),
        String(item?.message || ''),
      ].join('|');
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ ...item, __job_id: job?.id || '' });
    });
  });
  out.sort((a, b) => {
    const at = Date.parse(String(a?.ts || '')) || 0;
    const bt = Date.parse(String(b?.ts || '')) || 0;
    if (at !== bt) return at - bt;
    return Number(a?.row || 0) - Number(b?.row || 0);
  });
  return out;
}

function getJobsByMode(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  return (jobsCache || []).filter(job => getJobMode(job) === key);
}

function getSelectedJobIdForMode(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  return currentJobIdByMode[key] || null;
}

function setSelectedJobIdForMode(mode, jobId) {
  const key = String(mode || 'seeding').toLowerCase();
  currentJobIdByMode[key] = jobId || null;
}

function isActiveJobStatus(status) {
  const value = String(status || '').toLowerCase();
  return ['queued', 'running', 'paused'].includes(value);
}

function sortJobsByRecency(jobs) {
  return [...(jobs || [])].sort((a, b) => {
    const at = Date.parse(String(a?.created_at || a?.finished_at || '')) || 0;
    const bt = Date.parse(String(b?.created_at || b?.finished_at || '')) || 0;
    return bt - at;
  });
}

function resolveModeJobId(mode) {
  const jobs = sortJobsByRecency(getJobsByMode(mode));
  if (!jobs.length) return null;
  const ownActive = jobs.find(job => isJobOwnedByCurrentUser(job) && isActiveJobStatus(job?.status));
  if (ownActive) return ownActive.id;
  const selected = getSelectedJobIdForMode(mode);
  const matched = selected ? jobs.find(job => job.id === selected) : null;
  if (matched) return matched.id;
  const ownJob = jobs.find(isJobOwnedByCurrentUser);
  if (ownJob) return ownJob.id;
  const activeJob = jobs.find(job => isActiveJobStatus(job?.status));
  return activeJob ? activeJob.id : jobs[0].id;
}

function extractBlockingJobId(message) {
  const text = String(message || '');
  const match = text.match(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i);
  return match ? match[0] : '';
}

async function focusBlockingModeJob(err, requestedMode = currentRunMode) {
  const blockingJobId = extractBlockingJobId(err?.message);
  if (!blockingJobId) return false;
  await refreshJobs();
  const blockingJob = (jobsCache || []).find(job => job.id === blockingJobId);
  if (!blockingJob) return false;
  const blockingMode = getJobMode(blockingJob) || requestedMode;
  setSelectedJobIdForMode(blockingMode, blockingJobId);
  currentJobId = blockingJobId;
  if (currentRunMode !== blockingMode) {
    setRunMode(blockingMode);
  }
  await pollCurrent();
  setStatus(`${prettyWord(blockingMode)}: đang có job chạy · ${blockingJobId.slice(0, 8)}`, 'running');
  return true;
}

function syncModeSelections() {
  ['seeding', 'booking', 'scan'].forEach(mode => {
    setSelectedJobIdForMode(mode, resolveModeJobId(mode));
  });
}

function getSavedProjectJobs() {
  return (jobsCache || []).filter(job => {
    const status = String(job?.status || '').toLowerCase();
    if (['queued', 'running', 'paused', 'completed'].includes(status)) return true;
    if (!['stopped', 'failed'].includes(status)) return false;
    const summary = getJobSummary(job);
    return (
      summary.done > 0 ||
      summary.total > 0 ||
      summary.success > 0 ||
      summary.failed > 0 ||
      summary.unavailable > 0 ||
      !!String(job?.detail || '').trim() ||
      (Array.isArray(job?.recent_logs) && job.recent_logs.length > 0) ||
      (Array.isArray(job?.logs) && job.logs.length > 0)
    );
  });
}

function getProjectJobsForModeFilter() {
  const saved = getSavedProjectJobs();
  if (currentProjectModeFilter === 'all') return saved;
  return saved.filter(job => getJobMode(job) === currentProjectModeFilter);
}

function matchesProjectStatusFilter(job, statusFilter = currentProjectStatusFilter) {
  const normalized = String(statusFilter || 'all').toLowerCase();
  if (normalized === 'all') return true;
  const status = String(job?.status || '').toLowerCase();
  if (normalized === 'running') return ['queued', 'running', 'paused'].includes(status);
  return status === normalized;
}

function getFilteredProjectJobs() {
  const saved = getProjectJobsForModeFilter();
  if (currentProjectStatusFilter === 'all') return saved;
  return saved.filter(job => matchesProjectStatusFilter(job, currentProjectStatusFilter));
}

function getSelectedProjectJob() {
  const saved = getFilteredProjectJobs();
  if (!saved.length) {
    currentProjectJobId = null;
    return null;
  }
  const matched = currentProjectJobId ? saved.find(job => job.id === currentProjectJobId) : null;
  if (matched) return matched;
  currentProjectJobId = saved[0].id;
  return saved[0];
}

function selectProject(jobId) {
  currentProjectJobId = jobId || null;
  renderProjects();
}

function setProjectModeFilter(mode) {
  currentProjectModeFilter = String(mode || 'all').toLowerCase();
  currentProjectJobId = null;
  renderProjects();
}

function setProjectStatusFilter(status) {
  currentProjectStatusFilter = String(status || 'all').toLowerCase();
  currentProjectJobId = null;
  renderProjects();
}

function expandProjectFilter(select) {
  if (!select) return;
  const optionCount = Math.max(Number(select.options?.length || 0), 0);
  const expandedSize = Math.max(2, Math.min(optionCount || 2, 6));
  select.size = expandedSize;
  select.classList.add('project-filter-input-open');
}

function collapseProjectFilter(select, delay = 120) {
  if (!select) return;
  window.setTimeout(() => {
    if (document.activeElement === select) return;
    select.size = 1;
    select.classList.remove('project-filter-input-open');
  }, delay);
}

function handleProjectFilterKeydown(event) {
  if (!event || event.key !== 'Escape') return;
  const select = event.currentTarget;
  collapseProjectFilter(select, 0);
  if (select && typeof select.blur === 'function') select.blur();
}

function getActivityLogsFromJobs() {
  const rows = [];
  (jobsCache || []).forEach(job => {
    const logs = Array.isArray(job?.recent_logs) ? job.recent_logs : [];
    logs.forEach(item => {
      rows.push({
        ...item,
        __job_id: String(job?.id || ''),
        __job_mode: getJobMode(job),
        owner_email: getJobOwnerEmail(job),
      });
    });
  });
  rows.sort((a, b) => {
    const left = new Date(a?.ts || 0).getTime();
    const right = new Date(b?.ts || 0).getTime();
    return right - left;
  });
  return rows;
}

function getCombinedActivities() {
  const jobLogs = getActivityLogsFromJobs().map(item => ({ ...item, __source: 'job' }));
  const manualEvents = (currentActivityEvents || []).map(item => ({
    ...item,
    row: item?.row ?? '-',
    state: String(item?.kind || 'action').toUpperCase(),
    result: String(item?.run_mode || 'manual').toUpperCase(),
    __source: 'activity',
    __job_id: String(item?.job_id || ''),
    __job_mode: String(item?.run_mode || ''),
    owner_email: String(item?.owner_email || '').trim().toLowerCase(),
  }));
  return [...jobLogs, ...manualEvents]
    .sort((a, b) => new Date(b?.ts || 0).getTime() - new Date(a?.ts || 0).getTime())
    .slice(0, 20);
}

function openProjectInRuns(jobId) {
  const job = (jobsCache || []).find(item => item.id === jobId);
  if (!job) return;
  const request = job.request || {};
  const mode = getJobMode(job);
  sheet_url.value = request.sheet_url || '';
  sheet_name.value = request.sheet_name || '';
  drive_id.value = request.drive_id || '';
  currentRunFlagsByMode[mode] = {
    ...ensureRunFlagsForMode(mode),
    force_run_all: request.force_run_all !== false,
    highlight_sheet_errors: !!request.highlight_sheet_errors,
    capture_five_per_link: !!request.capture_five_per_link,
  };
  currentMappingBlocksByMode[mode] = (request.mappings || []).length
    ? request.mappings.map((block, index) => sanitizeMappingBlockForMode(mode, block, index + 1))
    : [defaultMappingBlock(mode, 1)];
  setSelectedJobIdForMode(mode, job.id);
  currentJobId = job.id;
  switchView('runs');
  setRunMode(mode);
  currentJobId = job.id;
  pollCurrent();
  setStatus(t('openProjectRunDone'), String(job.status || 'idle').toLowerCase());
}

async function deleteProject(jobId, ev = null) {
  if (ev && typeof ev.stopPropagation === 'function') ev.stopPropagation();
  if (!jobId) return;
  if (!confirm(t('deleteProjectConfirm'))) return;
  try {
    await req('/api/jobs/' + jobId, { method: 'DELETE' });
    if (currentProjectJobId === jobId) currentProjectJobId = null;
    if (currentJobId === jobId) currentJobId = null;
    await refreshJobs();
    renderProjects();
    setStatus(t('deleteProjectDone'), 'stopped');
  } catch (e) {
    alert(e.message);
  }
}

function classifyLog(log) {
  const level = String(log?.level || '').toLowerCase();
  if (level === 'error' || level === 'failed') return 'error';
  if (level === 'warning' || level === 'warn') return 'warning';
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('fail') || raw.includes('error')) return 'error';
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return 'warning';
  if (raw.includes('warn') || raw.includes('quota')) return 'warning';
  return 'info';
}

function prettyWord(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  if (raw.toLowerCase() === 'idle') return t('readyState');
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function showToast(message, type = 'info', title = '') {
  const host = document.getElementById('toastHost');
  if (!host) return;
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <div class="toast-icon">${type === 'success' ? '✓' : '!'}</div>
    <div class="toast-copy">
      <div class="toast-title">${esc(title || t('jobFinishedTitle'))}</div>
      <div class="toast-message">${esc(message)}</div>
    </div>
    <button type="button" class="toast-close" aria-label="Close">×</button>
  `;
  const closeToast = () => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(-8px)';
    setTimeout(() => toast.remove(), 160);
  };
  toast.querySelector('.toast-close')?.addEventListener('click', closeToast);
  host.appendChild(toast);
  setTimeout(closeToast, 5200);
}

function primeCompletionNotifications() {
  try {
    if (!('Notification' in window)) return;
    if (Notification.permission === 'default') Notification.requestPermission().catch(() => {});
  } catch (_) {}
}

function stopCompletionTitleFlash() {
  if (completionTitleFlashTimer) {
    clearInterval(completionTitleFlashTimer);
    completionTitleFlashTimer = null;
  }
  document.title = defaultDocumentTitle;
}

function startCompletionTitleFlash(message) {
  const text = String(message || '').trim();
  if (!text) return;
  completionTitleFlashText = text;
  stopCompletionTitleFlash();
  let toggle = false;
  document.title = `${text} • ${defaultDocumentTitle}`;
  completionTitleFlashTimer = setInterval(() => {
    toggle = !toggle;
    document.title = toggle ? `${completionTitleFlashText} • ${defaultDocumentTitle}` : defaultDocumentTitle;
  }, 1200);
}

function playCompletionAlertTone() {
  try {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) return;
    window.__toolEvidenceAudioCtx = window.__toolEvidenceAudioCtx || new AudioCtx();
    const ctx = window.__toolEvidenceAudioCtx;
    if (ctx.state === 'suspended') ctx.resume().catch(() => {});
    const pattern = [0, 0.22, 0.44];
    pattern.forEach((offset, index) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.value = index === 1 ? 880 : 740;
      gain.gain.value = 0.0001;
      osc.connect(gain);
      gain.connect(ctx.destination);
      const startAt = ctx.currentTime + offset;
      gain.gain.setValueAtTime(0.0001, startAt);
      gain.gain.exponentialRampToValueAtTime(0.16, startAt + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, startAt + 0.18);
      osc.start(startAt);
      osc.stop(startAt + 0.2);
    });
  } catch (_) {}
}

function pushBrowserCompletionNotification(title, message, tag = '') {
  try {
    if (!('Notification' in window) || Notification.permission !== 'granted') return;
    const note = new Notification(title, {
      body: message,
      tag: tag || 'tool-evidence-job-finished',
      renotify: true,
    });
    setTimeout(() => note.close(), 12000);
  } catch (_) {}
}

function showCompletionAlert(job, done, total) {
  const host = document.getElementById('completionAlertHost');
  if (!host) return;
  const title = t('jobFinishedBannerTitle');
  const message = t('jobFinishedToastFmt')(getJobSheetLabel(job), done, total);
  while (host.children.length >= 2) {
    host.lastElementChild?.remove();
  }
  const alertNode = document.createElement('div');
  alertNode.className = 'completion-alert';
  alertNode.innerHTML = `
    <div class="completion-alert-icon">✓</div>
    <div class="completion-alert-copy">
      <div class="completion-alert-kicker">${esc(t('jobFinishedTitle'))}</div>
      <div class="completion-alert-title">${esc(getJobSheetLabel(job))}</div>
      <div class="completion-alert-message">${esc(message)}</div>
      <div class="completion-alert-meta">
        <span class="completion-alert-chip">${esc(prettyWord(getJobMode(job)))}</span>
        <span class="completion-alert-chip">${esc(String(job?.id || '').slice(0, 8))}</span>
        <span class="completion-alert-chip">${esc(`${done}/${total}`)}</span>
      </div>
    </div>
    <button type="button" class="completion-alert-close" title="${esc(t('jobFinishedBannerDismiss'))}" aria-label="${esc(t('jobFinishedBannerDismiss'))}">×</button>
  `;
  const closeAlert = () => {
    alertNode.remove();
    if (!host.children.length) stopCompletionTitleFlash();
  };
  alertNode.querySelector('.completion-alert-close')?.addEventListener('click', closeAlert);
  host.prepend(alertNode);
  setTimeout(closeAlert, 12000);
  playCompletionAlertTone();
  startCompletionTitleFlash(title);
  pushBrowserCompletionNotification(title, message, `job-finished-${String(job?.id || '')}`);
}

window.addEventListener('focus', () => {
  stopCompletionTitleFlash();
});
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) stopCompletionTitleFlash();
});

function processJobLifecycleNotifications(jobs) {
  const nextStatusMemory = {};
  (jobs || []).forEach(job => {
    const jobId = String(job?.id || '').trim();
    if (!jobId) return;
    const status = String(job?.status || '').trim().toLowerCase();
    const previousStatus = String(jobStatusMemory[jobId] || '').trim().toLowerCase();
    nextStatusMemory[jobId] = status;
    const summary = getJobSummary(job);
    const done = Number(summary.done || 0);
    const total = Number(summary.total || 0);
    const completionKey = `${jobId}:${String(job?.finished_at || '')}:${done}/${total}`;
    const isReallyCompleted = status === 'completed' && total > 0 && done >= total;
    if (isReallyCompleted && previousStatus && previousStatus !== 'completed' && !notifiedCompletedJobKeys.has(completionKey)) {
      notifiedCompletedJobKeys.add(completionKey);
      showToast(t('jobFinishedToastFmt')(getJobSheetLabel(job), done, total), 'success', t('jobFinishedTitle'));
      showCompletionAlert(job, done, total);
    }
  });
  jobStatusMemory = nextStatusMemory;
}

function resultPill(result, state = '', tag = '', message = '') {
  const raw = `${tag || ''} ${result || ''} ${state || ''} ${message || ''}`.toLowerCase();
  let level = 'info';
  let label = prettyWord(result || state || level);
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) {
    level = 'warning';
    label = t('unavailableLabel');
  } else if (raw.includes('success') || raw.includes('ok') || raw.includes('done')) level = 'success';
  else if (raw.includes('fail') || raw.includes('error')) level = 'failed';
  else if (raw.includes('warn')) level = 'warning';
  else if (raw.includes('running') || raw.includes('process')) level = 'running';
  return `<span class="result-pill ${level}">${esc(label)}</span>`;
}

function extractLogBlockName(log) {
  const text = String(log?.message || '').trim();
  const match = text.match(/^([^:]{1,80}):/);
  return match ? match[1].trim() : '';
}

function getLogPostLabel(log) {
  return extractLogBlockName(log) || (currentRunMode === 'scan' ? 'Scan' : 'Post');
}

function compactIssuePostLabel(postName) {
  const raw = String(postName || '').trim();
  if (!raw) return '';
  let match = raw.match(/^post[ ]+([0-9]+)$/i);
  if (match) return `P${match[1]}`;
  match = raw.match(/^scan[ ]+([0-9]+)$/i);
  if (match) return `S${match[1]}`;
  match = raw.match(/^booking[ ]+([0-9]+)$/i);
  if (match) return `B${match[1]}`;
  return raw.length > 12 ? `${raw.slice(0, 12)}…` : raw;
}

function formatIssueCellChip(item) {
  const rowPart = `#${item?.row || '?'}`;
  const colPart = item?.column && item.column !== '-' ? `:${String(item.column).trim().toUpperCase()}` : '';
  return `${rowPart}${colPart}`.trim();
}

function getIssueColumnsForRequestPost(requestMeta, postLabel) {
  const mappings = Array.isArray(requestMeta?.mappings) ? requestMeta.mappings : [];
  const normalizedPost = String(postLabel || '').trim().toLowerCase();
  const normalizeCol = value => String(value || '').trim().toUpperCase();
  const addUnique = (bucket, value) => {
    const col = normalizeCol(value);
    if (col && !bucket.includes(col)) bucket.push(col);
  };
  let match = mappings.find(item => String(item?.name || '').trim().toLowerCase() === normalizedPost);
  if (!match && mappings.length === 1) match = mappings[0];
  if (!match) {
    return [];
  }
  const mode = String(match?.mode || requestMeta?.mode || currentRunMode || '').trim().toLowerCase();
  const columns = [];
  if (mode === 'scan') {
    addUnique(columns, match?.col_drive);
  } else {
    addUnique(columns, match?.col_profile);
    addUnique(columns, match?.col_content);
    addUnique(columns, match?.col_drive);
    addUnique(columns, match?.col_screenshot);
  }
  return columns;
}

function buildIssueCellEntries(errorRows, logs, issueCells = [], requestMeta = null) {
  const entries = new Map();
  const detailedRowPosts = new Set();
  const upsert = (rowValue, postLabel, columnValue, message, kind = '') => {
    const row = Number(rowValue || 0);
    if (!Number.isFinite(row) || row <= 0) return;
    const post = String(postLabel || '').trim();
    const column = String(columnValue || '').trim().toUpperCase();
    const rowPostKey = `${post}|${row}`;
    if (!column && detailedRowPosts.has(rowPostKey)) return;
    const key = `${post}|${row}|${column}`;
    if (!entries.has(key)) {
      entries.set(key, { key, row, post, column, message: String(message || '').trim(), kind: String(kind || '').trim() });
      if (column) {
        detailedRowPosts.add(rowPostKey);
      }
      return;
    }
    const existing = entries.get(key);
    if (!existing.message && message) existing.message = String(message || '').trim();
    if (!existing.kind && kind) existing.kind = String(kind || '').trim();
  };
  const upsertWithInferredColumns = (rowValue, postLabel, message, kind = '') => {
    const inferredColumns = getIssueColumnsForRequestPost(requestMeta, postLabel);
    if (inferredColumns.length) {
      inferredColumns.forEach(col => upsert(rowValue, postLabel, col, message, kind));
      return;
    }
    upsert(rowValue, postLabel, '', message, kind);
  };

  (Array.isArray(issueCells) ? issueCells : []).forEach(item => {
    upsert(
      item?.row,
      item?.post,
      item?.column,
      item?.message || '',
      item?.kind || ''
    );
  });

  if (detailedRowPosts.size) {
    for (const [key, value] of Array.from(entries.entries())) {
      if (!String(value?.column || '').trim()) {
        const rowPostKey = `${String(value?.post || '').trim()}|${Number(value?.row || 0)}`;
        if (detailedRowPosts.has(rowPostKey)) {
          entries.delete(key);
        }
      }
    }
  }

  Object.entries(errorRows || {}).forEach(([rowKey, rawMessage]) => {
    const message = String(rawMessage || '').trim();
    const post = extractLogBlockName({ message }) || '';
    upsertWithInferredColumns(rowKey, post, message, 'stored');
  });

  (Array.isArray(logs) ? logs : []).forEach(item => {
    if (!isUnavailableLog(item) && !isFailedLog(item)) return;
    upsertWithInferredColumns(
      item?.row,
      getLogPostLabel(item),
      item?.message || item?.result || item?.state || '',
      isUnavailableLog(item) ? 'unavailable' : 'failed'
    );
  });

  return Array.from(entries.values()).sort((a, b) => {
    if (a.row !== b.row) return a.row - b.row;
    if (String(a.column || '') !== String(b.column || '')) {
      return String(a.column || '').localeCompare(String(b.column || ''));
    }
    return String(a.post || '').localeCompare(String(b.post || ''));
  });
}

function isUnavailableLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  return raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung');
}

function isFailedLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return false;
  return raw.includes('fail') || raw.includes('error');
}

function isSuccessLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return false;
  if (raw.includes('fail') || raw.includes('error')) return false;
  return raw.includes('ok') || raw.includes('success') || raw.includes('done');
}

function normalizeIssueSummaryLabel(rawMessage) {
  let text = String(rawMessage || '').trim();
  if (!text) return '';
  text = text.replace(/^[^:]{1,80}: */, '').trim();
  text = text.replace(/^row *#?[0-9]+ *[-:] */i, '').trim();
  if (!text) return '';
  const lowered = text.toLowerCase();
  if (lowered.includes('không khả dụng') || lowered.includes('khong kha dung') || lowered.includes('unavailable')) {
    return t('unavailableLabel');
  }
  return text.length > 88 ? `${text.slice(0, 85).trim()}...` : text;
}

function buildIssueSummaryText(errorRows, logs, fallbackError) {
  const issueCounts = new Map();
  const addIssue = message => {
    const normalized = normalizeIssueSummaryLabel(message);
    if (!normalized) return;
    issueCounts.set(normalized, (issueCounts.get(normalized) || 0) + 1);
  };

  Object.values(errorRows || {}).forEach(addIssue);
  (Array.isArray(logs) ? logs : []).forEach(item => {
    if (!isUnavailableLog(item) && !isFailedLog(item)) return;
    addIssue(item?.message || item?.result || item?.state || '');
  });
  addIssue(fallbackError);

  if (!issueCounts.size) return t('monitorIssueSummaryNone');
  const ranked = Array.from(issueCounts.entries()).sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return a[0].localeCompare(b[0]);
  });
  const [topLabel, topCount] = ranked[0];
  const moreKinds = Math.max(ranked.length - 1, 0);
  return moreKinds > 0
    ? t('monitorIssueSummaryTopMoreFmt')(topLabel, topCount, moreKinds)
    : t('monitorIssueSummaryTopFmt')(topLabel, topCount);
}

function canReplayLog(log) {
  const row = Number(log?.row || 0);
  if (!Number.isFinite(row) || row < 1) return false;
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''}`.toLowerCase();
  return raw.includes('ok') || raw.includes('fail') || raw.includes('unavailable');
}

function statusBadge(status) {
  const key = String(status || '').toLowerCase();
  const normalized = key || 'queued';
  return `<span class="project-status-badge status-${esc(normalized)}">${esc(prettyWord(normalized))}</span>`;
}

function aggregateErrorCounts(jobs) {
  const map = new Map();
  (jobs || []).forEach(job => {
    const rows = job?.error_rows || {};
    Object.values(rows).forEach(msg => {
      const key = String(msg || '').trim() || 'Unknown error';
      map.set(key, (map.get(key) || 0) + 1);
    });
  });
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

function groupJobsBySheet(jobs) {
  const groups = new Map();
  (jobs || []).forEach(job => {
    const label = getJobSheetLabel(job);
    if (!groups.has(label)) groups.set(label, []);
    groups.get(label).push(job);
  });
  return [...groups.entries()].map(([label, items]) => {
    const completed = items.filter(x => x.status === 'completed').length;
    const failed = items.filter(x => x.status === 'failed').length;
    return { label, items, count: items.length, completed, failed };
  }).sort((a, b) => b.count - a.count);
}

function renderOverview() {
  const savedProjects = getSavedProjectJobs();
  const savedSheets = new Set(savedProjects.map(job => getJobSheetLabel(job))).size;
  let selectedProject = currentProjectJobId ? savedProjects.find(job => job.id === currentProjectJobId) : null;
  if (!selectedProject && savedProjects.length) selectedProject = savedProjects[0];
  const selectedProjectSummary = getJobSummary(selectedProject);
  const modeCounts = ['seeding', 'booking', 'scan'].map(mode => ({
    mode,
    count: jobsCache.filter(job => getJobMode(job) === mode).length,
  }));
  const modeTotal = modeCounts.reduce((sum, item) => sum + item.count, 0);
  document.getElementById('ovSavedProjects').textContent = savedProjects.length;
  document.getElementById('ovSavedSheets').textContent = savedSheets;
  document.getElementById('ovSelectedProject').textContent = selectedProject
    ? `${selectedProjectSummary.done || 0}/${selectedProjectSummary.total || 0}`
    : '-';
  const modeSplitHost = document.getElementById('ovModeSplit');
  if (modeSplitHost) {
    if (!modeTotal) {
      modeSplitHost.innerHTML = `<div class="overview-side-empty">${esc(t('overviewModeSplitEmpty'))}</div>`;
    } else {
      modeSplitHost.innerHTML = modeCounts.map(item => {
        const pct = modeTotal ? Math.round((item.count / modeTotal) * 100) : 0;
        const width = item.count > 0 ? Math.max(8, Math.round((item.count / modeTotal) * 100)) : 0;
        return `<div class="overview-mode-row">
          <div class="overview-mode-head">
            <span class="mode-pill mode-${item.mode}">${esc(getRunModeLabel(item.mode))}</span>
            <span class="overview-mode-value">${item.count}</span>
          </div>
          <div class="overview-mode-track"><span class="overview-mode-fill mode-${item.mode}" style="width:${width}%"></span></div>
          <div class="overview-mode-meta">${esc(t('overviewModeShareFmt')(item.count, pct))}</div>
        </div>`;
      }).join('');
    }
  }

  const historyBars = document.getElementById('ovHistoryBars');
  const historyBadges = document.getElementById('ovHistoryBadges');
  const buckets = buildOverviewDateBuckets(jobsCache, 7);
  if (historyBars) {
    if (!buckets.length) {
      historyBars.innerHTML = `<div class="overview-history-empty">${esc(t('overviewTimelineEmpty'))}</div>`;
    } else {
      const maxSeries = Math.max(1, ...buckets.flatMap(bucket => [bucket.success, bucket.failed, bucket.unavailable]));
      historyBars.innerHTML = buckets.map((bucket, idx, arr) => {
        const latestClass = idx === arr.length - 1 ? ' is-latest' : '';
        const successHeight = bucket.success > 0 ? Math.max(18, Math.round((bucket.success / maxSeries) * 150)) : 8;
        const failedHeight = bucket.failed > 0 ? Math.max(18, Math.round((bucket.failed / maxSeries) * 150)) : 8;
        const unavailableHeight = bucket.unavailable > 0 ? Math.max(18, Math.round((bucket.unavailable / maxSeries) * 150)) : 8;
        return `<div class="overview-history-group">
          <div class="overview-history-columns">
            <div class="overview-history-col-wrap">
              <div class="overview-history-col-value">${bucket.success}</div>
              <div class="overview-history-col success${latestClass}" style="height:${successHeight}px" title="${esc(t('overviewCompletedLegend'))}: ${bucket.success}"></div>
            </div>
            <div class="overview-history-col-wrap">
              <div class="overview-history-col-value">${bucket.failed}</div>
              <div class="overview-history-col failed${latestClass}" style="height:${failedHeight}px" title="${esc(t('overviewFailedLegend'))}: ${bucket.failed}"></div>
            </div>
            <div class="overview-history-col-wrap">
              <div class="overview-history-col-value">${bucket.unavailable}</div>
              <div class="overview-history-col unavailable${latestClass}" style="height:${unavailableHeight}px" title="${esc(t('overviewUnavailableLegend'))}: ${bucket.unavailable}"></div>
            </div>
          </div>
          <div class="overview-history-day">${esc(toLocalDayLabel(bucket.key))}</div>
        </div>`;
      }).join('');
    }
  }
  if (historyBadges) {
    if (!buckets.length) {
      historyBadges.innerHTML = '';
    } else {
      const latestBucket = buckets[buckets.length - 1];
      historyBadges.innerHTML = [
        `<div class="overview-history-badge">${esc(t('overviewDateFmt')(toLocalDayLabel(latestBucket.key)))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineJobsBadgeFmt')(latestBucket.jobs))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineSuccessBadgeFmt')(latestBucket.success))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineFailedBadgeFmt')(latestBucket.failed))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineUnavailableBadgeFmt')(latestBucket.unavailable))}</div>`,
      ].join('');
    }
  }
}

function switchView(name, tabEl = null) {
  if ((name === 'settings' || name === 'access') && !isAdminUser()) {
    setStatus(t('adminOnly'), 'failed');
    name = 'runs';
    tabEl = document.querySelector('.side-btn[data-view="runs"]');
  }
  document.querySelectorAll('.view').forEach(node => node.classList.remove('active'));
  const view = document.getElementById('view-' + name);
  if (view) view.classList.add('active');
  document.querySelectorAll('.side-btn[data-view]').forEach(node => node.classList.remove('active'));
  const activeTab = tabEl || document.querySelector(`.side-btn[data-view="${name}"]`);
  if (activeTab) activeTab.classList.add('active');
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', name === 'runs');
  if (name === 'access' && isAdminUser()) Promise.all([loadAccessPolicy(), loadMailConfig()]);
}

function setStatus(text, status) {
  const statusText = document.getElementById('statusText');
  if (statusText) statusText.textContent = text;
  const chip = document.getElementById('envChip');
  if (!chip) return;
  chip.style.background = '#eef2f6';
  chip.style.color = '#334155';
  if (status === 'running') { chip.style.background = '#dbeafe'; chip.style.color = '#1d4ed8'; }
  if (status === 'paused') { chip.style.background = '#fef3c7'; chip.style.color = '#b45309'; }
  if (status === 'completed') { chip.style.background = '#dcfce7'; chip.style.color = '#166534'; }
  if (status === 'failed') { chip.style.background = '#fee2e2'; chip.style.color = '#991b1b'; }
  if (status === 'stopped') { chip.style.background = '#ffedd5'; chip.style.color = '#9a3412'; }
  chip.textContent = `${t('state')}: ` + prettyWord(status || 'idle');
}

function setKPI(summary, jobId) {
  const s = summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
  const pct = s.total > 0 ? Math.min(100, Math.floor((s.done / s.total) * 100)) : 0;
  document.getElementById('kpiJob').textContent = jobId ? jobId.slice(0, 8) : '-';
  document.getElementById('kpiSF').textContent = s.success + ' / ' + s.failed;
  document.getElementById('pctText').textContent = pct + '%';
  document.getElementById('pfill').style.width = pct + '%';
  document.getElementById('overviewText').textContent = jobId
    ? t('overviewTextFmt')(jobId.slice(0, 8), s.done, s.total)
    : t('noRunSelected');
}

function renderProjects() {
  const allSaved = getSavedProjectJobs();
  const modeFiltered = getProjectJobsForModeFilter();
  const saved = getFilteredProjectJobs();
  const selected = getSelectedProjectJob();
  const uniqueSheets = new Set(saved.map(job => getJobSheetLabel(job))).size;
  const summary = getJobSummary(selected);
  const completionText = String(selected?.completion?.summary || '').trim();
  const request = selected?.request || {};
  const projectLogs = selected
    ? getLineageDisplayLogs(
        selected,
        Array.isArray(selected?.logs)
          ? selected.logs
          : (Array.isArray(selected?.recent_logs) ? selected.recent_logs : [])
      ).slice().reverse()
    : [];
  const projectLogsHtml = selected
    ? `
      <div class="project-log-panel">
        <div class="project-log-head">
          <div class="project-log-title">${esc(t('projectLogs'))}</div>
          <div class="project-log-sub">${esc(t('projectLogsSub'))}</div>
        </div>
        <div class="project-log-list">
          ${projectLogs.length
            ? projectLogs.slice(0, 120).map(item => {
                const postName = getLogPostLabel(item);
                const lineageMeta = item.__job_id ? String(item.__job_id).slice(0, 8) : String(selected?.id || '').slice(0, 8);
                return `
                  <div class="project-log-item">
                    <div class="project-log-top">
                      <div class="project-log-meta">
                        <span>${esc(toLocalStamp(item.ts))}</span>
                        <span>${esc(postName)}</span>
                        <span>#${esc(item.row)}</span>
                        <span>${esc(lineageMeta)}</span>
                      </div>
                      ${resultPill(item.result, item.state, item.tag, item.message)}
                    </div>
                    <div class="project-log-message">${esc(item.message || `${item.state}/${item.result}`)}</div>
                  </div>`;
              }).join('')
            : `<div class="project-log-empty">${esc(t('projectNoLogs'))}</div>`}
        </div>
      </div>`
    : '';
  const filterOptions = [
    { key: 'all', label: t('allProjects'), count: allSaved.length },
    { key: 'seeding', label: getRunModeLabel('seeding'), count: allSaved.filter(job => getJobMode(job) === 'seeding').length },
    { key: 'booking', label: getRunModeLabel('booking'), count: allSaved.filter(job => getJobMode(job) === 'booking').length },
    { key: 'scan', label: getRunModeLabel('scan'), count: allSaved.filter(job => getJobMode(job) === 'scan').length },
  ];
  const statusFilterOptions = [
    { key: 'all', label: t('projectStatusAll'), count: modeFiltered.length },
    { key: 'running', label: t('projectStatusRunning'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'running')).length },
    { key: 'completed', label: t('projectStatusCompleted'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'completed')).length },
    { key: 'stopped', label: t('projectStatusStopped'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'stopped')).length },
    { key: 'failed', label: t('projectStatusFailed'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'failed')).length },
  ];
  const totalNode = document.getElementById('projectsTotalJobs');
  const sheetsNode = document.getElementById('projectsCompletedJobs');
  const selectedNode = document.getElementById('projectsSelectedJob');
  if (totalNode) totalNode.textContent = saved.length;
  if (sheetsNode) sheetsNode.textContent = uniqueSheets;
  if (selectedNode) selectedNode.textContent = selected ? `${summary.done || 0}/${summary.total || 0}` : '-';
  const focusedFilterId = document.activeElement?.id || '';
  if (focusedFilterId !== 'projectsModeFilterInput') {
    document.getElementById('projectsModeFilters').innerHTML = `
      <label class="project-filter-select">
        <span>${esc(t('projectModeLabel'))}</span>
        <select id="projectsModeFilterInput" class="project-filter-input" aria-label="${esc(t('projectModeLabel'))}" size="1" onclick="expandProjectFilter(this)" onfocus="expandProjectFilter(this)" onblur="collapseProjectFilter(this)" onkeydown="handleProjectFilterKeydown(event)" onchange="setProjectModeFilter(this.value); collapseProjectFilter(this, 0)">
          ${filterOptions.map(opt => `<option value="${esc(opt.key)}"${currentProjectModeFilter === opt.key ? ' selected' : ''}>${esc(opt.label)} (${opt.count})</option>`).join('')}
        </select>
      </label>`;
  }
  if (focusedFilterId !== 'projectsStatusFilterInput') {
    document.getElementById('projectsStatusFilters').innerHTML = `
      <label class="project-filter-select">
        <span>${esc(t('projectStatusLabel'))}</span>
        <select id="projectsStatusFilterInput" class="project-filter-input" aria-label="${esc(t('projectStatusLabel'))}" size="1" onclick="expandProjectFilter(this)" onfocus="expandProjectFilter(this)" onblur="collapseProjectFilter(this)" onkeydown="handleProjectFilterKeydown(event)" onchange="setProjectStatusFilter(this.value); collapseProjectFilter(this, 0)">
          ${statusFilterOptions.map(opt => `<option value="${esc(opt.key)}"${currentProjectStatusFilter === opt.key ? ' selected' : ''}>${esc(opt.label)} (${opt.count})</option>`).join('')}
        </select>
      </label>`;
  }
  document.getElementById('projectsSnapshotAction').innerHTML = selected
    ? `<div class="project-detail-actions"><button type="button" class="project-nav-btn" title="${esc(t('openProjectRun'))}" aria-label="${esc(t('openProjectRun'))}" onclick="openProjectInRuns('${selected.id}')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h12"></path><path d="m13 6 6 6-6 6"></path></svg></button></div>`
    : '';
  document.getElementById('projectsList').innerHTML = saved.length
    ? saved.map(job => {
        const jobSummary = getJobSummary(job);
        const active = currentProjectJobId === job.id ? ' active' : '';
        const mode = getJobMode(job);
        const ownerLabel = getJobOwnerBadge(job);
        return `<div class="list-row project-item${active}" onclick="selectProject('${job.id}')">
          <div class="project-item-main">
            <div class="project-item-title">${esc(getJobSheetLabel(job))}</div>
            <div class="project-item-meta"><span class="mode-pill mode-${mode}">${esc(prettyWord(mode))}</span><span>${esc(toLocalStamp(job.finished_at || job.created_at))}</span><span>${esc(job.id.slice(0, 8))}</span>${ownerLabel ? `<span>${esc(ownerLabel)}</span>` : ''}</div>
          </div>
          <div class="project-item-side">
            ${statusBadge(job.status)}
            <span class="project-item-progress">${jobSummary.done || 0}/${jobSummary.total || 0}</span>
            ${isAdminUser() && isJobOwnedByCurrentUser(job) ? `<button type="button" class="project-delete-btn" title="${esc(t('deleteLabel'))}" onclick="deleteProject('${job.id}', event)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M6 7l1 12h10l1-12"></path><path d="M9 7V4h6v3"></path></svg>
            </button>` : ''}
          </div>
        </div>`;
      }).join('')
    : `<div class="list-row"><span>${allSaved.length ? t('noProjectsInFilter') : t('noGroupsYet')}</span><span>-</span></div>`;
  document.getElementById('projectsSnapshot').innerHTML = selected
    ? [
        `<div class="timeline-item"><strong>${t('group')}</strong><div>${esc(getJobSheetLabel(selected))}</div></div>`,
        `<div class="timeline-item"><strong>${t('state')}</strong><div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">${statusBadge(selected.status)}<span class="mode-pill mode-${getJobMode(selected)}">${esc(prettyWord(getJobMode(selected)))}</span></div></div>`,
        ...(getJobOwnerBadge(selected) ? [`<div class="timeline-item"><strong>${t('projectOwner')}</strong><div>${esc(getJobOwnerBadge(selected))}</div></div>`] : []),
        `<div class="timeline-item"><strong>${t('latestUpdate')}</strong><div>${esc(toLocalStamp(selected.finished_at || selected.created_at))}</div></div>`,
        `<div class="timeline-item"><strong>${t('jobs')}</strong><div>${summary.done || 0}/${summary.total || 0} · ${summary.success || 0} ${t('success').toLowerCase()} · ${summary.failed || 0} ${t('failedLabel').toLowerCase()}</div></div>`,
        `<div class="timeline-item"><strong>${t('driveFolder')}</strong><div>${esc(request.drive_id || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('detailLabel')}</strong><div>${esc(selected.detail || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('summaryLabel')}</strong><div style="white-space:pre-line">${esc(completionText || '-')}</div></div>`,
        projectLogsHtml,
      ].join('')
    : `<div class="timeline-item"><strong>${t('noProjectGroup')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function renderActivities(logs) {
  const items = (logs || []).slice();
  document.getElementById('activitiesTimeline').innerHTML = items.length
    ? items.map(x => {
        const level = classifyLog(x);
        const levelLabel = level === 'info' ? t('activityLevel') : prettyWord(level);
        const jobMeta = x.__job_id ? `${String(x.__job_mode || '').trim() ? `${prettyWord(x.__job_mode)} · ` : ''}${String(x.__job_id || '').slice(0, 8)}` : '';
        const ownerMeta = isAdminUser() && x?.owner_email ? String(x.owner_email).trim().toLowerCase() : '';
        const rowLabel = x.__source === 'activity' ? esc(String(x.state || 'ACTION')) : `#${esc(x.row)} ${esc(x.state)}/${esc(x.result)}`;
        return `<div class="timeline-item"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>${rowLabel}</strong><span class="badge ${level}">${esc(levelLabel)}</span></div><div>${esc(x.message)}</div><div class="s">${ownerMeta ? `${esc(ownerMeta)} · ` : ''}${jobMeta ? `${esc(jobMeta)} · ` : ''}${toLocalStamp(x.ts)}</div></div>`;
      }).join('')
    : `<div class="timeline-item"><strong>${t('noActivity')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function toggleIssueStrip(kind) {
  const key = String(kind || '').toLowerCase();
  if (!['failed', 'unavailable'].includes(key)) return;
  monitorIssueExpandState[key] = !monitorIssueExpandState[key];
  renderRunMonitor(currentJobSnapshot, currentLogsCache);
}

function renderRunMonitor(snapshot, logs) {
  const st = snapshot || {};
  const renderJobId = String(st.id || currentJobId || '').trim();
  if (renderJobId !== monitorIssueExpandJobId) {
    monitorIssueExpandJobId = renderJobId;
    monitorIssueExpandState = { failed: false, unavailable: false };
  }
  const s = getJobSummary(st);
  let displayStatus = String(st.status || 'idle').toLowerCase();
  if (displayStatus === 'completed' && Number(s.total || 0) <= 0 && !(Array.isArray(logs) && logs.length)) {
    displayStatus = 'stopped';
  }
  const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
  const errorRows = st.error_rows || {};
  const issueCells = Array.isArray(st.issue_cells) ? st.issue_cells : [];
  const logItems = Array.isArray(logs) ? logs : [];
  const displayLogs = getLineageDisplayLogs(st, logItems);
  const successCount = Number(s.success || 0);
  const failedCount = Number(s.failed || 0);
  const unavailableCount = Number(s.unavailable || 0);
  const issueEntries = buildIssueCellEntries(errorRows, logItems, issueCells, st.request || null);
  const hasIssueState = (failedCount + unavailableCount) > 0 || issueEntries.length > 0 || String(st.status || '').toLowerCase() === 'failed' || !!String(st.error || '').trim();
  const statusLabel = prettyWord(displayStatus || 'idle');
  const latestLog = logItems.length ? logItems[logItems.length - 1] : null;
  const detailText = String(st.detail || latestLog?.message || '').trim();
  const etaText = s.eta && s.eta !== '---' ? `${t('eta')}: ${s.eta}` : '';
  const title = st.request ? getJobSheetLabel(st) : t('monitorNoJob');
  const metaParts = [];
  const ownerLabel = getJobOwnerBadge(st);
  const ownJob = isJobOwnedByCurrentUser(st);
  if (st.mode || st.request?.mode) metaParts.push(prettyWord(getJobMode(st)));
  if (ownerLabel) metaParts.push(ownerLabel);
  if (currentJobId) metaParts.push(currentJobId.slice(0, 8));
  if (st.created_at) metaParts.push(toLocalStamp(st.created_at));
  const statusNode = document.getElementById('runMonitorStatus');
  statusNode.textContent = statusLabel;
  statusNode.style.background = 'var(--blue-soft)';
  statusNode.style.color = 'var(--blue)';
  statusNode.style.borderColor = 'rgba(91,147,211,.25)';
  if (displayStatus === 'completed') {
    statusNode.style.background = 'rgba(52,195,143,.16)';
    statusNode.style.color = 'var(--green)';
    statusNode.style.borderColor = 'rgba(52,195,143,.35)';
  } else if (displayStatus === 'paused') {
    statusNode.style.background = 'rgba(245,158,11,.16)';
    statusNode.style.color = '#b45309';
    statusNode.style.borderColor = 'rgba(245,158,11,.35)';
  } else if (displayStatus === 'failed') {
    statusNode.style.background = 'rgba(240,138,160,.16)';
    statusNode.style.color = 'var(--red)';
    statusNode.style.borderColor = 'rgba(240,138,160,.35)';
  } else if (displayStatus === 'stopped') {
    statusNode.style.background = 'rgba(243,197,142,.16)';
    statusNode.style.color = '#b45309';
    statusNode.style.borderColor = 'rgba(243,197,142,.35)';
  }
  document.getElementById('runMonitorJobTitle').textContent = title;
  document.getElementById('runMonitorJobMeta').textContent = metaParts.join(' · ') || '-';
  document.getElementById('runMonitorProgressMain').textContent = `${s.done || 0} / ${s.total || 0}`;
  document.getElementById('runMonitorPercent').textContent = `${pct}%`;
  document.getElementById('runMonitorBar').style.width = `${pct}%`;
  document.getElementById('runMonitorProgressMeta').textContent = detailText
    ? `${detailText}${etaText ? ' · ' + etaText : ''}`
    : (etaText || '-');
  document.getElementById('runMonitorErrorMain').innerHTML = `
    <div class="monitor-error-stats">
      <span class="monitor-error-stat success">${esc(t('success'))} <strong>${esc(successCount)}</strong></span>
      <span class="monitor-error-stat failed">${esc(t('errorRows'))} <strong>${esc(failedCount)}</strong></span>
      <span class="monitor-error-stat unavailable">${esc(t('unavailableLabel'))} <strong>${esc(unavailableCount)}</strong></span>
    </div>
  `;
  const issueRowsStrip = document.getElementById('runMonitorIssueRowsStrip');
  const unavailableRowsStrip = document.getElementById('runMonitorUnavailableRowsStrip');
  const issueRowsNode = document.getElementById('runMonitorErrorRows');
  const unavailableRowsNode = document.getElementById('runMonitorUnavailableRows');
  const failedIssueItems = issueEntries.filter(item =>
    !isUnavailableLog({ message: item.message, result: item.kind, state: item.kind, tag: item.kind })
  );
  const unavailableIssueItems = issueEntries.filter(item =>
    isUnavailableLog({ message: item.message, result: item.kind, state: item.kind, tag: item.kind })
  );
  const failedExpanded = !!monitorIssueExpandState.failed;
  const unavailableExpanded = !!monitorIssueExpandState.unavailable;
  if (issueRowsNode) {
    if (failedIssueItems.length) {
      const visibleRows = failedExpanded ? failedIssueItems : failedIssueItems.slice(0, 8);
      const chips = visibleRows.map(item => {
        return `<span class="monitor-issue-chip">${esc(formatIssueCellChip(item))}</span>`;
      });
      if (failedIssueItems.length > 8) {
        chips.push(
          failedExpanded
            ? `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('failed')">${esc(t('monitorIssueCollapse'))}</button>`
            : `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('failed')">${esc(t('monitorIssueExpandFmt')(failedIssueItems.length - 8))}</button>`
        );
      }
      issueRowsNode.innerHTML = chips.join('');
    } else {
      issueRowsNode.innerHTML = '';
    }
  }
  if (unavailableRowsNode) {
    if (unavailableIssueItems.length) {
      const visibleRows = unavailableExpanded ? unavailableIssueItems : unavailableIssueItems.slice(0, 8);
      const chips = visibleRows.map(item => {
        return `<span class="monitor-issue-chip unavailable">${esc(formatIssueCellChip(item))}</span>`;
      });
      if (unavailableIssueItems.length > 8) {
        chips.push(
          unavailableExpanded
            ? `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('unavailable')">${esc(t('monitorIssueCollapse'))}</button>`
            : `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('unavailable')">${esc(t('monitorIssueExpandFmt')(unavailableIssueItems.length - 8))}</button>`
        );
      }
      unavailableRowsNode.innerHTML = chips.join('');
    } else {
      unavailableRowsNode.innerHTML = '';
    }
  }
  document.getElementById('runMonitorErrorMeta').textContent = '';
  if (issueRowsStrip) issueRowsStrip.hidden = failedIssueItems.length <= 0;
  if (unavailableRowsStrip) unavailableRowsStrip.hidden = unavailableIssueItems.length <= 0;

  const rows = displayLogs.slice().reverse();
  const replayLocked = ['running', 'paused'].includes(displayStatus) || !ownJob;
  document.getElementById('runMonitorRows').innerHTML = rows.length
    ? rows.map(x => {
        const postName = getLogPostLabel(x);
        const message = x.message || `${x.state}/${x.result}`;
        const replayBlockName = extractLogBlockName(x);
        const replayButton = canReplayLog(x)
          ? `<button class="monitor-replay-btn" type="button" ${replayLocked ? `disabled title="${!ownJob ? 'Chỉ replay được job của chính bạn' : 'Job đang chạy, chưa thể replay'}"` : `onclick="replayLogRow('${esc(st.id || currentJobId || '')}', ${Number(x.row || 0)}, '${esc(replayBlockName)}')"`}>
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5V1L7 6l5 5V7c3.309 0 6 2.691 6 6a6 6 0 0 1-6 6 6 6 0 0 1-5.657-4H4.263A8.001 8.001 0 0 0 12 21c4.411 0 8-3.589 8-8s-3.589-8-8-8Z"></path></svg>
              <span>${esc(t('replay'))}</span>
            </button>`
          : `<span class="muted">-</span>`;
        return `<tr>
          <td>${esc(toLocalStamp(x.ts))}</td>
          <td>${esc(postName)}</td>
          <td>${esc(x.row)}</td>
          <td>${resultPill(x.result, x.state, x.tag, message)}</td>
          <td>${esc(message)}</td>
          <td class="monitor-replay-cell">${replayButton}</td>
        </tr>`;
      }).join('')
    : `<tr><td colspan="6">${t('noData')}</td></tr>`;
}

function updateRunActionButtons(snapshot = currentJobSnapshot) {
  const pauseLabel = document.getElementById('pauseJobLabel');
  const pauseIcon = document.getElementById('pauseJobIcon');
  const pauseButton = pauseLabel ? pauseLabel.closest('button') : null;
  const continueLabel = document.getElementById('continueJobLabel');
  const continueIcon = document.getElementById('continueJobIcon');
  const continueButton = continueLabel ? continueLabel.closest('button') : null;
  const errorOnlyLabel = document.getElementById('errorOnlyJobLabel');
  const errorOnlyIcon = document.getElementById('errorOnlyJobIcon');
  const errorOnlyButton = errorOnlyLabel ? errorOnlyLabel.closest('button') : null;
  if (!pauseLabel || !pauseIcon || !pauseButton || !continueLabel || !continueIcon || !continueButton || !errorOnlyLabel || !errorOnlyIcon || !errorOnlyButton) return;
  const status = String(snapshot?.status || '').toLowerCase();
  const ownJob = isJobOwnedByCurrentUser(snapshot);
  const canStop = ownJob && ['running', 'paused'].includes(status);
  const canContinue = ownJob && ['stopped', 'failed', 'completed'].includes(status) && !!String(snapshot?.id || currentJobId || '').trim();
  const canRetryErrors = ownJob && ['stopped', 'failed', 'completed'].includes(status) && Object.keys(snapshot?.error_rows || {}).length > 0;
  pauseButton.classList.remove('resume', 'pause', 'red', 'soft', 'stop');
  if (canContinue) {
    pauseLabel.textContent = t('continueJob');
    pauseIcon.innerHTML = '<path d="M8 6.5v11l9-5.5-9-5.5Z"></path>';
    pauseButton.classList.add('resume');
    pauseButton.disabled = false;
    pauseButton.title = ownJob ? '' : 'Chỉ chạy tiếp được job của chính bạn';
    pauseButton.onclick = continueJob;
  } else {
    pauseLabel.textContent = t('stopJob');
    pauseIcon.innerHTML = '<rect x="7" y="7" width="10" height="10" rx="1.5"></rect>';
    pauseButton.classList.add('pause');
    pauseButton.disabled = !canStop;
    pauseButton.title = ownJob ? '' : 'Chỉ dừng được job của chính bạn';
    pauseButton.onclick = stopJob;
  }

  continueLabel.textContent = t('continueJob');
  continueIcon.innerHTML = '<path d="M8 6.5v11l9-5.5-9-5.5Z"></path>';
  continueButton.classList.remove('pause', 'resume', 'red', 'stop');
  continueButton.classList.add('soft');
  continueButton.disabled = !canContinue;
  continueButton.title = ownJob ? '' : 'Chỉ chạy tiếp được job của chính bạn';

  errorOnlyLabel.textContent = t('errorOnlyJob');
  errorOnlyIcon.innerHTML = '<path d="M12 8v5"></path><circle cx="12" cy="16.5" r=".9" fill="currentColor" stroke="none"></circle><path d="M10.2 4.8 3.9 16a1.4 1.4 0 0 0 1.22 2.1h13.76A1.4 1.4 0 0 0 20.1 16L13.8 4.8a1.4 1.4 0 0 0-2.6 0Z"></path>';
  errorOnlyButton.classList.remove('pause', 'resume', 'red', 'stop');
  errorOnlyButton.classList.add('soft');
  errorOnlyButton.disabled = !canRetryErrors;
  errorOnlyButton.title = !ownJob ? 'Chỉ chạy lại lỗi được với job của chính bạn' : (Object.keys(snapshot?.error_rows || {}).length ? '' : 'Job này chưa có dòng lỗi');
}

async function replayLogRow(jobId, row, blockName = '') {
  try {
    primeCompletionNotifications();
    if (!jobId) throw new Error('No job selected');
    const payload = {
      row: Number(row || 0),
      block_name: String(blockName || ''),
    };
    const out = await req(`/api/jobs/${jobId}/replay-row`, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    await refreshJobs();
    currentJobId = out.job_id;
    setSelectedJobIdForMode(currentRunMode, out.job_id);
    await pollCurrent();
    setStatus(`${t('replayStartedFmt')(payload.row)} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
    const sourceJob = (jobsCache || []).find(job => job.id === jobId);
    if (await focusBlockingModeJob(e, getJobMode(sourceJob || {}))) return;
    alert(e.message);
  }
}

function exportCurrentLog() {
  const jobId = String(currentJobSnapshot?.id || currentJobId || '').trim();
  if (!jobId) {
    alert(t('monitorNoJob'));
    return;
  }
  if (!Array.isArray(currentLogsCache) || !currentLogsCache.length) {
    alert(t('noLogsToExport'));
    return;
  }
  const link = document.createElement('a');
  const exportQuery = new URLSearchParams({ ts: String(Date.now()) });
  if (shouldUseLocalAgent(`/api/jobs/${encodeURIComponent(jobId)}/export-log`) && authState.email) {
    exportQuery.set('user_email', authState.email);
  }
  link.href = runtimeHref(`/api/jobs/${encodeURIComponent(jobId)}/export-log?${exportQuery.toString()}`);
  link.target = '_blank';
  link.rel = 'noopener';
  document.body.appendChild(link);
  link.click();
  link.remove();
}

function setSettingsNote(text, isError = false) {
  const node = document.getElementById('settings_note');
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function setMailConfigNote(text, isError = false) {
  const node = document.getElementById('access_mail_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function setAccessEntryNote(text, isError = false) {
  const node = document.getElementById('access_entry_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function normalizeAccessType(value, email = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'internal' || raw === 'external') return raw;
  const domain = String(email || '').trim().toLowerCase().split('@')[1] || '';
  return domain === 'fanscom.vn' ? 'internal' : 'external';
}

function getAccessEmailTypes(policy = currentAccessPolicy) {
  const data = policy || {};
  const raw = data.email_types && typeof data.email_types === 'object' ? data.email_types : {};
  const lists = getAccessPolicyLists(data);
  const union = Array.from(new Set([...(lists.managed || []), ...(lists.admins || []), ...(lists.allowed || [])]));
  const out = {};
  union.forEach(email => {
    out[email] = normalizeAccessType(raw[email], email);
  });
  return out;
}

function setAccessMailEditorOpen(open, shouldScroll = false) {
  accessMailEditorOpen = !!open;
  if (accessMailEditorOpen) {
    accessEntryEditorState.open = false;
  }
  const card = document.querySelector('.access-mail-card');
  if (card) {
    card.classList.toggle('open', accessMailEditorOpen);
    if (accessMailEditorOpen && shouldScroll) {
      requestAnimationFrame(() => {
        card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      });
    }
  }
}

function setAccessEntryEditorOpen(open, shouldScroll = false) {
  accessEntryEditorState.open = !!open;
  const card = document.querySelector('.access-entry-editor');
  if (card) {
    card.classList.toggle('open', accessEntryEditorState.open);
    if (accessEntryEditorState.open && shouldScroll) {
      requestAnimationFrame(() => {
        card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      });
    }
  }
}

function renderMailConfig(config = currentMailConfig) {
  const data = config || { sender_email: '', from_email: '', has_password: false, source: 'env' };
  const senderNode = document.getElementById('access_mail_sender_email');
  const fromNode = document.getElementById('access_mail_from_email');
  const passwordNode = document.getElementById('access_mail_app_password');
  if (senderNode) senderNode.value = data.sender_email || '';
  if (fromNode) fromNode.value = data.from_email || data.sender_email || '';
  if (passwordNode) passwordNode.value = '';
  const currentPill = document.getElementById('accessMailCurrentPill');
  if (currentPill) currentPill.textContent = t('accessMailCurrentFmt')(data.sender_email || '');
  const passwordPill = document.getElementById('accessMailPasswordPill');
  if (passwordPill) {
    passwordPill.textContent = data.has_password ? t('accessMailPasswordSaved') : t('accessMailPasswordMissing');
    passwordPill.className = `access-mail-pill ${data.has_password ? 'ok' : 'warn'}`;
  }
  setAccessMailEditorOpen(accessMailEditorOpen, false);
  renderAccessDirectory(currentAccessPolicy);
}

function renderAccessEntryEditor() {
  const emailNode = document.getElementById('access_entry_email');
  const roleNode = document.getElementById('access_entry_role');
  const typeNode = document.getElementById('access_entry_type');
  if (emailNode) emailNode.value = accessEntryEditorState.email || '';
  if (roleNode) roleNode.value = accessEntryEditorState.role || 'user';
  if (typeNode) typeNode.value = accessEntryEditorState.type || 'internal';
  const pill = document.getElementById('accessEntryCurrentPill');
  if (pill) pill.textContent = t('accessEntryCurrentFmt')(accessEntryEditorState.originalEmail || accessEntryEditorState.email || '');
  setAccessEntryEditorOpen(accessEntryEditorState.open, false);
}

function setAccessPolicyNote(text, isError = false) {
  const node = document.getElementById('access_policy_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function parseAccessEmailLines(text) {
  return Array.from(new Set(String(text || '')
    .split(/[\\n,;]+/)
    .map(item => String(item || '').trim().toLowerCase())
    .filter(Boolean)));
}

function getAccessPolicyLists(policy = currentAccessPolicy) {
  const data = policy || { allowed_emails: [], admin_emails: [] };
  return {
    allowed: Array.isArray(data.allowed_emails) ? data.allowed_emails.map(item => String(item || '').trim().toLowerCase()).filter(Boolean) : [],
    admins: Array.isArray(data.admin_emails) ? data.admin_emails.map(item => String(item || '').trim().toLowerCase()).filter(Boolean) : [],
    managed: Array.isArray(data.managed_emails) ? data.managed_emails.map(item => String(item || '').trim().toLowerCase()).filter(Boolean) : [],
  };
}

function syncAccessPolicyEditors(policy = currentAccessPolicy) {
  const { allowed, admins } = getAccessPolicyLists(policy);
  const allowedNode = document.getElementById('access_allowed_emails');
  const adminNode = document.getElementById('access_admin_emails');
  if (allowedNode) allowedNode.value = allowed.join('\\n');
  if (adminNode) adminNode.value = admins.join('\\n');
}

function isValidAccessEmail(email) {
  return /^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$/.test(String(email || '').trim());
}

function buildAccessDirectoryRows(policy = currentAccessPolicy) {
  const data = policy || { allowed_emails: [], admin_emails: [], updated_at: null };
  const { allowed, admins, managed } = getAccessPolicyLists(data);
  const emailTypes = getAccessEmailTypes(data);
  const currentEmail = String(authState.email || '').trim().toLowerCase();
  const union = Array.from(new Set([...managed, ...admins, ...allowed])).sort((a, b) => {
    const aSelf = !!currentEmail && String(a || '').trim().toLowerCase() === currentEmail;
    const bSelf = !!currentEmail && String(b || '').trim().toLowerCase() === currentEmail;
    if (aSelf && !bSelf) return -1;
    if (!aSelf && bSelf) return 1;
    return a.localeCompare(b);
  });
  const updated = data.updated_at ? toLocalStamp(data.updated_at) : '-';
  const rows = union.map(email => {
    const isAdmin = admins.includes(email);
    const canLogin = isAdmin || allowed.includes(email) || managed.includes(email);
    const type = normalizeAccessType(emailTypes[email], email);
    const isCurrentUser = !!currentEmail && String(email || '').trim().toLowerCase() === currentEmail;
    return {
      key: email,
      email,
      title: email,
      subtitle: isAdmin ? t('accessAdminEntrySub') : t('accessAllowedEntrySub'),
      access: isAdmin ? 'admin' : 'allowed',
      role: isAdmin ? 'admin' : 'user',
      type,
      status: isAdmin ? 'admin' : (canLogin ? 'active' : 'open'),
      updated,
      initial: email.charAt(0).toUpperCase() || 'G',
      isSystem: false,
      isCurrentUser,
    };
  });
  rows.unshift({
    key: '__open__',
    email: '',
    title: t('accessOpenEntryTitle'),
    subtitle: `${t('accessOpenEntrySub')} · ${t('accessOpenEntryMailFmt')(currentMailConfig.sender_email || '')}`,
    access: 'open',
    role: 'otp',
    type: 'internal',
    status: 'open',
    updated,
    initial: 'OTP',
    isSystem: true,
  });
  return rows.filter(row => {
    const query = String(accessDirectoryQuery || '').trim().toLowerCase();
    const roleOk = accessDirectoryRole === 'all' || row.role === accessDirectoryRole;
    const scopeOk = accessDirectoryScope === 'all' || row.access === accessDirectoryScope;
    const typeOk = accessDirectoryType === 'all' || row.type === accessDirectoryType;
    const queryOk = !query || [row.title, row.subtitle, row.access, row.role, row.type, row.status]
      .join(' ')
      .toLowerCase()
      .includes(query);
    return roleOk && scopeOk && typeOk && queryOk;
  });
}

function updateAccessDirectoryFilters() {
  const roleSelect = document.getElementById('accessRoleFilterSelect');
  const scopeSelect = document.getElementById('accessScopeFilterSelect');
  const typeSelect = document.getElementById('accessTypeFilterSelect');
  if (roleSelect) roleSelect.value = accessDirectoryRole;
  if (scopeSelect) scopeSelect.value = accessDirectoryScope;
  if (typeSelect) typeSelect.value = accessDirectoryType;
}

function renderAccessDirectory(policy = currentAccessPolicy) {
  updateAccessDirectoryFilters();
  const rows = buildAccessDirectoryRows(policy);
  const countNode = document.getElementById('accessDirectoryCount');
  if (countNode) countNode.textContent = String(rows.length);
  const body = document.getElementById('accessDirectoryBody');
  if (!body) return;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="6"><div class="access-empty">${esc(t('accessDirectoryNoMatch'))}</div></td></tr>`;
    return;
  }
  const typeLabel = type => type === 'internal' ? t('accessTypeInternal') : t('accessTypeExternal');
  const roleLabel = role => role === 'admin' ? t('roleAdmin') : (role === 'otp' ? t('accessScopeOpen') : t('roleUser'));
  const statusLabel = status => status === 'admin' ? t('accessStatusAdmin') : (status === 'open' ? t('accessStatusOpen') : t('accessStatusActive'));
  const rowActions = row => {
    if (row.isSystem) {
      return `<div class="access-row-actions"><button class="access-row-btn edit" type="button" onclick="setAccessMailEditorOpen(true, true)">${esc(t('accessMailEdit'))}</button></div>`;
    }
    const token = encodeURIComponent(row.email);
    const edit = `<button class="access-row-btn edit" type="button" onclick="openAccessEntryEditor('${token}')">${esc(t('accessMailEdit'))}</button>`;
    const remove = `<button class="access-row-btn remove" type="button" onclick="removeAccessEmail('${token}')">${esc(t('accessRemove'))}</button>`;
    return `<div class="access-row-actions">${edit}${remove}</div>`;
  };
  body.innerHTML = rows.map(row => `
    <tr>
      <td>
        <div class="access-person">
          <div class="access-avatar ${esc(row.access)}">${esc(row.initial)}</div>
          <div class="access-person-meta">
            <div class="access-person-name">${esc(row.title)}${row.isCurrentUser ? ` <span class="access-you-tag">(${esc(t('accessYouTag'))})</span>` : ''}</div>
            <div class="access-person-sub">${esc(row.subtitle)}</div>
          </div>
        </div>
      </td>
      <td><span class="access-role-pill ${esc(row.role)}">${esc(roleLabel(row.role))}</span></td>
      <td><span class="access-type-pill ${esc(row.type)}">${esc(typeLabel(row.type))}</span></td>
      <td><span class="access-status ${esc(row.status)}">${esc(statusLabel(row.status))}</span></td>
      <td>${esc(row.updated)}</td>
      <td>${rowActions(row)}</td>
    </tr>`).join('');
}

function setAccessDirectoryQuery(value) {
  accessDirectoryQuery = String(value || '').trim();
  renderAccessDirectory(currentAccessPolicy);
}

function setAccessDirectoryRole(role) {
  accessDirectoryRole = ['all', 'admin', 'user'].includes(String(role || '').toLowerCase()) ? String(role).toLowerCase() : 'all';
  renderAccessDirectory(currentAccessPolicy);
}

function setAccessDirectoryScope(scope) {
  accessDirectoryScope = ['all', 'allowed', 'admin', 'open'].includes(String(scope || '').toLowerCase()) ? String(scope).toLowerCase() : 'all';
  renderAccessDirectory(currentAccessPolicy);
}

function setAccessDirectoryType(type) {
  accessDirectoryType = ['all', 'internal', 'external'].includes(String(type || '').toLowerCase()) ? String(type).toLowerCase() : 'all';
  renderAccessDirectory(currentAccessPolicy);
}

function openAccessEntryEditor(email) {
  const target = decodeURIComponent(String(email || '')).trim().toLowerCase();
  if (!target) return;
  setAccessMailEditorOpen(false, false);
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const emailTypes = getAccessEmailTypes(currentAccessPolicy);
  accessEntryEditorState = {
    open: true,
    originalEmail: target,
    email: target,
    role: lists.admins.includes(target) ? 'admin' : 'user',
    type: normalizeAccessType(emailTypes[target], target),
  };
  renderAccessEntryEditor();
  setAccessEntryNote('');
}

async function loadMailConfig() {
  if (!isAdminUser()) return;
  try {
    const out = await req('/api/admin/mail-config');
    currentMailConfig = out.config || { sender_email: '', from_email: '', has_password: false, updated_at: null, source: 'env' };
    renderMailConfig(currentMailConfig);
    setMailConfigNote('');
  } catch (e) {
    setMailConfigNote(e.message, true);
  }
}

async function reloadAccessAdminPanel() {
  await Promise.all([loadAccessPolicy(), loadMailConfig()]);
  setMailConfigNote(t('accessMailReloaded'));
}

async function saveMailConfig() {
  if (!isAdminUser()) {
    setMailConfigNote(t('adminOnly'), true);
    return;
  }
  try {
    const payload = {
      sender_email: String(document.getElementById('access_mail_sender_email')?.value || '').trim(),
      from_email: String(document.getElementById('access_mail_from_email')?.value || '').trim(),
      app_password: String(document.getElementById('access_mail_app_password')?.value || '').trim(),
    };
    const out = await req('/api/admin/mail-config', { method: 'POST', body: JSON.stringify(payload) });
    currentMailConfig = out.config || currentMailConfig;
    renderMailConfig(currentMailConfig);
    setMailConfigNote(t('accessMailSaved'));
  } catch (e) {
    setMailConfigNote(e.message, true);
  }
}

async function saveAccessEntryEditor() {
  const originalEmail = String(accessEntryEditorState.originalEmail || '').trim().toLowerCase();
  const nextEmail = String(document.getElementById('access_entry_email')?.value || '').trim().toLowerCase();
  const nextRole = String(document.getElementById('access_entry_role')?.value || 'user').trim().toLowerCase();
  const nextType = normalizeAccessType(String(document.getElementById('access_entry_type')?.value || 'internal').trim().toLowerCase(), nextEmail);
  if (!isValidAccessEmail(nextEmail)) {
    setAccessEntryNote(t('accessEntryInvalid'), true);
    return;
  }
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy) };
  const allowedSet = new Set(lists.allowed);
  const adminSet = new Set(lists.admins);
  const managedSet = new Set(lists.managed);
  allowedSet.delete(originalEmail);
  adminSet.delete(originalEmail);
  managedSet.delete(originalEmail);
  delete emailTypes[originalEmail];
  if (nextRole === 'admin') {
    adminSet.add(nextEmail);
    if (allowedSet.size) allowedSet.add(nextEmail);
    managedSet.add(nextEmail);
  } else if (allowedSet.size) {
    allowedSet.add(nextEmail);
    managedSet.add(nextEmail);
  } else {
    managedSet.add(nextEmail);
  }
  emailTypes[nextEmail] = nextType;
  currentAccessPolicy = { ...(currentAccessPolicy || {}), managed_emails: Array.from(managedSet), email_types: emailTypes };
  setAccessPolicyListsInEditor(Array.from(allowedSet), Array.from(adminSet));
  try {
    await saveAccessPolicy();
    accessEntryEditorState = { open: false, originalEmail: nextEmail, email: nextEmail, role: nextRole === 'admin' ? 'admin' : 'user', type: nextType };
    renderAccessEntryEditor();
    setAccessPolicyNote(t('accessEntrySaved'));
  } catch (e) {
    await loadAccessPolicy();
    setAccessEntryNote(e.message, true);
  }
}

function setAccessPolicyListsInEditor(allowed, admins) {
  const normalizedAllowed = Array.from(new Set((allowed || []).map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  const normalizedAdmins = Array.from(new Set((admins || []).map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  const currentManaged = Array.isArray(currentAccessPolicy?.managed_emails) ? currentAccessPolicy.managed_emails : [];
  const normalizedManaged = Array.from(new Set(currentManaged.map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  const normalizedTypes = getAccessEmailTypes({ ...(currentAccessPolicy || {}), managed_emails: normalizedManaged, allowed_emails: normalizedAllowed, admin_emails: normalizedAdmins });
  currentAccessPolicy = {
    ...(currentAccessPolicy || {}),
    allowed_emails: normalizedAllowed,
    admin_emails: normalizedAdmins,
    managed_emails: normalizedManaged,
    email_types: normalizedTypes,
  };
  const allowedNode = document.getElementById('access_allowed_emails');
  const adminNode = document.getElementById('access_admin_emails');
  if (allowedNode) allowedNode.value = normalizedAllowed.join('\\n');
  if (adminNode) adminNode.value = normalizedAdmins.join('\\n');
}

async function addAccessEmailFromSearch() {
  const input = document.getElementById('accessDirectorySearch');
  const email = String(input?.value || '').trim().toLowerCase();
  if (!isValidAccessEmail(email)) {
    setAccessPolicyNote(t('accessQuickAddInvalid'), true);
    if (input) input.focus();
    return;
  }
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const managedSet = new Set(lists.managed);
  managedSet.add(email);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy), [email]: normalizeAccessType('', email) };
  if (lists.allowed.length) {
    lists.allowed = Array.from(new Set([...lists.allowed, email]));
  }
  currentAccessPolicy = { ...(currentAccessPolicy || {}), managed_emails: Array.from(managedSet), email_types: emailTypes };
  setAccessPolicyListsInEditor(lists.allowed, lists.admins);
  try {
    await saveAccessPolicy();
    setAccessPolicyNote(t('accessQuickAddDoneFmt')(email));
  } catch (e) {
    await loadAccessPolicy();
    setAccessPolicyNote(e.message, true);
  }
}

async function changeAccessRole(email, nextRole) {
  const target = decodeURIComponent(String(email || '')).trim().toLowerCase();
  if (!target) return;
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const allowedSet = new Set(lists.allowed);
  const adminSet = new Set(lists.admins);
  const managedSet = new Set(lists.managed);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy) };
  if (String(nextRole || '').toLowerCase() === 'admin') {
    adminSet.add(target);
    if (allowedSet.size) allowedSet.add(target);
    managedSet.add(target);
  } else {
    adminSet.delete(target);
    managedSet.add(target);
  }
  emailTypes[target] = normalizeAccessType(emailTypes[target], target);
  currentAccessPolicy = { ...(currentAccessPolicy || {}), managed_emails: Array.from(managedSet), email_types: emailTypes };
  setAccessPolicyListsInEditor(Array.from(allowedSet), Array.from(adminSet));
  try {
    await saveAccessPolicy();
  } catch (e) {
    await loadAccessPolicy();
    setAccessPolicyNote(e.message, true);
  }
}

async function removeAccessEmail(email) {
  const target = decodeURIComponent(String(email || '')).trim().toLowerCase();
  if (!target) return;
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy) };
  delete emailTypes[target];
  currentAccessPolicy = {
    ...(currentAccessPolicy || {}),
    managed_emails: lists.managed.filter(item => item !== target),
    email_types: emailTypes,
  };
  setAccessPolicyListsInEditor(
    lists.allowed.filter(item => item !== target),
    lists.admins.filter(item => item !== target),
  );
  try {
    await saveAccessPolicy();
  } catch (e) {
    await loadAccessPolicy();
    setAccessPolicyNote(e.message, true);
  }
}

function renderAccessPolicySummary(policy = currentAccessPolicy) {
  const data = policy || { allowed_emails: [], admin_emails: [], updated_at: null };
  const { allowed, admins, managed } = getAccessPolicyLists(data);
  const allowedUnion = Array.from(new Set([...managed, ...admins, ...allowed]));
  const updated = data.updated_at ? toLocalStamp(data.updated_at) : '-';
  const host = document.getElementById('accessSummaryTimeline');
  if (!host) return;
  const chips = (items, emptyText) => {
    if (!items.length) return `<span class="access-chip empty">${esc(emptyText)}</span>`;
    return items.map(item => `<span class="access-chip">${esc(item)}</span>`).join('');
  };
  host.innerHTML = [
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryCurrentMail'))}</div><div class="access-summary-main">${esc(authState.email || '-')}</div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryCurrentRole'))}</div><div class="access-summary-main"><span class="access-role-pill ${(authState.role || 'user').toLowerCase() === 'admin' ? 'admin' : 'user'}">${esc(getRoleLabel())}</span></div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryAllowed'))}</div><div class="access-chip-list">${chips(allowed, t('accessSummaryOpen'))}</div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryAdmins'))}</div><div class="access-chip-list">${chips(admins, t('accessSummaryEmptyAdmins'))}</div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryUpdated'))}</div><div class="access-summary-main dim">${esc(updated)}</div></div>`,
  ].join('');
}

function syncAuthUI() {
  const roleBadge = document.getElementById('authRoleBadge');
  if (roleBadge) {
    roleBadge.textContent = getRoleLabel();
    roleBadge.className = `auth-role auth-role-${authState.role || 'user'}`;
  }
  const authEmailNode = document.querySelector('.auth-email');
  if (authEmailNode) {
    const emailText = String(authState.email || '').trim() || '-';
    authEmailNode.textContent = emailText;
    authEmailNode.title = emailText === '-' ? '' : emailText;
  }
  renderOverviewGreeting();
  const accessButton = document.getElementById('access_nav_button');
  if (accessButton) accessButton.style.display = isAdminUser() ? 'flex' : 'none';
  const settingsButton = document.getElementById('settings_nav_button');
  if (settingsButton) settingsButton.style.display = 'flex';
  const accessView = document.getElementById('view-access');
  if (accessView) accessView.style.display = isAdminUser() ? '' : 'none';
  const settingsView = document.getElementById('view-settings');
  if (settingsView) settingsView.style.display = '';
  const stateNode = document.querySelector('#view-settings .state');
  if (stateNode) stateNode.textContent = t('settingsState');
  const accessStateNode = document.querySelector('#view-access .state');
  if (accessStateNode) accessStateNode.textContent = isAdminUser() ? t('accessState') : t('adminOnly');
  if (!isAdminUser() && document.getElementById('view-access')?.classList.contains('active')) {
    switchView('runs');
  }
}

function setSheetUrlHint(text, isError = false) {
  const node = document.getElementById('sheet_url_hint');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function setSheetNameHint(text, isError = false) {
  const node = document.getElementById('sheet_name_hint');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function renderSheetNameSuggestions(titles) {
  const list = document.getElementById('sheet_name_suggestions');
  if (!list) return;
  list.innerHTML = (titles || []).map(title => `<option value="${esc(title)}"></option>`).join('');
}

function getCachedSheetNameTitles(rawUrl, allowStale = false) {
  const entry = sheetNameSuggestCache[String(rawUrl || '').trim()];
  if (!entry || !Array.isArray(entry.titles)) return null;
  if (allowStale) return entry.titles;
  if ((Date.now() - Number(entry.ts || 0)) > SHEET_NAME_CACHE_TTL_MS) return null;
  return entry.titles;
}

function isKnownSheetName(rawUrl, rawName) {
  const titles = getCachedSheetNameTitles(rawUrl, true) || [];
  const target = String(rawName || '').trim().toLowerCase();
  if (!target) return false;
  return titles.some(title => String(title || '').trim().toLowerCase() === target);
}

function rememberResolvedSheetName(rawUrl, sheetTitle) {
  const normalizedUrl = String(rawUrl || '').trim();
  const normalizedTitle = String(sheetTitle || '').trim();
  if (!normalizedUrl || !normalizedTitle) return;
  const entry = sheetNameSuggestCache[normalizedUrl];
  const existing = Array.isArray(entry?.titles)
    ? entry.titles.map(value => String(value || '').trim()).filter(Boolean)
    : [];
  if (!existing.some(value => value.toLowerCase() === normalizedTitle.toLowerCase())) {
    existing.push(normalizedTitle);
  }
  sheetNameSuggestCache[normalizedUrl] = { titles: existing, ts: Date.now() };
}

async function fetchSheetNameSuggestions(force = false) {
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  if (!rawUrl) {
    sheetNameSuggestKey = '';
    renderSheetNameSuggestions([]);
    setSheetUrlHint('');
    setSheetNameHint('');
    return;
  }
  const cached = getCachedSheetNameTitles(rawUrl, false);
  if (cached && (!force || sheetNameSuggestKey === rawUrl)) {
    renderSheetNameSuggestions(cached);
    setSheetUrlHint(cached.length ? t('sheetUrlHintCountFmt')(cached.length) : t('sheetUrlHintEmpty'));
    return;
  }
  if (sheetNameSuggestInflight[rawUrl]) {
    const pendingTitles = await sheetNameSuggestInflight[rawUrl];
    sheetNameSuggestKey = rawUrl;
    renderSheetNameSuggestions(pendingTitles);
    setSheetUrlHint(pendingTitles.length ? t('sheetUrlHintCountFmt')(pendingTitles.length) : t('sheetUrlHintEmpty'));
    return;
  }
  setSheetUrlHint(t('sheetUrlHintLoading'));
  try {
    sheetNameSuggestInflight[rawUrl] = (async () => {
      const qs = new URLSearchParams({ sheet_url: rawUrl });
      if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
      const out = await req('/api/sheets/names?' + qs.toString());
      return Array.isArray(out.titles) ? out.titles : [];
    })();
    const titles = await sheetNameSuggestInflight[rawUrl];
    sheetNameSuggestKey = rawUrl;
    sheetNameSuggestCache[rawUrl] = { titles, ts: Date.now() };
    renderSheetNameSuggestions(titles);
    if (!String(document.getElementById('sheet_name')?.value || '').trim() && titles.length === 1) {
      document.getElementById('sheet_name').value = titles[0];
    }
    setSheetUrlHint(titles.length ? t('sheetUrlHintCountFmt')(titles.length) : t('sheetUrlHintEmpty'));
  } catch (e) {
    const staleTitles = getCachedSheetNameTitles(rawUrl, true);
    if (staleTitles) {
      renderSheetNameSuggestions(staleTitles);
      setSheetUrlHint(staleTitles.length ? t('sheetUrlHintCountFmt')(staleTitles.length) : t('sheetUrlHintEmpty'));
    } else {
      renderSheetNameSuggestions([]);
      setSheetUrlHint(e.message, true);
    }
  } finally {
    delete sheetNameSuggestInflight[rawUrl];
  }
}

function scheduleSheetNameSuggestions(force = false) {
  if (sheetNameSuggestTimer) clearTimeout(sheetNameSuggestTimer);
  sheetNameSuggestTimer = setTimeout(() => {
    fetchSheetNameSuggestions(force);
  }, force ? 0 : 800);
}

function bindSheetNameAutocomplete() {
  const urlInput = document.getElementById('sheet_url');
  const nameInput = document.getElementById('sheet_name');
  if (!urlInput || urlInput.dataset.sheetSuggestBound === '1') return;
  urlInput.dataset.sheetSuggestBound = '1';
  ['input', 'change', 'paste'].forEach(evt => {
    urlInput.addEventListener(evt, () => {
      scheduleSheetNameSuggestions(false);
      resetSheetLinkSuggestions();
      setSheetNameHint('');
    });
  });
  urlInput.addEventListener('blur', () => {
    scheduleSheetNameSuggestions(true);
  });
  if (nameInput) {
    ['input', 'change', 'paste'].forEach(evt => {
      nameInput.addEventListener(evt, () => {
        resetSheetLinkSuggestions();
        setSheetNameHint('');
        scheduleSheetLinkCountSummary(false);
      });
    });
    nameInput.addEventListener('blur', () => {
      scheduleSheetLinkCountSummary(true);
    });
    nameInput.addEventListener('focus', () => {
      const rawUrl = String(urlInput.value || '').trim();
      if (rawUrl && !getCachedSheetNameTitles(rawUrl, false)) scheduleSheetNameSuggestions(true);
    });
  }
}

function renderRunShareInfo(settings) {
  const s = settings || {};
  const emailNode = document.getElementById('runShareEmail');
  if (!emailNode) return;
  emailNode.textContent = s.service_account_email || t('noServiceEmail');
}

function renderServiceAccountCard(settings) {
  const s = settings || {};
  const card = document.getElementById('settings_service_card');
  if (!card) return;
  card.style.display = s.service_account_fixed ? 'none' : '';
}

function resetServiceAccountFileInput() {
  const fileInput = document.getElementById('settings_service_account_file');
  const hiddenInput = document.getElementById('settings_service_account_json');
  const hint = document.getElementById('settings_service_account_file_hint');
  if (fileInput) fileInput.value = '';
  if (hiddenInput) hiddenInput.value = '';
  if (hint) {
    delete hint.dataset.fileName;
    hint.textContent = t('serviceJsonNoFile');
  }
}

function handleServiceAccountFileChange(event) {
  const input = event?.target || document.getElementById('settings_service_account_file');
  const file = input?.files?.[0];
  const hiddenInput = document.getElementById('settings_service_account_json');
  const hint = document.getElementById('settings_service_account_file_hint');
  if (!file) {
    if (hiddenInput) hiddenInput.value = '';
    if (hint) {
      delete hint.dataset.fileName;
      hint.textContent = t('serviceJsonNoFile');
    }
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    if (hiddenInput) hiddenInput.value = String(reader.result || '');
    if (hint) {
      hint.dataset.fileName = file.name;
      hint.textContent = t('serviceJsonSelectedFmt')(file.name);
    }
  };
  reader.onerror = () => {
    if (hiddenInput) hiddenInput.value = '';
    if (hint) {
      delete hint.dataset.fileName;
      hint.textContent = t('serviceJsonNoFile');
    }
    setSettingsNote(t('serviceJsonReadError'), true);
  };
  reader.readAsText(file, 'utf-8');
}

function renderSettingsSummary(settings) {
  const s = settings || {};
  document.getElementById('settings_summary_viewport').textContent = `${s.viewport_width || '-'} x ${s.viewport_height || '-'}`;
  document.getElementById('settings_summary_timeout').textContent = `${s.page_timeout_ms || '-'} ms`;
  document.getElementById('settings_summary_full_page').textContent = s.full_page_capture ? t('fullPage') : t('viewportOnly');
  const serviceState = s.service_account_fixed ? t('fixedCredentials') : (s.service_account_saved ? t('saved') : t('notSaved'));
  document.getElementById('settings_summary_service_account').textContent = serviceState;
  document.getElementById('settings_summary_service_email').textContent = s.service_account_email || t('noServiceEmail');
  renderRunShareInfo(s);
  renderServiceAccountCard(s);
  const status = document.getElementById('settings_service_status');
  status.className = 'badge ' + (s.service_account_saved ? 'ok' : 'info');
  status.textContent = serviceState;
}

async function loadDefaults() {
  const [d, s] = await Promise.all([req('/api/default-config'), req('/api/settings')]);
  currentSettingsCache = s || {};
  currentMappingBlocksByMode = normalizeMappingsByModeForClient(currentSettingsCache.mappings_by_mode || {});
  currentRunFlagsByMode = normalizeRunFlagsByModeForClient(currentSettingsCache.run_flags_by_mode || {});
  sheet_url.value = s.sheet_url || d.sheet_url || '';
  sheet_name.value = s.sheet_name || d.sheet_name || '';
  drive_id.value = s.drive_id || d.drive_id || '';
  applyRunFlagsForMode(currentRunMode);
  document.getElementById('settings_viewport_width').value = s.viewport_width || 1920;
  document.getElementById('settings_viewport_height').value = s.viewport_height || 1400;
  document.getElementById('settings_page_timeout_ms').value = s.page_timeout_ms || 3000;
  document.getElementById('settings_scan_negative_terms').value = s.scan_negative_terms || '';
  document.getElementById('settings_full_page_capture').checked = !!s.full_page_capture;
  renderSettingsSummary(s);
  if (isAdminUser()) await Promise.all([loadAccessPolicy(), loadMailConfig()]);
  if (String(sheet_url.value || '').trim()) {
    scheduleSheetNameSuggestions(false);
    if (String(sheet_name.value || '').trim()) scheduleSheetLinkCountSummary(false);
  } else {
    setSheetUrlHint('');
    setSheetNameHint('');
  }
  resetSheetLinkSuggestions();
}

async function saveSidebarSettings() {
  try {
    rememberCurrentRunFlags(currentRunMode);
    const payload = {
      credentials_path: currentSettingsCache.credentials_path || '',
      service_account_json: document.getElementById('settings_service_account_json').value,
      sheet_url: sheet_url.value,
      sheet_name: sheet_name.value,
      drive_id: drive_id.value,
      scan_negative_terms: document.getElementById('settings_scan_negative_terms').value,
      viewport_width: Number(document.getElementById('settings_viewport_width').value || 1920),
      viewport_height: Number(document.getElementById('settings_viewport_height').value || 1400),
      page_timeout_ms: Number(document.getElementById('settings_page_timeout_ms').value || 3000),
      ready_state: currentSettingsCache.ready_state || 'interactive',
      full_page_capture: document.getElementById('settings_full_page_capture').checked,
      mappings_by_mode: serializeMappingsByModeForSave(),
      run_flags_by_mode: currentRunFlagsByMode,
    };
    const out = await req('/api/settings', { method: 'POST', body: JSON.stringify(payload) });
    const saved = out.settings || payload;
    currentSettingsCache = saved;
    currentMappingBlocksByMode = normalizeMappingsByModeForClient(saved.mappings_by_mode || serializeMappingsByModeForSave());
    currentRunFlagsByMode = normalizeRunFlagsByModeForClient(saved.run_flags_by_mode || currentRunFlagsByMode);
    applyRunFlagsForMode(currentRunMode);
    resetServiceAccountFileInput();
    renderSettingsSummary(saved);
    if (String(sheet_url.value || '').trim()) {
      scheduleSheetNameSuggestions(false);
    }
    resetSheetLinkSuggestions();
    setSettingsNote(t('saved'));
  } catch (e) {
    setSettingsNote(e.message, true);
  }
}

async function loadAccessPolicy() {
  if (!isAdminUser()) return;
  try {
    const out = await req('/api/admin/access-policy');
    currentAccessPolicy = out.policy || { allowed_emails: [], admin_emails: [] };
    syncAccessPolicyEditors(currentAccessPolicy);
    renderAccessDirectory(currentAccessPolicy);
    renderAccessEntryEditor();
    renderAccessPolicySummary(currentAccessPolicy);
    setAccessPolicyNote('');
  } catch (e) {
    setAccessPolicyNote(e.message, true);
  }
}

async function saveAccessPolicy() {
  if (!isAdminUser()) {
    setAccessPolicyNote(t('adminOnly'), true);
    return;
  }
  try {
    const allowedNode = document.getElementById('access_allowed_emails');
    const adminNode = document.getElementById('access_admin_emails');
    const payload = {
      allowed_emails: allowedNode ? allowedNode.value : (currentAccessPolicy.allowed_emails || []).join('\\n'),
      admin_emails: adminNode ? adminNode.value : (currentAccessPolicy.admin_emails || []).join('\\n'),
      managed_emails: Array.isArray(currentAccessPolicy.managed_emails) ? currentAccessPolicy.managed_emails : [],
      email_types: currentAccessPolicy.email_types || {},
    };
    const out = await req('/api/admin/access-policy', { method: 'POST', body: JSON.stringify(payload) });
    currentAccessPolicy = out.policy || {};
    syncAccessPolicyEditors(currentAccessPolicy);
    renderAccessDirectory(currentAccessPolicy);
    renderAccessPolicySummary(currentAccessPolicy);
    const sentCount = Array.isArray(out.notifications?.sent) ? out.notifications.sent.length : 0;
    const failedCount = Array.isArray(out.notifications?.failed) ? out.notifications.failed.length : 0;
    if (sentCount && failedCount) setAccessPolicyNote(t('accessNotifyPartialFmt')(sentCount, failedCount));
    else if (sentCount) setAccessPolicyNote(`${t('accessPolicySaved')} · ${t('accessNotifySentFmt')(sentCount)}`);
    else if (failedCount) setAccessPolicyNote(t('accessNotifyPartialFmt')(0, failedCount), true);
    else setAccessPolicyNote(t('accessPolicySaved'));
  } catch (e) {
    setAccessPolicyNote(e.message, true);
    throw e;
  }
}

async function launchChrome() {
  try {
    const browserPort = getModeBasePort(currentRunMode);
    const out = await req('/api/chrome/launch', {
      method: 'POST',
      body: JSON.stringify({ run_mode: currentRunMode, browser_port: browserPort })
    });
    await logActivityEvent({
      kind: 'chrome',
      level: 'info',
      run_mode: currentRunMode,
      browser_port: browserPort,
      message: `${prettyWord(currentRunMode)}: đã mở Chrome ${browserPort}`,
    });
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) { alert(e.message); }
}

function buildMappingsForCurrentMode() {
  return ensureMappingBlocks(currentRunMode).map((block, index) => sanitizeMappingBlockForMode(currentRunMode, block, index + 1));
}

async function startJob() {
  try {
    primeCompletionNotifications();
    const mappings = buildMappingsForCurrentMode();
    const firstStartLine = mappings.length ? Number(mappings[0].start_line || 4) : 4;
    const modeFlags = rememberCurrentRunFlags(currentRunMode);
    const forceRunAll = !!modeFlags.force_run_all;
    const highlightSheetErrors = !!modeFlags.highlight_sheet_errors;
    const scanNegativeFilter = currentRunMode === 'scan' && !!modeFlags.scan_negative_filter;
    const browserPort = getModeBasePort(currentRunMode);
    const out = await req('/api/jobs/start', {
      method: 'POST',
      body: JSON.stringify({
        run_mode: currentRunMode,
        sheet_url: sheet_url.value,
        sheet_name: sheet_name.value,
        drive_id: drive_id.value,
        browser_port: browserPort,
        start_line: firstStartLine,
        mappings,
        force_run_all: !!forceRunAll,
        highlight_sheet_errors: !!highlightSheetErrors,
        scan_negative_filter: !!scanNegativeFilter,
        credentials_input: currentSettingsCache.credentials_path || '',
        capture_five_per_link: currentRunMode === 'booking' && captureFivePerLink,
        auto_launch_chrome: DEFAULT_AUTO_LAUNCH_CHROME
      })
    });
    currentJobId = out.job_id;
    setSelectedJobIdForMode(currentRunMode, out.job_id);
    await refreshJobs();
    await pollCurrent();
    ensureTimers();
  } catch (e) {
    if (await focusBlockingModeJob(e, currentRunMode)) return;
    alert(e.message);
  }
}

async function startErrorRowsJob() {
  if (!currentJobId) { alert(t('monitorNoJob')); return; }
  try {
    primeCompletionNotifications();
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    if (!isJobOwnedByCurrentUser(st)) {
      throw new Error('Chỉ chạy lại lỗi được với job của chính bạn');
    }
    const errorRowCount = Object.keys(st?.error_rows || {}).length;
    if (!errorRowCount) {
      throw new Error('Job này chưa có dòng lỗi để chạy lại');
    }
    const out = await req('/api/jobs/' + currentJobId + '/retry-errors', { method: 'POST' });
    const runMode = getJobMode(st);
    currentJobId = out.job_id;
    setSelectedJobIdForMode(runMode, out.job_id);
    await refreshJobs();
    await pollCurrent();
    ensureTimers();
    setStatus(`${t('errorOnlyStarted')} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
    const jobMode = getJobMode(currentJobSnapshot || {});
    if (await focusBlockingModeJob(e, jobMode || currentRunMode)) return;
    alert(e.message);
  }
}

async function stopJob() {
  if (!currentJobId) { alert('Choose a job first'); return; }
  try {
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    const status = String(st?.status || '').toLowerCase();
    if (!['running', 'paused'].includes(status)) {
      throw new Error('Ch? c? th? d?ng job ?ang ch?y');
    }
    await req('/api/jobs/' + currentJobId + '/stop', { method: 'POST' });
    await pollCurrent();
    await refreshJobs();
  } catch (e) { alert(e.message); }
}

async function continueJob() {
  if (!currentJobId) { alert(t('monitorNoJob')); return; }
  try {
    primeCompletionNotifications();
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    const status = String(st?.status || '').toLowerCase();
    const jobMode = getJobMode(st);
    if (!['stopped', 'failed', 'completed'].includes(status)) {
      throw new Error('Chỉ có thể chạy tiếp từ job đã dừng, lỗi hoặc hoàn tất');
    }
    const out = await req('/api/jobs/' + currentJobId + '/continue', { method: 'POST' });
    currentJobId = out.job_id;
    setSelectedJobIdForMode(jobMode, out.job_id);
    await refreshJobs();
    await pollCurrent();
    ensureTimers();
    setStatus(`${t('continueStarted')} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
    const jobMode = getJobMode(currentJobSnapshot || {});
    if (await focusBlockingModeJob(e, jobMode || currentRunMode)) return;
    alert(e.message);
  }
}

async function pauseJob() {
  return stopJob();
}

async function refreshJobs() {
  try {
    const [out, activityOut] = await Promise.all([
      req('/api/jobs'),
      req('/api/activity?limit=0'),
    ]);
    const jobs = out.jobs || [];
    currentActivityEvents = activityOut.items || [];
    processJobLifecycleNotifications(jobs);
    jobsCache = jobs;
    syncModeSelections();
    if (currentJobId && !jobs.some(job => job.id === currentJobId)) currentJobId = null;
    if (!currentJobId && jobs.length) currentJobId = jobs[0].id;
    if (document.getElementById('view-runs')?.classList.contains('active')) {
      currentJobId = resolveModeJobId(currentRunMode);
      if (!currentJobId) {
        currentJobSnapshot = null;
        currentLogsCache = [];
        renderRunMonitor(null, []);
      }
    }
    document.getElementById('jobCountText').textContent = t('jobsLoadedFmt')(jobs.length);
    document.getElementById('jobCountText').dataset.jobs = jobs.length;
    const rows = jobs.map(j => {
      const s = j.summary || { done: 0, total: 0 };
      const active = currentJobId === j.id ? 'active' : '';
      const modeLabel = getJobMode(j).slice(0, 3).toUpperCase();
      const ownerLabel = getJobOwnerBadge(j);
      return `<tr class="${active}" onclick="selectJob('${j.id}')"><td>${statusBadge(j.status)}</td><td title="${esc(getJobMode(j))} · ${esc(j.id)}">${esc(modeLabel)} · ${esc(j.id.slice(0,8))}${ownerLabel ? `<div class="muted" style="font-size:11px;margin-top:2px">${esc(ownerLabel)}</div>` : ''}</td><td>${s.done}/${s.total}</td></tr>`;
    }).join('');
    document.getElementById('jobsBody').innerHTML = rows;
    renderOverview();
    renderProjects();
    renderActivities(getCombinedActivities());
    return true;
  } catch (e) {
    setStatus('Load jobs error: ' + e.message, 'failed');
    return false;
  }
}

function resetSyncFeedback(btn) {
  if (!btn) return;
  btn.classList.remove('is-loading', 'is-done', 'is-error');
  btn.disabled = false;
  const label = btn.querySelector('span');
  if (label) label.textContent = t('sync');
}

async function refreshJobsWithFeedback(btn) {
  if (!btn || btn.classList.contains('is-loading')) return;
  if (syncFeedbackTimer) {
    clearTimeout(syncFeedbackTimer);
    syncFeedbackTimer = null;
  }
  const label = btn.querySelector('span');
  btn.classList.remove('is-done', 'is-error');
  btn.classList.add('is-loading');
  btn.disabled = true;
  if (label) label.textContent = t('syncing');
  const ok = await refreshJobs();
  btn.classList.remove('is-loading');
  btn.classList.add(ok ? 'is-done' : 'is-error');
  if (label) label.textContent = ok ? t('synced') : t('syncFailed');
  syncFeedbackTimer = setTimeout(() => resetSyncFeedback(btn), 1400);
}

function selectJob(jobId) {
  currentJobId = jobId;
  const matched = (jobsCache || []).find(job => job.id === jobId);
  if (matched) {
    setSelectedJobIdForMode(getJobMode(matched), jobId);
  }
  pollCurrent();
  refreshJobs();
}

async function pollCurrent() {
  if (!currentJobId) return;
  try {
    const st = await req('/api/jobs/' + currentJobId);
    currentJobSnapshot = st;
    const s = st.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
    setKPI(s, currentJobId);
    setStatus('Status: ' + st.status + ' | Detail: ' + (st.detail || '-'), st.status);
    const lg = await req('/api/jobs/' + currentJobId + '/logs?limit=200');
    const logs = lg.logs || [];
    currentLogsCache = logs;
    const targetJob = (jobsCache || []).find(job => job.id === currentJobId);
    if (targetJob) targetJob.recent_logs = logs.slice();
    renderRunMonitor(st, logs);
    updateRunActionButtons(st);
    renderOverview();
    renderProjects();
    renderActivities(getCombinedActivities());
  } catch (e) {
    setStatus('Poll error: ' + e.message, 'failed');
  }
}

function ensureTimers() {
  if (!pollTimer) pollTimer = setInterval(pollCurrent, 450);
  if (!jobsTimer) jobsTimer = setInterval(refreshJobs, 1800);
}

async function init() {
  await loadAuthState();
  await detectLocalAgent();
  syncAuthUI();
  switchView('runs', document.querySelector('.side-btn[data-view="runs"]'));
  bindSheetNameAutocomplete();
  await loadDefaults();
  await refreshJobs();
  await pollCurrent();
  renderOverview();
  renderActivities(getCombinedActivities());
  renderRunMonitor(null, []);
  renderAccessPolicySummary(currentAccessPolicy);
  ensureTimers();
  applyTheme();
  applyLanguage();
  setStatus('ready', 'idle');
}

init().catch(e => setStatus('Init error: ' + e.message, 'failed'));
