
let currentJobId = null;
let pollTimer = null;
let jobsTimer = null;
let jobsCache = [];
let currentJobSnapshot = null;
let currentLogsCache = [];
let currentJobIdByMode = { seeding: null, booking: null, scan: null };
let currentProjectJobId = null;
let currentProjectModeFilter = 'all';
let currentSettingsCache = {};
let currentRunMode = 'seeding';
let currentMappingBlocksByMode = {};
let captureFivePerLink = false;
let sheetNameSuggestTimer = null;
let sheetNameSuggestKey = '';
let sheetNameSuggestCache = {};
let pendingMappingScrollMode = '';
let pendingMappingHighlightIndex = -1;
let currentAccessPolicy = { allowed_emails: [], admin_emails: [], updated_at: null };
const BROWSER_PORT_BY_MODE = { seeding: 9223, booking: 9423, scan: 9623 };
const DEFAULT_AUTO_LAUNCH_CHROME = false;
let currentLang = localStorage.getItem('ui_lang') || 'vi';
let currentTheme = localStorage.getItem('ui_theme') || 'light';
const authState = {
  email: 'thu.phannguyenanh@fanscom.vn',
  role: 'admin',
  isAdmin: true,
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
    settings: 'Cài đặt',
    state: 'Trạng thái',
    openRuns: 'Mở Runs',
    view: 'Xem',
    sync: 'Đồng bộ',
    goToRuns: 'Đi tới Runs',
    selectedJob: 'Job đang chọn',
    storedJobs: 'Job đã lưu',
    successFailed: 'Thành công / Lỗi',
    overallProgress: 'Tiến độ tổng',
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
    sheetNameHintLoading: 'Đang tải danh sách sheet...',
    sheetNameHintEmpty: 'Không tìm thấy sheet nào trong file này',
    sheetNameHintCountFmt: count => `Tìm thấy ${count} sheet`,
    browserPort: 'Browser Port',
    startLine: 'Dòng bắt đầu',
    autoLaunchChrome: 'Tự mở Chrome',
    startJob: 'Chạy job',
    overwriteRun: 'Chạy đè',
    stopJob: 'Dừng',
    resumeJob: 'Tiếp tục',
    refreshJobs: 'Làm mới job',
    runQueue: 'Hàng đợi job',
    runQueueHelp: 'Chọn job để theo dõi. Mỗi mode được chạy 1 job cùng lúc.',
    liveLogs: 'Live log',
    errorRows: 'Dòng lỗi',
    selectedJobMeta: 'Job đang chọn',
    monitorKicker: '4. Kết quả & Theo dõi',
    monitorTitle: 'Theo dõi tiến độ và lỗi',
    monitorJob: 'Job',
    monitorProgress: 'Tiến độ',
    monitorErrors: 'Lỗi theo link sheet',
    monitorTable: 'Bảng log xử lý',
    monitorNoJob: 'Chưa chọn job',
    monitorNoErrors: 'Không có lỗi',
    monitorNoLogs: 'Chưa có dữ liệu',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Success ${ok} · Failed ${fail} · Không khả dụng ${unavailable}`,
    unavailableLabel: 'Không khả dụng',
    time: 'Time',
    post: 'Post',
    result: 'Kết quả',
    message: 'Thông điệp',
    replay: 'Replay',
    exportLog: 'Xuất log Excel',
    noLogsToExport: 'Chưa có log để xuất',
    replayStartedFmt: row => `Đã tạo replay cho dòng ${row}`,
    noData: 'Chưa có dữ liệu',
    projectsState: 'Lưu các run hoàn tất và xem lại chi tiết',
    groupedProjects: 'Dự án đã lưu',
    completedGroups: 'Sheet đã lưu',
    largestGroup: 'Dự án đang chọn',
    groupedRegistry: 'Thư viện dự án',
    groupSnapshot: 'Chi tiết dự án',
    allProjects: 'Tất cả',
    noProjectsInFilter: 'Chưa có dự án trong nhóm này',
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
    recentTimeline: 'Dòng thời gian gần nhất',
    settingsState: 'Cấu hình đã lưu',
    settingsTitle: 'Thông số screenshot & credentials',
    settingsHelp: 'Các giá trị này sẽ được áp dụng cho các job mới. Bạn cũng có thể dán JSON service account để lưu một lần.',
    accessPolicyTitle: 'Phân quyền truy cập',
    accessPolicyHelp: 'Admin quản lý mail nào được đăng nhập và mail nào có quyền admin.',
    accessAllowedLabel: 'Mail được phép đăng nhập',
    accessAllowedHelp: 'Để trống nếu muốn mọi mail xác thực OTP đều có thể vào web.',
    accessAdminLabel: 'Mail admin',
    accessAdminHelp: 'Mail admin luôn được giữ quyền quản trị và tự nằm trong allowlist nếu allowlist có dùng.',
    saveAccessPolicy: 'Lưu phân quyền',
    reloadAccessPolicy: 'Tải lại phân quyền',
    accessPolicySaved: 'Đã lưu phân quyền',
    accessPolicySelfProtect: 'Không thể tự gỡ quyền admin của chính bạn trong phiên này',
    viewportWidth: 'Viewport width',
    viewportHeight: 'Viewport height',
    pageTimeout: 'Timeout tải trang (ms)',
    waitReadyState: 'Chờ trang ở trạng thái',
    fullPageCapture: 'Chụp full page',
    fullPageHelp: 'Bật nếu bạn muốn giữ toàn bộ chiều dài trang thay vì chỉ phần đang thấy.',
    jsonServiceAccount: 'JSON service account',
    jsonHelp: 'Dán JSON để lưu cục bộ và tự cập nhật credentials path.',
    serviceJsonLabel: 'Nội dung JSON mới',
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
    settings: 'Settings',
    state: 'State',
    openRuns: 'Open Runs',
    view: 'View',
    sync: 'Sync',
    goToRuns: 'Go To Runs',
    selectedJob: 'Selected job',
    storedJobs: 'Stored jobs',
    successFailed: 'Success / Failed',
    overallProgress: 'Overall progress',
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
    sheetNameHintLoading: 'Loading sheet names...',
    sheetNameHintEmpty: 'No sheets found in this spreadsheet',
    sheetNameHintCountFmt: count => `${count} sheets found`,
    browserPort: 'Browser Port',
    startLine: 'Start Line',
    autoLaunchChrome: 'Auto Launch Chrome',
    startJob: 'Start Job',
    overwriteRun: 'Overwrite',
    stopJob: 'Pause',
    resumeJob: 'Resume',
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
    monitorErrors: 'Errors by sheet link',
    monitorTable: 'Processing log table',
    monitorNoJob: 'No job selected',
    monitorNoErrors: 'No errors',
    monitorNoLogs: 'No data yet',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Success ${ok} · Failed ${fail} · Unavailable ${unavailable}`,
    unavailableLabel: 'Unavailable',
    time: 'Time',
    post: 'Post',
    result: 'Result',
    message: 'Message',
    replay: 'Replay',
    exportLog: 'Export Excel Log',
    noLogsToExport: 'No logs to export',
    replayStartedFmt: row => `Replay job queued for row ${row}`,
    noData: 'No data',
    projectsState: 'Store completed runs and reopen their details',
    groupedProjects: 'Saved Projects',
    completedGroups: 'Saved Sheets',
    largestGroup: 'Selected Project',
    groupedRegistry: 'Project Library',
    groupSnapshot: 'Project Detail',
    allProjects: 'All',
    noProjectsInFilter: 'No projects in this category',
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
    recentTimeline: 'Recent Timeline',
    settingsState: 'Saved configuration',
    settingsTitle: 'Screenshot & credentials',
    settingsHelp: 'These values are reused by future jobs. You can also paste service account JSON here and save it once.',
    accessPolicyTitle: 'Access control',
    accessPolicyHelp: 'Admins manage which emails can log in and which emails keep admin permission.',
    accessAllowedLabel: 'Allowed emails',
    accessAllowedHelp: 'Leave empty if any OTP-verified email can enter the web app.',
    accessAdminLabel: 'Admin emails',
    accessAdminHelp: 'Admin emails always keep admin permission and are auto-kept in the allowlist when one is used.',
    saveAccessPolicy: 'Save Access',
    reloadAccessPolicy: 'Reload Access',
    accessPolicySaved: 'Access control saved',
    accessPolicySelfProtect: 'You cannot remove your own admin right in this session',
    viewportWidth: 'Viewport width',
    viewportHeight: 'Viewport height',
    pageTimeout: 'Page timeout (ms)',
    waitReadyState: 'Wait ready state',
    fullPageCapture: 'Full page capture',
    fullPageHelp: 'Enable this if you want to keep the entire page length instead of only the visible area.',
    jsonServiceAccount: 'JSON service account',
    jsonHelp: 'Paste JSON to save it locally and update the credentials path automatically.',
    serviceJsonLabel: 'Service account JSON',
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

function isAdminUser() {
  return !!authState.isAdmin;
}

function getRunModeLabel(mode) {
  return t(String(mode || 'seeding').toLowerCase());
}

function formatRunTitle(mode = currentRunMode) {
  return `${t('runs')} - ${getRunModeLabel(mode)}`;
}

function formatRunConfigTitle(mode = currentRunMode) {
  return `${t('runConfig')} - ${getRunModeLabel(mode)}`;
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

function updateMappingBlock(mode, index, key, value) {
  const blocks = ensureMappingBlocks(mode);
  if (!blocks[index]) return;
  blocks[index][key] = key === 'start_line' ? Number(value || 4) : String(value || '');
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

async function launchChromeBlock(index) {
  try {
    const out = await req(`/api/chrome/launch-block/${Number(index)}?run_mode=${encodeURIComponent(currentRunMode)}`, { method: 'POST' });
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
    host.innerHTML = `<div class="mapping-scan-grid">${blocks.map((block, index) => {
      const title = block.name || `Scan ${index + 1}`;
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        const inputType = field.type === 'number' ? 'number' : 'text';
        return `<div class="mapping-label">${esc(field.label)}</div><div><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      return `<section class="mapping-block">
        <div class="mapping-block-head">
          <div class="mapping-block-title">${esc(title)}</div>
          ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
        </div>
        <div class="mapping-block-grid">${rows}</div>
      </section>`;
    }).join('')}</div>`;
  } else if (currentRunMode === 'seeding') {
    host.innerHTML = `<div class="mapping-seeding-row">${blocks.map((block, index) => {
      const blockClass = pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex === index
        ? 'mapping-block mapping-block-new'
        : 'mapping-block';
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        if (field.key === 'col_air_date') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input class="mapping-input" type="text" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" onclick="openAirDatePicker('${currentRunMode}', ${index})">...</button><input id="air_date_picker_${currentRunMode}_${index}" type="date" style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px" onchange="applyAirDate('${currentRunMode}', ${index}, this.value)" /></div>`;
        }
        const inputType = field.type === 'number' ? 'number' : 'text';
        if (field.key === 'name') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" />${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}</div>`;
        }
        return `<div class="mapping-label">${esc(field.label)}</div><div><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      const chromeRow = `<div class="mapping-label">${esc(t('chrome'))}</div><div><button class="btn mapping-chrome-btn" type="button" onclick="launchChromeBlock(${index})">${esc(`${t('chrome')} ${getChromePortForBlock(index, currentRunMode)}`)}</button></div>`;
      return `<section class="${blockClass}"><div class="mapping-block-grid">${rows}${chromeRow}</div></section>`;
    }).join('')}</div>`;
  } else {
    const colTemplate = `132px repeat(${blocks.length}, minmax(110px, 1fr))`;
    const nameRow = [
      `<div class="mapping-matrix-label">${esc(t('postName'))}</div>`,
      ...blocks.map((block, index) => {
        const title = block.name || `Post ${index + 1}`;
        return `<div class="mapping-matrix-name">
          <input class="mapping-input" type="text" value="${esc(title)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, 'name', this.value)" />
          ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
        </div>`;
      })
    ].join('');
    const rows = fields
      .filter(field => field.key !== 'name')
      .map(field => {
        const cells = blocks.map((block, index) => {
          const value = block[field.key] ?? '';
          const inputType = field.type === 'number' ? 'number' : 'text';
          if (field.key === 'col_air_date') {
            return `<div class="mapping-field-combo"><input class="mapping-input" type="text" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" onclick="openAirDatePicker('${currentRunMode}', ${index})">...</button><input id="air_date_picker_${currentRunMode}_${index}" type="date" style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px" onchange="applyAirDate('${currentRunMode}', ${index}, this.value)" /></div>`;
          }
          return `<div><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
        }).join('');
        return `<div class="mapping-matrix-label">${esc(field.label)}</div>${cells}`;
      }).join('');
    const chromeRow = [
      `<div class="mapping-matrix-label">${esc(t('chrome'))}</div>`,
      ...blocks.map((_, index) => `<div><button class="btn mapping-chrome-btn" type="button" onclick="launchChromeBlock(${index})">${esc(`${t('chrome')} ${getChromePortForBlock(index, currentRunMode)}`)}</button></div>`)
    ].join('');
    host.innerHTML = `<section class="mapping-matrix"><div class="mapping-matrix-grid" style="grid-template-columns:${colTemplate}">${nameRow}${rows}${chromeRow}</div></section>`;
  }
  const addRow = document.querySelector('.mapping-add-row');
  if (addRow) {
    const bookingExtra = currentRunMode === 'booking'
      ? `<label class="mapping-check"><input type="checkbox" ${captureFivePerLink ? 'checked' : ''} onchange="toggleCaptureFivePerLink(this.checked)" /> <span>${esc(t('captureFive'))}</span></label>`
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
  const runConfigTitle = document.getElementById('runConfigTitle');
  if (runConfigTitle) runConfigTitle.textContent = formatRunConfigTitle(currentRunMode);
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', document.getElementById('view-runs')?.classList.contains('active'));
  renderMappingEditor();
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

  const menuMap = { overview: 'overview', runs: 'runs', projects: 'projects', tasks: 'tasks', activities: 'activities', settings: 'settings' };
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
  setText('#view-tasks .h1', t('tasks'));
  setText('#view-activities .h1', t('activities'));
  setText('#view-settings .h1', t('settings'));
  setText('#view-projects .state', t('projectsState'));
  setText('#view-tasks .state', t('tasksState'));
  setText('#view-activities .state', t('activitiesState'));
  setText('#view-settings .state', t('settingsState'));
  setText('#view-runs .state', t('runConfigHelp'));

  setText('#view-overview .stats .stat:nth-child(1) .k', t('jobsToday'));
  setText('#view-overview .stats .stat:nth-child(1) .s', t('createdLast24h'));
  setText('#view-overview .stats .stat:nth-child(2) .k', t('avgSuccess'));
  setText('#view-overview .stats .stat:nth-child(2) .s', t('acrossTracked'));
  setText('#view-overview .stats .stat:nth-child(3) .k', t('latestJob'));
  setText('#view-overview .stats .stat:nth-child(4) .k', t('topError'));
  setText('#ovHistoryTitle', t('overviewTimeline'));
  setText('#ovLegendSuccess', t('overviewCompletedLegend'));
  setText('#ovLegendFailed', t('overviewFailedLegend'));
  setText('#ovLegendUnavailable', t('overviewUnavailableLegend'));
  setText('#view-overview .overview-note .btn', t('goToRuns'));
  setText('#view-overview aside .right-top > div:first-child', t('runSummary'));
  setText('#view-overview aside .right-top > div:nth-child(2)', t('overviewClean'));
  setText('#view-overview .item:nth-child(1) .t', t('selectedJob'));
  setText('#view-overview .item:nth-child(1) .btn', t('openRuns'));
  setText('#view-overview .item:nth-child(2) .t', t('storedJobs'));
  setText('#view-overview .item:nth-child(2) .btn', t('sync'));
  setText('#view-overview .item:nth-child(3) .t', t('successFailed'));
  setText('#view-overview .item:nth-child(3) .btn', t('view'));
  setText('#view-overview .mini > div span:first-child', t('overallProgress'));
  setNthText('#view-overview .day', 0, t('totalScope'));
  setNthText('#view-overview .day', 1, t('done'));
  setNthText('#view-overview .day', 2, t('success'));
  setNthText('#view-overview .day', 3, t('failed'));
  setNthText('#view-overview .day', 4, t('jobs'));

  setText('#view-runs .headline .state', t('runConfigHelp'));
  setText('#runShareLabel', t('runShareLabel'));
  setText('#runConfigTitle', formatRunConfigTitle());
  applyRunModeUI();
  setText('label[for="sheet_url"]', t('sheetUrl'));
  setText('label[for="sheet_name"]', t('sheetName'));
  setText('label[for="drive_id"]', t('driveFolder'));
  setText('#startJobLabel', t('startJob'));
  setText('#overwriteRunLabel', t('overwriteRun'));
  setText('#runMonitorKicker', t('monitorKicker'));
  setText('#runMonitorTitle', t('monitorTitle'));
  setText('#runMonitorJobLabel', t('monitorJob'));
  setText('#runMonitorProgressLabel', t('monitorProgress'));
  setText('#runMonitorErrorLabel', t('monitorErrors'));
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

  setText('#view-tasks .cards-3 .card:nth-child(1) .k', t('done'));
  setText('#view-tasks .cards-3 .card:nth-child(1) .s', t('rowsProcessed'));
  setText('#view-tasks .cards-3 .card:nth-child(2) .k', t('pending'));
  setText('#view-tasks .cards-3 .card:nth-child(2) .s', t('rowsRemaining'));
  setText('#view-tasks .cards-3 .card:nth-child(3) .k', t('success'));
  setText('#view-tasks .cards-3 .card:nth-child(3) .s', t('rowsPassed'));
  setText('#view-tasks .cards-3 .card:nth-child(4) .k', t('failed'));
  setText('#view-tasks .cards-3 .card:nth-child(4) .s', t('rowsNeedRetry'));
  setText('#view-tasks .bottom:nth-of-type(1) .card:first-child > div:first-child', t('taskDistribution'));
  setText('#view-tasks .bottom:nth-of-type(1) .card:last-child > div:first-child', t('progressOverTime'));
  setText('#view-tasks .bottom:nth-of-type(2) .card:first-child > div:first-child', t('errorQueue'));
  setText('#view-tasks .bottom:nth-of-type(2) .card:last-child > div:first-child', t('currentProgress'));

  setText('#view-activities .card > div:first-child', t('recentTimeline'));

  setText('#view-settings .settings-layout .card:first-child > div:first-child', t('settingsTitle'));
  setText('#view-settings .settings-layout .card:first-child > div:nth-child(2)', t('settingsHelp'));
  setText('label[for="settings_viewport_width"]', t('viewportWidth'));
  setText('label[for="settings_viewport_height"]', t('viewportHeight'));
  setText('label[for="settings_page_timeout_ms"]', t('pageTimeout'));
  setText('#view-settings .list-row div div:first-child', t('fullPageCapture'));
  setText('#view-settings .list-row .muted', t('fullPageHelp'));
  setText('#view-settings .settings-layout .card:first-child .card > div:first-child', t('jsonServiceAccount'));
  setText('#view-settings .settings-layout .card:first-child .card > div:nth-child(2)', t('jsonHelp'));
  setText('label[for="settings_service_account_json"]', t('serviceJsonLabel'));
  setText('#saveSettingsButton', t('saveSettings'));
  setText('#reloadSettingsButton', t('reloadSettings'));
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
  const nextMode = String(mode || 'seeding').toLowerCase();
  currentRunMode = ['seeding', 'booking', 'scan'].includes(nextMode) ? nextMode : 'seeding';
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
  renderTasks(currentJobSnapshot?.summary || null, currentJobSnapshot?.error_rows || {}, currentLogsCache);
  renderActivities(currentLogsCache);
  renderRunMonitor(currentJobSnapshot, currentLogsCache);
  if (String(document.getElementById('sheet_url')?.value || '').trim()) scheduleSheetNameSuggestions(false);
}

function toggleLanguage() {
  setLanguage(currentLang === 'vi' ? 'en' : 'vi');
}

async function req(url, opts = {}) {
  const res = await fetch(url, { headers: { 'Content-Type': 'application/json' }, ...opts });
  const data = await res.json().catch(() => ({}));
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error(data.detail || 'Authentication required');
  }
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
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
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
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
      unavailable: 0,
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

function getJobSummary(job) {
  return job?.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
}

function getJobSheetLabel(job) {
  const req = job?.request || {};
  return req.sheet_name || req.sheet_url || 'Unknown sheet';
}

function getJobMode(job) {
  return String(job?.mode || job?.request?.mode || job?.request?.mappings?.[0]?.mode || 'seeding').toLowerCase();
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

function resolveModeJobId(mode) {
  const jobs = getJobsByMode(mode);
  if (!jobs.length) return null;
  const selected = getSelectedJobIdForMode(mode);
  const matched = selected ? jobs.find(job => job.id === selected) : null;
  return matched ? matched.id : jobs[0].id;
}

function syncModeSelections() {
  ['seeding', 'booking', 'scan'].forEach(mode => {
    setSelectedJobIdForMode(mode, resolveModeJobId(mode));
  });
}

function getSavedProjectJobs() {
  return (jobsCache || []).filter(job => job.status === 'completed');
}

function getFilteredProjectJobs() {
  const saved = getSavedProjectJobs();
  if (currentProjectModeFilter === 'all') return saved;
  return saved.filter(job => getJobMode(job) === currentProjectModeFilter);
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

function openProjectInRuns(jobId) {
  const job = (jobsCache || []).find(item => item.id === jobId);
  if (!job) return;
  const request = job.request || {};
  const mode = getJobMode(job);
  sheet_url.value = request.sheet_url || '';
  sheet_name.value = request.sheet_name || '';
  drive_id.value = request.drive_id || '';
  document.getElementById('force_run_all').checked = !!request.force_run_all;
  currentMappingBlocksByMode[mode] = (request.mappings || []).length
    ? request.mappings.map((block, index) => sanitizeMappingBlockForMode(mode, block, index + 1))
    : [defaultMappingBlock(mode, 1)];
  captureFivePerLink = !!request.capture_five_per_link;
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
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('fail') || raw.includes('error')) return 'error';
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return 'warning';
  if (raw.includes('warn') || raw.includes('quota')) return 'warning';
  return 'info';
}

function prettyWord(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  return raw.charAt(0).toUpperCase() + raw.slice(1);
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

function isUnavailableLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  return raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung');
}

function canReplayLog(log) {
  const row = Number(log?.row || 0);
  if (!Number.isFinite(row) || row < 1) return false;
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''}`.toLowerCase();
  return raw.includes('ok') || raw.includes('fail') || raw.includes('unavailable');
}

function statusBadge(status) {
  const key = String(status || '').toLowerCase();
  if (key === 'completed') return '<span class="badge ok">completed</span>';
  if (key === 'failed') return '<span class="badge error">failed</span>';
  if (key === 'running') return '<span class="badge info">running</span>';
  if (key === 'paused') return '<span class="badge warning">paused</span>';
  if (key === 'stopped') return '<span class="badge warning">stopped</span>';
  return `<span class="badge info">${esc(key || 'idle')}</span>`;
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

function buildProgressBuckets(logs) {
  const buckets = new Map();
  (logs || []).forEach(log => {
    const dt = new Date(log.ts || '');
    const key = Number.isNaN(dt.getTime())
      ? 'unknown'
      : `${dt.getHours().toString().padStart(2, '0')}:${dt.getMinutes().toString().padStart(2, '0')}`;
    buckets.set(key, (buckets.get(key) || 0) + 1);
  });
  return [...buckets.entries()].slice(-6);
}

function renderOverview() {
  const todayKey = toCalendarDayKey(new Date().toISOString());
  const todayJobs = jobsCache.filter(j => toCalendarDayKey(j.created_at || '') === todayKey);
  const ratios = jobsCache
    .map(j => {
      const s = getJobSummary(j);
      return s.total > 0 ? (s.success / s.total) * 100 : null;
    })
    .filter(v => v !== null);
  const avg = ratios.length ? Math.round(ratios.reduce((a, b) => a + b, 0) / ratios.length) : 0;
  const latest = jobsCache[0] || null;
  const topError = aggregateErrorCounts(jobsCache)[0] || null;

  document.getElementById('ovTodayJobs').textContent = todayJobs.length;
  document.getElementById('ovAvgSuccess').textContent = avg + '%';
  document.getElementById('ovLatestJob').textContent = latest ? latest.id.slice(0, 8) : '-';
  document.getElementById('ovLatestMeta').textContent = latest
    ? t('latestJobMetaFmt')(latest.status, toLocalStamp(latest.created_at))
    : t('noRecentRun');
  document.getElementById('ovTopError').textContent = topError ? String(topError[1]) : '-';
  document.getElementById('ovTopErrorMeta').textContent = topError ? topError[0] : t('noRecurring');

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
  if (name === 'settings' && !isAdminUser()) {
    setStatus(t('adminOnly'), 'failed');
    name = 'overview';
    tabEl = document.querySelector('.side-btn[data-view="overview"]');
  }
  document.querySelectorAll('.view').forEach(node => node.classList.remove('active'));
  const view = document.getElementById('view-' + name);
  if (view) view.classList.add('active');
  document.querySelectorAll('.side-btn[data-view]').forEach(node => node.classList.remove('active'));
  const activeTab = tabEl || document.querySelector(`.side-btn[data-view="${name}"]`);
  if (activeTab) activeTab.classList.add('active');
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', name === 'runs');
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
  chip.textContent = `${t('state')}: ` + (status || 'idle');
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
  const saved = getFilteredProjectJobs();
  const selected = getSelectedProjectJob();
  const uniqueSheets = new Set(saved.map(job => getJobSheetLabel(job))).size;
  const summary = getJobSummary(selected);
  const completionText = String(selected?.completion?.summary || '').trim();
  const request = selected?.request || {};
  const filterOptions = [
    { key: 'all', label: t('allProjects'), count: allSaved.length },
    { key: 'seeding', label: getRunModeLabel('seeding'), count: allSaved.filter(job => getJobMode(job) === 'seeding').length },
    { key: 'booking', label: getRunModeLabel('booking'), count: allSaved.filter(job => getJobMode(job) === 'booking').length },
    { key: 'scan', label: getRunModeLabel('scan'), count: allSaved.filter(job => getJobMode(job) === 'scan').length },
  ];
  document.getElementById('projectsTotalJobs').textContent = saved.length;
  document.getElementById('projectsCompletedJobs').textContent = uniqueSheets;
  document.getElementById('projectsSelectedJob').textContent = selected ? `${summary.done || 0}/${summary.total || 0}` : '-';
  document.getElementById('projectsModeFilters').innerHTML = filterOptions.map(opt => {
    const active = currentProjectModeFilter === opt.key ? ' active' : '';
    return `<button type="button" class="project-mode-filter mode-${opt.key}${active}" onclick="setProjectModeFilter('${opt.key}')">${esc(opt.label)}<span>${opt.count}</span></button>`;
  }).join('');
  document.getElementById('projectsSnapshotAction').innerHTML = selected
    ? `<div class="project-detail-actions"><button type="button" class="project-nav-btn" title="${esc(t('openProjectRun'))}" aria-label="${esc(t('openProjectRun'))}" onclick="openProjectInRuns('${selected.id}')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h12"></path><path d="m13 6 6 6-6 6"></path></svg></button></div>`
    : '';
  document.getElementById('projectsList').innerHTML = saved.length
    ? saved.map(job => {
        const jobSummary = getJobSummary(job);
        const active = currentProjectJobId === job.id ? ' active' : '';
        const mode = getJobMode(job);
        return `<div class="list-row project-item${active}" onclick="selectProject('${job.id}')">
          <div class="project-item-main">
            <div class="project-item-title">${esc(getJobSheetLabel(job))}</div>
            <div class="project-item-meta"><span class="mode-pill mode-${mode}">${esc(prettyWord(mode))}</span><span>${esc(toLocalStamp(job.finished_at || job.created_at))}</span><span>${esc(job.id.slice(0, 8))}</span></div>
          </div>
          <div class="project-item-side">
            <span>${jobSummary.success || 0}/${jobSummary.total || 0}</span>
            ${isAdminUser() ? `<button type="button" class="project-delete-btn" title="${esc(t('deleteLabel'))}" onclick="deleteProject('${job.id}', event)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M6 7l1 12h10l1-12"></path><path d="M9 7V4h6v3"></path></svg>
            </button>` : ''}
          </div>
        </div>`;
      }).join('')
    : `<div class="list-row"><span>${allSaved.length ? t('noProjectsInFilter') : t('noGroupsYet')}</span><span>-</span></div>`;
  document.getElementById('projectsSnapshot').innerHTML = selected
    ? [
        `<div class="timeline-item"><strong>${t('group')}</strong><div>${esc(getJobSheetLabel(selected))}</div></div>`,
        `<div class="timeline-item"><strong>${t('state')}</strong><div>${esc(prettyWord(selected.status))} · <span class="mode-pill mode-${getJobMode(selected)}">${esc(prettyWord(getJobMode(selected)))}</span></div></div>`,
        `<div class="timeline-item"><strong>${t('latestUpdate')}</strong><div>${esc(toLocalStamp(selected.finished_at || selected.created_at))}</div></div>`,
        `<div class="timeline-item"><strong>${t('jobs')}</strong><div>${summary.done || 0}/${summary.total || 0} · ${summary.success || 0} ok · ${summary.failed || 0} ${t('failedLabel').toLowerCase()}</div></div>`,
        `<div class="timeline-item"><strong>${t('driveFolder')}</strong><div>${esc(request.drive_id || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('detailLabel')}</strong><div>${esc(selected.detail || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('summaryLabel')}</strong><div style="white-space:pre-line">${esc(completionText || '-')}</div></div>`,
      ].join('')
    : `<div class="timeline-item"><strong>${t('noProjectGroup')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function renderTasks(summary, errorRows, logs) {
  const s = summary || { done: 0, success: 0, failed: 0, total: 0 };
  const pending = Math.max(0, (s.total || 0) - (s.done || 0));
  document.getElementById('tasksDone').textContent = s.done || 0;
  document.getElementById('tasksPending').textContent = pending;
  document.getElementById('tasksSuccess').textContent = s.success || 0;
  document.getElementById('tasksFailed').textContent = s.failed || 0;
  document.getElementById('tasksDistribution').innerHTML = [
    [t('totalScope'), s.total || 0],
    [t('processed'), s.done || 0],
    [t('pending'), pending],
    [t('succeeded'), s.success || 0],
    [t('failedLabel'), s.failed || 0],
  ].map(([a,b]) => `<div class="list-row"><span>${a}</span><span>${b}</span></div>`).join('');
  const keys = Object.keys(errorRows || {});
  document.getElementById('tasksErrors').innerHTML = keys.length
    ? keys.sort((a,b)=>Number(a)-Number(b)).slice(0, 12).map(k => `<div class="list-row"><span>${t('rowFmt')(k)}</span><span>${esc(errorRows[k])}</span></div>`).join('')
    : `<div class="list-row"><span>${t('noErrors')}</span><span>${t('clear')}</span></div>`;
  const buckets = buildProgressBuckets(logs || []);
  const maxVal = Math.max(1, ...buckets.map(([,v]) => v));
  document.getElementById('tasksTimelineBars').innerHTML = buckets.length
    ? buckets.map(([label, val], idx, arr) => {
        const h = Math.max(20, Math.round((val / maxVal) * 140));
        return `<div class="mini-bar"><div class="mini-bar-value">${val}</div><div class="mini-bar-fill ${idx === arr.length - 1 ? 'active' : ''}" style="height:${h}px"></div><div class="mini-bar-label">${esc(label)}</div></div>`;
      }).join('')
    : `<div class="list-row"><span>${t('noProgressHistory')}</span><span>-</span></div>`;
  document.getElementById('tasksProgressMeta').innerHTML = [
    `<div class="timeline-item"><strong>${t('done')} / ${t('totalScope')}</strong><div>${s.done || 0} / ${s.total || 0}</div></div>`,
    `<div class="timeline-item"><strong>${t('pendingFailed')}</strong><div>${pending} / ${s.failed || 0}</div></div>`,
    `<div class="timeline-item"><strong>${t('eta')}</strong><div>${esc(s.eta || '---')}</div></div>`,
  ].join('');
}

function renderActivities(logs) {
  const items = (logs || []).slice(-10).reverse();
  document.getElementById('activitiesTimeline').innerHTML = items.length
    ? items.map(x => {
        const level = classifyLog(x);
        return `<div class="timeline-item"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>#${x.row} ${esc(x.state)}/${esc(x.result)}</strong><span class="badge ${level}">${level}</span></div><div>${esc(x.message)}</div><div class="s">${toLocalStamp(x.ts)}</div></div>`;
      }).join('')
    : `<div class="timeline-item"><strong>${t('noActivity')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function renderRunMonitor(snapshot, logs) {
  const st = snapshot || {};
  const s = st.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
  const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
  const errorRows = st.error_rows || {};
  const errorKeys = Object.keys(errorRows);
  const unavailableCount = (logs || []).filter(isUnavailableLog).length;
  const statusLabel = prettyWord(st.status || 'idle');
  const latestLog = (logs || []).length ? logs[logs.length - 1] : null;
  const detailText = String(st.detail || latestLog?.message || '').trim();
  const etaText = s.eta && s.eta !== '---' ? `${t('eta')}: ${s.eta}` : '';
  const title = st.request ? getJobSheetLabel(st) : t('monitorNoJob');
  const metaParts = [];
  if (st.mode || st.request?.mode) metaParts.push(prettyWord(getJobMode(st)));
  if (currentJobId) metaParts.push(currentJobId.slice(0, 8));
  if (st.created_at) metaParts.push(toLocalStamp(st.created_at));
  const statusNode = document.getElementById('runMonitorStatus');
  statusNode.textContent = statusLabel;
  statusNode.style.background = 'var(--blue-soft)';
  statusNode.style.color = 'var(--blue)';
  statusNode.style.borderColor = 'rgba(91,147,211,.25)';
  if (st.status === 'completed') {
    statusNode.style.background = 'rgba(52,195,143,.16)';
    statusNode.style.color = 'var(--green)';
    statusNode.style.borderColor = 'rgba(52,195,143,.35)';
  } else if (st.status === 'paused') {
    statusNode.style.background = 'rgba(245,158,11,.16)';
    statusNode.style.color = '#b45309';
    statusNode.style.borderColor = 'rgba(245,158,11,.35)';
  } else if (st.status === 'failed') {
    statusNode.style.background = 'rgba(240,138,160,.16)';
    statusNode.style.color = 'var(--red)';
    statusNode.style.borderColor = 'rgba(240,138,160,.35)';
  } else if (st.status === 'stopped') {
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
  document.getElementById('runMonitorErrorMain').textContent = errorKeys.length ? `${errorKeys.length}` : t('monitorNoErrors');
  document.getElementById('runMonitorErrorMeta').textContent = errorKeys.length
    ? errorKeys.slice(0, 5).map(x => `#${x}`).join(', ')
    : t('monitorSuccessFailedFmt')(s.success || 0, s.failed || 0, unavailableCount);

  const rows = (logs || []).slice().reverse();
  const replayLocked = ['running', 'paused'].includes(String(st.status || '').toLowerCase());
  document.getElementById('runMonitorRows').innerHTML = rows.length
    ? rows.map(x => {
        const postName = getLogPostLabel(x);
        const message = x.message || `${x.state}/${x.result}`;
        const replayBlockName = extractLogBlockName(x);
        const replayButton = canReplayLog(x)
          ? `<button class="monitor-replay-btn" type="button" ${replayLocked ? 'disabled title="Job đang chạy, chưa thể replay"' : `onclick="replayLogRow('${esc(st.id || currentJobId || '')}', ${Number(x.row || 0)}, '${esc(replayBlockName)}')"`}>
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
  const stopLabel = document.getElementById('stopJobLabel');
  const stopIcon = document.getElementById('stopJobIcon');
  const stopButton = stopLabel ? stopLabel.closest('button') : null;
  if (!stopLabel || !stopIcon || !stopButton) return;
  const status = String(snapshot?.status || '').toLowerCase();
  const paused = status === 'paused';
  stopLabel.textContent = paused ? t('resumeJob') : t('stopJob');
  stopIcon.innerHTML = paused
    ? '<path d="M8 6.5v11l9-5.5-9-5.5Z"></path>'
    : '<rect x="7" y="7" width="10" height="10" rx="1.5"></rect>';
  stopButton.classList.remove('blue');
  stopButton.classList.toggle('resume', paused);
  stopButton.classList.toggle('red', !paused);
}

async function replayLogRow(jobId, row, blockName = '') {
  try {
    if (!jobId) throw new Error('No job selected');
    const sourceJobId = jobId;
    const payload = {
      row: Number(row || 0),
      block_name: String(blockName || ''),
    };
    const out = await req(`/api/jobs/${jobId}/replay-row`, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    await refreshJobs();
    currentJobId = sourceJobId;
    setSelectedJobIdForMode(currentRunMode, sourceJobId);
    await pollCurrent();
    setStatus(`${t('replayStartedFmt')(payload.row)} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
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
  link.href = `/api/jobs/${encodeURIComponent(jobId)}/export-log?ts=${Date.now()}`;
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

function setAccessPolicyNote(text, isError = false) {
  const node = document.getElementById('access_policy_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function syncAuthUI() {
  const roleBadge = document.getElementById('authRoleBadge');
  if (roleBadge) {
    roleBadge.textContent = getRoleLabel();
    roleBadge.className = `auth-role auth-role-${authState.role || 'user'}`;
  }
  const settingsButton = document.getElementById('settings_nav_button');
  if (settingsButton) settingsButton.style.display = isAdminUser() ? 'flex' : 'none';
  const accessCard = document.getElementById('accessPolicyCard');
  if (accessCard) accessCard.style.display = isAdminUser() ? '' : 'none';
  const settingsActionButtons = document.querySelectorAll('#view-settings .run-actions button');
  settingsActionButtons.forEach(button => {
    if (!(button instanceof HTMLButtonElement)) return;
    if (button.id === 'saveAccessButton' || button.id === 'reloadAccessButton') return;
    button.style.display = isAdminUser() ? '' : 'none';
  });
  const stateNode = document.querySelector('#view-settings .state');
  if (stateNode) stateNode.textContent = isAdminUser() ? t('settingsState') : t('adminOnly');
  if (!isAdminUser() && document.getElementById('view-settings')?.classList.contains('active')) {
    switchView('overview');
  }
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

async function fetchSheetNameSuggestions(force = false) {
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  if (!rawUrl) {
    sheetNameSuggestKey = '';
    renderSheetNameSuggestions([]);
    setSheetNameHint('');
    return;
  }
  if (!force && sheetNameSuggestKey === rawUrl && Array.isArray(sheetNameSuggestCache[rawUrl])) {
    const cached = sheetNameSuggestCache[rawUrl];
    renderSheetNameSuggestions(cached);
    setSheetNameHint(cached.length ? t('sheetNameHintCountFmt')(cached.length) : t('sheetNameHintEmpty'));
    return;
  }
  setSheetNameHint(t('sheetNameHintLoading'));
  try {
    const qs = new URLSearchParams({ sheet_url: rawUrl });
    if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
    const out = await req('/api/sheets/names?' + qs.toString());
    const titles = Array.isArray(out.titles) ? out.titles : [];
    sheetNameSuggestKey = rawUrl;
    sheetNameSuggestCache[rawUrl] = titles;
    renderSheetNameSuggestions(titles);
    if (!String(document.getElementById('sheet_name')?.value || '').trim() && titles.length === 1) {
      document.getElementById('sheet_name').value = titles[0];
    }
    setSheetNameHint(titles.length ? t('sheetNameHintCountFmt')(titles.length) : t('sheetNameHintEmpty'));
  } catch (e) {
    renderSheetNameSuggestions([]);
    setSheetNameHint(e.message, true);
  }
}

function scheduleSheetNameSuggestions(force = false) {
  if (sheetNameSuggestTimer) clearTimeout(sheetNameSuggestTimer);
  sheetNameSuggestTimer = setTimeout(() => {
    fetchSheetNameSuggestions(force);
  }, force ? 0 : 450);
}

function bindSheetNameAutocomplete() {
  const urlInput = document.getElementById('sheet_url');
  const nameInput = document.getElementById('sheet_name');
  if (!urlInput || urlInput.dataset.sheetSuggestBound === '1') return;
  urlInput.dataset.sheetSuggestBound = '1';
  ['input', 'change', 'paste'].forEach(evt => {
    urlInput.addEventListener(evt, () => scheduleSheetNameSuggestions(false));
  });
  urlInput.addEventListener('blur', () => scheduleSheetNameSuggestions(true));
  if (nameInput) {
    nameInput.addEventListener('focus', () => {
      if (String(urlInput.value || '').trim()) scheduleSheetNameSuggestions(true);
    });
  }
}

function renderRunShareInfo(settings) {
  const s = settings || {};
  const emailNode = document.getElementById('runShareEmail');
  if (!emailNode) return;
  emailNode.textContent = s.service_account_email || t('noServiceEmail');
}

function renderSettingsSummary(settings) {
  const s = settings || {};
  document.getElementById('settings_summary_viewport').textContent = `${s.viewport_width || '-'} x ${s.viewport_height || '-'}`;
  document.getElementById('settings_summary_timeout').textContent = `${s.page_timeout_ms || '-'} ms`;
  document.getElementById('settings_summary_full_page').textContent = s.full_page_capture ? t('fullPage') : t('viewportOnly');
  document.getElementById('settings_summary_service_account').textContent = s.service_account_saved ? t('saved') : t('notSaved');
  document.getElementById('settings_summary_service_email').textContent = s.service_account_email || t('noServiceEmail');
  renderRunShareInfo(s);
  const status = document.getElementById('settings_service_status');
  status.className = 'badge ' + (s.service_account_saved ? 'ok' : 'info');
  status.textContent = s.service_account_saved ? t('saved') : t('notSaved');
}

async function loadDefaults() {
  const [d, s] = await Promise.all([req('/api/default-config'), req('/api/settings')]);
  currentSettingsCache = s || {};
  sheet_url.value = d.sheet_url || s.sheet_url || '';
  sheet_name.value = d.sheet_name || s.sheet_name || '';
  drive_id.value = d.drive_id || s.drive_id || '';
  document.getElementById('settings_viewport_width').value = s.viewport_width || 1920;
  document.getElementById('settings_viewport_height').value = s.viewport_height || 1400;
  document.getElementById('settings_page_timeout_ms').value = s.page_timeout_ms || 3000;
  document.getElementById('settings_full_page_capture').checked = !!s.full_page_capture;
  renderSettingsSummary(s);
  if (isAdminUser()) await loadAccessPolicy();
  if (String(sheet_url.value || '').trim()) scheduleSheetNameSuggestions(true);
  else setSheetNameHint('');
}

async function saveSidebarSettings() {
  if (!isAdminUser()) {
    setSettingsNote(t('adminOnly'), true);
    return;
  }
  try {
    const payload = {
      credentials_path: currentSettingsCache.credentials_path || '',
      service_account_json: document.getElementById('settings_service_account_json').value,
      sheet_url: sheet_url.value,
      sheet_name: sheet_name.value,
      drive_id: drive_id.value,
      viewport_width: Number(document.getElementById('settings_viewport_width').value || 1920),
      viewport_height: Number(document.getElementById('settings_viewport_height').value || 1400),
      page_timeout_ms: Number(document.getElementById('settings_page_timeout_ms').value || 3000),
      ready_state: currentSettingsCache.ready_state || 'interactive',
      full_page_capture: document.getElementById('settings_full_page_capture').checked,
    };
    const out = await req('/api/settings', { method: 'POST', body: JSON.stringify(payload) });
    const saved = out.settings || payload;
    currentSettingsCache = saved;
    document.getElementById('settings_service_account_json').value = '';
    renderSettingsSummary(saved);
    if (String(sheet_url.value || '').trim()) scheduleSheetNameSuggestions(true);
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
    const allowed = (currentAccessPolicy.allowed_emails || []).join('
');
    const admins = (currentAccessPolicy.admin_emails || []).join('
');
    document.getElementById('access_allowed_emails').value = allowed;
    document.getElementById('access_admin_emails').value = admins;
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
    const payload = {
      allowed_emails: document.getElementById('access_allowed_emails').value,
      admin_emails: document.getElementById('access_admin_emails').value,
    };
    const out = await req('/api/admin/access-policy', { method: 'POST', body: JSON.stringify(payload) });
    currentAccessPolicy = out.policy || {};
    document.getElementById('access_allowed_emails').value = (currentAccessPolicy.allowed_emails || []).join('
');
    document.getElementById('access_admin_emails').value = (currentAccessPolicy.admin_emails || []).join('
');
    setAccessPolicyNote(t('accessPolicySaved'));
  } catch (e) {
    setAccessPolicyNote(e.message, true);
  }
}

async function launchChrome() {
  try {
    const out = await req('/api/chrome/launch', {
      method: 'POST',
      body: JSON.stringify({ run_mode: currentRunMode, browser_port: getModeBasePort(currentRunMode) })
    });
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) { alert(e.message); }
}

function buildMappingsForCurrentMode() {
  return ensureMappingBlocks(currentRunMode).map((block, index) => sanitizeMappingBlockForMode(currentRunMode, block, index + 1));
}

async function startJob() {
  try {
    const mappings = buildMappingsForCurrentMode();
    const firstStartLine = mappings.length ? Number(mappings[0].start_line || 4) : 4;
    const forceRunAll = !!document.getElementById('force_run_all')?.checked;
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
  } catch (e) { alert(e.message); }
}

async function stopJob() {
  if (!currentJobId) { alert('Choose a job first'); return; }
  try {
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    const status = String(st?.status || '').toLowerCase();
    if (!['running', 'paused'].includes(status)) {
      throw new Error('Job này không ở trạng thái dừng / tiếp tục được');
    }
    await req('/api/jobs/' + currentJobId + '/pause-toggle', { method: 'POST' });
    await pollCurrent();
    await refreshJobs();
  } catch (e) { alert(e.message); }
}

async function refreshJobs() {
  try {
    const out = await req('/api/jobs');
    const jobs = out.jobs || [];
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
      return `<tr class="${active}" onclick="selectJob('${j.id}')"><td>${statusBadge(j.status)}</td><td title="${esc(getJobMode(j))} · ${esc(j.id)}">${esc(modeLabel)} · ${esc(j.id.slice(0,8))}</td><td>${s.done}/${s.total}</td></tr>`;
    }).join('');
    document.getElementById('jobsBody').innerHTML = rows;
    renderOverview();
    renderProjects();
  } catch (e) {
    setStatus('Load jobs error: ' + e.message, 'failed');
  }
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
    renderRunMonitor(st, logs);
    updateRunActionButtons(st);
    renderOverview();
    renderProjects();
    renderTasks(s, st.error_rows || {}, logs);
    renderActivities(logs);
  } catch (e) {
    setStatus('Poll error: ' + e.message, 'failed');
  }
}

function ensureTimers() {
  if (!pollTimer) pollTimer = setInterval(pollCurrent, 800);
  if (!jobsTimer) jobsTimer = setInterval(refreshJobs, 3000);
}

async function init() {
  syncAuthUI();
  bindSheetNameAutocomplete();
  await loadDefaults();
  await refreshJobs();
  await pollCurrent();
  renderOverview();
  renderTasks(null, {}, []);
  renderActivities([]);
  renderRunMonitor(null, []);
  ensureTimers();
  applyTheme();
  applyLanguage();
  setStatus('ready', 'idle');
}

init().catch(e => setStatus('Init error: ' + e.message, 'failed'));
