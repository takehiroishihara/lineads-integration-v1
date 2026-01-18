/**
 * LINE広告 API - BigQuery転送スクリプト
 *
 * スプレッドシートで管理された複数のアカウントからLINE広告データを取得し、
 * BigQueryのテーブルに出力します。
 *
 * 【対応レポート】
 * - ADレポート（日別）
 * - キャンペーン設定
 * - 広告グループ設定
 * - メディア一覧
 * - 性別レポート
 * - 年齢レポート
 * - デバイス（OS）レポート
 *
 * 【アカウント管理】
 * LINE広告にはMCC機能がないため、アカウント情報はスプレッドシートで管理します。
 * 「LINE広告アカウント一覧」シートに以下の情報を入力してください：
 * - A列: アカウントID
 * - B列: アカウント名
 * - C列: AccessKey
 * - D列: SecretKey
 */

// ===========================================
// 共通設定
// ===========================================

const CONFIG = {
  // LINE Ads API設定
  LINE_ADS_API_BASE: 'https://ads.line.me',
  LINE_ADS_API_PATH: '/api',

  // レポート設定
  DAY_COUNT: 45,
  INCLUDE_TODAY: false,

  // BigQuery設定
  BQ_PROJECT_ID: 'your-project-id',
  BQ_DATASET_ID: 'line_ads_raw',

  // BigQueryテーブル名設定
  TABLES: {
    ACCOUNT_LIST: 'account_list',
    CAMPAIGN: 'campaign_settings',
    ADGROUP: 'adgroup_settings',
    AD: 'ad_report',
    MEDIA: 'media_master',
    GENDER: 'gender_report',
    AGE: 'age_report',
    DEVICE: 'device_report'
  },

  // レポートポーリング設定
  REPORT_POLL_INTERVAL_MS: 10000,
  REPORT_POLL_MAX_ATTEMPTS: 30,

  // アカウント間の待機時間
  ACCOUNT_WAIT_MS: 2000
};

// ===========================================
// BigQuery 転送用共通関数
// ===========================================

/**
 * 2次元配列データをCSVに変換してBigQueryにロードする
 */
function loadToBigQuery_(tableId, dataHeader, dataBody) {
  if (!dataBody || dataBody.length === 0) {
    log_(`⚠ ${tableId}: データがないためスキップします`);
    return;
  }

  log_(`🚀 BigQuery転送開始: ${tableId} (${dataBody.length}件)`);

  const allData = [dataHeader, ...dataBody];

  const csvString = allData.map(row => {
    return row.map(cell => {
      const str = String(cell === null || cell === undefined ? '' : cell);
      if (str.includes('"') || str.includes(',') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    }).join(',');
  }).join('\n');

  const blob = Utilities.newBlob(csvString, 'application/octet-stream');

  const job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: CONFIG.BQ_PROJECT_ID,
          datasetId: CONFIG.BQ_DATASET_ID,
          tableId: tableId
        },
        writeDisposition: 'WRITE_TRUNCATE',
        createDisposition: 'CREATE_IF_NEEDED',
        sourceFormat: 'CSV',
        autodetect: true,
        skipLeadingRows: 1
      }
    }
  };

  try {
    const insertJob = BigQuery.Jobs.insert(job, CONFIG.BQ_PROJECT_ID, blob);
    log_(`✅ BigQueryジョブ投入成功: JobId ${insertJob.jobReference.jobId}`);
  } catch (e) {
    log_(`❌ BigQuery転送エラー: ${e.message}`);
    throw e;
  }
}

// ===========================================
// 共通ユーティリティ関数
// ===========================================

/**
 * ログ出力
 */
function log_(message) {
  Logger.log(message);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName('ログ');
    if (logSheet) {
      const now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
      logSheet.appendRow([now, message]);
    }
  } catch (e) {
    // ログ出力エラーは無視
  }
}

/**
 * 日付範囲計算
 */
function getDateRange_(dayCount, includeToday) {
  const now = new Date();
  const end = new Date(now);

  if (!includeToday) {
    end.setDate(end.getDate() - 1);
  }

  const start = new Date(end);
  start.setDate(start.getDate() - (dayCount - 1));

  const startStr = Utilities.formatDate(start, 'Asia/Tokyo', 'yyyy-MM-dd');
  const endStr = Utilities.formatDate(end, 'Asia/Tokyo', 'yyyy-MM-dd');

  return { startStr, endStr };
}

/**
 * 対象アカウント一覧を取得（スプレッドシートから）
 *
 * LINE広告にはMCC機能がないため、アカウント情報はスプレッドシートで管理します。
 * 各アカウントごとにAccessKey/SecretKeyが異なるため、アカウントごとに認証を行います。
 *
 * シート形式:
 * - A列: アカウントID
 * - B列: アカウント名
 * - C列: AccessKey
 * - D列: SecretKey
 */
function getTargetAccounts_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('LINE広告アカウント一覧');

  if (!sheet || sheet.getLastRow() < 2) {
    log_('⚠ LINE広告アカウント一覧シートがないか、データがありません');
    return [];
  }

  const lastRow = sheet.getLastRow();
  const data = sheet.getRange(2, 1, lastRow - 1, 4).getValues();
  const accounts = [];

  data.forEach((row, index) => {
    const accountId = String(row[0]).trim();
    const accountName = String(row[1] || '').trim();
    const accessKey = String(row[2]).trim();
    const secretKey = String(row[3]).trim();

    if (accountId && accessKey && secretKey) {
      accounts.push({
        accountId: accountId,
        accountName: accountName,
        accessKey: accessKey,
        secretKey: secretKey
      });
    } else if (accountId) {
      log_(`⚠ 行${index + 2}: アカウントID ${accountId} の認証情報が不完全です`);
    }
  });

  return accounts;
}

// ===========================================
// LINE Ads APIクライアントクラス
// ===========================================

/**
 * LINE Ads APIクライアント
 *
 * 各アカウントごとに異なる認証情報（AccessKey/SecretKey）を使用するため、
 * アカウントごとにクライアントインスタンスを作成します。
 */
class LineAdsClient {
  constructor(accountId, accessKey, secretKey) {
    this.accountId = accountId;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  /**
   * APIリクエスト実行
   */
  request(method, endpoint, payload = {}, expectJson = true) {
    const baseUrl = CONFIG.LINE_ADS_API_BASE;
    const canonicalURI = CONFIG.LINE_ADS_API_PATH + endpoint;
    let url = baseUrl + canonicalURI;
    let contentType, payloadStr;

    const options = {
      method: method,
      muteHttpExceptions: true
    };

    if (method === 'GET') {
      payloadStr = '';
      contentType = '';
      if (Object.keys(payload).length) {
        url += this._buildQueryString(payload);
      }
    } else {
      payloadStr = this._jsonStringify(payload);
      contentType = 'application/json';
      options.payload = payloadStr;
    }

    // 認証ヘッダーを毎回生成（アカウントごとの認証情報を使用）
    options.headers = this._getHeaders(contentType, canonicalURI, payloadStr);

    const response = UrlFetchApp.fetch(url, options);
    const content = response.getContentText();
    const statusCode = response.getResponseCode();

    if (statusCode !== 200) {
      throw new Error(`API Error (${statusCode}): ${content.substring(0, 500)}`);
    }

    if (expectJson) {
      return JSON.parse(content);
    }
    return content;
  }

  /**
   * クエリ文字列を構築
   */
  _buildQueryString(params) {
    const queryParts = [];
    for (const [key, value] of Object.entries(params)) {
      if (Array.isArray(value)) {
        value.forEach(v => {
          queryParts.push(`${key}=${encodeURIComponent(v)}`);
        });
      } else {
        queryParts.push(`${key}=${encodeURIComponent(value)}`);
      }
    }
    return '?' + queryParts.join('&');
  }

  /**
   * 日付をフォーマット
   */
  _getDate(format, offset = 0, timezone = 'JST') {
    const date = new Date();
    date.setDate(date.getDate() + offset);
    return Utilities.formatDate(date, timezone, format);
  }

  /**
   * JSONを文字列に変換（LINE Ads API仕様に準拠）
   */
  _jsonStringify(o) {
    return JSON.stringify(o).replace(/([^\\][:,])/g, '$1 ');
  }

  /**
   * 認証ヘッダーを取得
   * アカウントごとのAccessKey/SecretKeyを使用して署名を生成
   */
  _getHeaders(contentType, endpoint, payload) {
    const signature = this._getSignature(contentType, endpoint, payload);
    const headers = {
      'Date': this._getDate('E, dd MMM yyyy HH:mm:ss z', 0, 'GMT'),
      'Authorization': `Bearer ${signature}`
    };
    if (contentType) {
      headers['Content-Type'] = contentType;
    }
    return headers;
  }

  /**
   * JWS署名を生成
   */
  _getSignature(contentType, endpoint, payload) {
    const accessKey = this.accessKey;
    const secretKey = this.secretKey;

    const jwsHeader = {
      'alg': 'HS256',
      'kid': accessKey,
      'typ': 'text/plain'
    };

    const hexDigest = this._sha256(payload);
    const jwsPayload = [
      hexDigest,
      contentType,
      this._getDate('yyyyMMdd', 0, 'GMT'),
      endpoint
    ].join('\n');

    const jwsInput = [
      this._base64(this._jsonStringify(jwsHeader)),
      this._base64(jwsPayload)
    ].join('.');

    const signature = [
      jwsInput,
      this._hmacSha256(secretKey, jwsInput)
    ].join('.');

    return signature;
  }

  /**
   * SHA256ハッシュ化
   */
  _sha256(input) {
    const rawHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, input, Utilities.Charset.UTF_8);
    let txtHash = '';
    for (let i = 0; i < rawHash.length; i++) {
      let hashVal = rawHash[i];
      if (hashVal < 0) {
        hashVal += 256;
      }
      if (hashVal.toString(16).length === 1) {
        txtHash += '0';
      }
      txtHash += hashVal.toString(16);
    }
    return txtHash;
  }

  /**
   * Base64エンコード
   */
  _base64(input) {
    return Utilities.base64Encode(input, Utilities.Charset.UTF_8);
  }

  /**
   * HMAC-SHA256ハッシュ化
   */
  _hmacSha256(key, text) {
    const rawHash = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, text, key);
    return Utilities.base64Encode(rawHash);
  }

  // ===========================================
  // レポート関連メソッド
  // ===========================================

  /**
   * パフォーマンスレポートを作成
   * @param {string} level - レポートレベル (AD, ADGROUP, CAMPAIGN)
   * @param {string} since - 開始日 (yyyy-MM-dd)
   * @param {string} until - 終了日 (yyyy-MM-dd)
   * @param {Object} breakdown - 分類軸オプション
   *
   * LINE Ads API breakdown オプション:
   * - time: 'DAY' | 'HOUR' | 'WEEK' | 'MONTH'
   * - attribute: 'GENDER' | 'AGE' | 'OS' | 'REGION' | 'DETAILED_REGION'
   */
  createReport(level, since, until, breakdown = { time: 'DAY' }) {
    const endpoint = `/v3/adaccounts/${this.accountId}/pfreports`;
    const payload = {
      level: level,
      since: since,
      until: until,
      breakdown: breakdown,
      filtering: {
        idType: 'ADACCOUNT',
        ids: [this.accountId]
      },
      fileFormat: 'CSV',
      includeRemove: true
    };

    const response = this.request('POST', endpoint, payload);
    return response.id;
  }

  /**
   * レポートステータスを取得
   */
  getReportStatus(reportId) {
    const endpoint = `/v3/adaccounts/${this.accountId}/pfreports`;
    const payload = { ids: [reportId] };
    const response = this.request('GET', endpoint, payload);

    if (response.datas && response.datas.length > 0) {
      return response.datas[0].status;
    }
    return 'UNKNOWN';
  }

  /**
   * レポートをダウンロード
   */
  downloadReport(reportId) {
    const endpoint = `/v3/adaccounts/${this.accountId}/pfreports/${reportId}/download`;
    const content = this.request('GET', endpoint, {}, false);
    return Utilities.parseCsv(content);
  }

  /**
   * レポートを作成してダウンロード（ポーリング付き）
   */
  createAndDownloadReport(level, since, until, breakdown = { time: 'DAY' }) {
    const reportId = this.createReport(level, since, until, breakdown);

    let status;
    let attempts = 0;

    do {
      status = this.getReportStatus(reportId);

      if (status === 'READY') {
        break;
      } else if (status === 'FAILED' || status === 'ERROR') {
        throw new Error(`Report generation failed with status: ${status}`);
      }

      attempts++;
      if (attempts >= CONFIG.REPORT_POLL_MAX_ATTEMPTS) {
        throw new Error(`Report generation timeout after ${attempts} attempts`);
      }

      Utilities.sleep(CONFIG.REPORT_POLL_INTERVAL_MS);
    } while (true);

    return this.downloadReport(reportId);
  }

  // ===========================================
  // エンティティ取得メソッド
  // ===========================================

  /**
   * キャンペーン一覧を取得
   * エンドポイント: GET /v3/adaccounts/{adAccountId}/campaigns
   */
  getCampaigns() {
    const endpoint = `/v3/adaccounts/${this.accountId}/campaigns`;
    const response = this.request('GET', endpoint, {});
    return response.campaigns || response.datas || [];
  }

  /**
   * 広告グループ一覧を取得
   * エンドポイント: GET /v3/adaccounts/{adAccountId}/adgroups
   */
  getAdGroups(campaignId = null) {
    const endpoint = `/v3/adaccounts/${this.accountId}/adgroups`;
    const params = campaignId ? { campaignId: campaignId } : {};
    const response = this.request('GET', endpoint, params);
    return response.adgroups || response.datas || [];
  }

  /**
   * メディア一覧を取得
   * エンドポイント: GET /v3/adaccounts/{adAccountId}/medias
   */
  getMedias() {
    const endpoint = `/v3/adaccounts/${this.accountId}/medias`;
    const response = this.request('GET', endpoint, {});
    return response.medias || response.datas || [];
  }
}

// ===========================================
// 1. アカウント一覧取得
// ===========================================

/**
 * アカウント一覧をシートから取得してBigQueryに出力
 */
function getAccountList() {
  log_('🚀 LINE広告アカウント一覧取得開始');

  const accounts = getTargetAccounts_();

  if (accounts.length === 0) {
    log_('⚠ 対象アカウントがありません');
    return [];
  }

  log_(`📋 登録アカウント数: ${accounts.length}`);

  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  const bqHeader = ['アカウントID', 'アカウント名', 'ステータス', '取得日時'];
  const bqData = accounts.map(acc => [
    acc.accountId,
    acc.accountName,
    'ACTIVE',
    timestamp
  ]);

  loadToBigQuery_(CONFIG.TABLES.ACCOUNT_LIST, bqHeader, bqData);

  log_(`✅ アカウント一覧取得完了: ${accounts.length}件`);

  return accounts;
}

// ===========================================
// 2. キャンペーン設定取得
// ===========================================

/**
 * 全アカウントのキャンペーン設定を取得
 */
function getCampaignSettings() {
  log_('===== 🚀 全アカウント キャンペーン設定取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      const campaigns = client.getCampaigns();

      if (campaigns.length > 0) {
        const formattedData = formatCampaignSettingsData_(campaigns, account.accountId, account.accountName);
        allData = allData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`キャンペーン設定総数: ${allData.length}件`);

  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  const bqHeader = [
    'account_id', 'account_name',
    'campaign_id', 'campaign_name', 'campaign_objective',
    'status', 'budget_type', 'budget_amount',
    'start_date', 'end_date',
    'created_date', 'updated_date', 'fetch_timestamp'
  ];

  loadToBigQuery_(CONFIG.TABLES.CAMPAIGN, bqHeader, allData.map(row => [...row, timestamp]));

  return allData;
}

/**
 * キャンペーン設定データをフォーマット
 */
function formatCampaignSettingsData_(campaigns, accountId, accountName) {
  const results = [];

  campaigns.forEach(c => {
    results.push([
      accountId,
      accountName,
      c.id || c.campaignId || '',
      c.name || c.campaignName || '',
      c.objective || c.campaignObjective || '',
      c.status || c.userStatus || '',
      c.budgetType || c.budget?.type || '',
      c.budgetAmount || c.budget?.amount || c.budget?.dailyBudget || '',
      c.startDate || c.startTime || '',
      c.endDate || c.endTime || '',
      c.createdDate || c.createdTime || '',
      c.updatedDate || c.updatedTime || ''
    ]);
  });

  return results;
}

// ===========================================
// 3. 広告グループ設定取得
// ===========================================

/**
 * 全アカウントの広告グループ設定を取得
 */
function getAdGroupSettings() {
  log_('===== 🚀 全アカウント 広告グループ設定取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      const adGroups = client.getAdGroups();

      if (adGroups.length > 0) {
        const formattedData = formatAdGroupSettingsData_(adGroups, account.accountId, account.accountName);
        allData = allData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`広告グループ設定総数: ${allData.length}件`);

  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  const bqHeader = [
    'account_id', 'account_name',
    'campaign_id', 'adgroup_id', 'adgroup_name',
    'status', 'bid_type', 'bid_amount',
    'targeting_type', 'os_type',
    'created_date', 'updated_date', 'fetch_timestamp'
  ];

  loadToBigQuery_(CONFIG.TABLES.ADGROUP, bqHeader, allData.map(row => [...row, timestamp]));

  return allData;
}

/**
 * 広告グループ設定データをフォーマット
 */
function formatAdGroupSettingsData_(adGroups, accountId, accountName) {
  const results = [];

  adGroups.forEach(ag => {
    results.push([
      accountId,
      accountName,
      ag.campaignId || '',
      ag.id || ag.adgroupId || '',
      ag.name || ag.adgroupName || '',
      ag.status || ag.userStatus || '',
      ag.bidType || ag.bid?.type || '',
      ag.bidAmount || ag.bid?.amount || '',
      ag.targetingType || ag.targeting?.type || '',
      ag.osType || ag.targeting?.os || '',
      ag.createdDate || ag.createdTime || '',
      ag.updatedDate || ag.updatedTime || ''
    ]);
  });

  return results;
}

// ===========================================
// 4. ADレポート取得
// ===========================================

/**
 * 全アカウントのADレポートを取得
 */
function fetchAllAccountsAdReport() {
  log_('===== 🚀 全アカウント ADレポート取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
  log_(`📆 対象期間: ${startStr} ～ ${endStr}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      const csvData = client.createAndDownloadReport('AD', startStr, endStr, { time: 'DAY' });

      if (csvData.length > 1) {
        const formattedData = formatAdReportData_(csvData, account.accountId, account.accountName);
        allData = allData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`ADレポート総数: ${allData.length}件`);

  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id',
    'ad_id', 'ad_name', 'ad_status', 'ad_type',
    'impressions', 'clicks', 'cost',
    // コンバージョン関連（複数取得して確認用）
    'conversions', 'total_conversions', 'conversion_value',
    'results', 'actions', 'view_conversions', 'click_conversions'
  ];

  loadToBigQuery_(CONFIG.TABLES.AD, bqHeader, allData);

  return allData;
}

/**
 * ADレポートデータをフォーマット
 */
function formatAdReportData_(csvData, accountId, accountName) {
  if (csvData.length < 2) return [];

  const header = csvData[0];
  const results = [];

  // デバッグ用: ヘッダーをログ出力
  log_(`  📋 CSVヘッダー: ${header.join(', ')}`);

  const idx = {
    DAY: findColumnIndex_(header, ['日付', 'date', 'day', 'Date']),
    CAMPAIGN_ID: findColumnIndex_(header, ['キャンペーンID', 'campaign_id', 'campaignId', 'Campaign ID', 'campaign id']),
    ADGROUP_ID: findColumnIndex_(header, ['広告グループID', 'adgroup_id', 'adgroupId', 'ad_group_id', 'Ad Group ID', 'adGroupId']),
    AD_ID: findColumnIndex_(header, ['広告ID', 'ad_id', 'adId', 'Ad ID', 'creative_id', 'creativeId']),
    AD_NAME: findColumnIndex_(header, ['広告名', 'ad_name', 'adName', 'Ad Name', 'creative_name', 'creativeName', 'クリエイティブ名']),
    AD_STATUS: findColumnIndex_(header, ['ステータス', 'status', 'ad_status', 'Status']),
    AD_TYPE: findColumnIndex_(header, ['広告タイプ', 'ad_type', 'adType', 'Ad Type', 'format', 'Format']),
    IMPRESSIONS: findColumnIndex_(header, ['インプレッション', 'impressions', 'imps', 'Impressions', 'imp']),
    CLICKS: findColumnIndex_(header, ['クリック', 'clicks', 'Clicks', 'click']),
    COST: findColumnIndex_(header, ['費用', 'cost', 'spend', 'Cost', 'Spend', '消化金額', '利用金額']),
    // コンバージョン関連フィールド（複数取得して確認）
    CONVERSIONS: findColumnIndex_(header, ['コンバージョン', 'conversions', 'cv', 'Conversions', 'CV']),
    TOTAL_CONVERSIONS: findColumnIndex_(header, ['総コンバージョン', 'total_conversions', 'totalConversions', 'Total Conversions']),
    CONVERSION_VALUE: findColumnIndex_(header, ['コンバージョン値', 'conversion_value', 'conversionValue', 'Conversion Value', 'コンバージョン金額', 'cv_value']),
    RESULTS: findColumnIndex_(header, ['成果', '結果', 'results', 'Results', 'result', 'Result']),
    ACTIONS: findColumnIndex_(header, ['アクション', 'actions', 'Actions', 'action', 'Action']),
    VIEW_CONVERSIONS: findColumnIndex_(header, ['ビュースルーコンバージョン', 'view_through_conversions', 'viewThroughConversions', 'View Through Conversions', 'vtc', 'VTC']),
    CLICK_CONVERSIONS: findColumnIndex_(header, ['クリックスルーコンバージョン', 'click_through_conversions', 'clickThroughConversions', 'Click Through Conversions', 'ctc', 'CTC'])
  };

  // デバッグ用: 見つかったインデックスをログ出力
  log_(`  🔍 カラムインデックス: CAMPAIGN_ID=${idx.CAMPAIGN_ID}, ADGROUP_ID=${idx.ADGROUP_ID}, AD_ID=${idx.AD_ID}, AD_NAME=${idx.AD_NAME}`);
  log_(`  🔍 コンバージョン系: CONVERSIONS=${idx.CONVERSIONS}, TOTAL_CONVERSIONS=${idx.TOTAL_CONVERSIONS}, CONVERSION_VALUE=${idx.CONVERSION_VALUE}, RESULTS=${idx.RESULTS}, ACTIONS=${idx.ACTIONS}, VIEW_CONVERSIONS=${idx.VIEW_CONVERSIONS}, CLICK_CONVERSIONS=${idx.CLICK_CONVERSIONS}`);

  for (let i = 1; i < csvData.length; i++) {
    const row = csvData[i];
    if (!row || row.length === 0 || !row[0]) continue;

    results.push([
      accountId,
      accountName,
      getValueSafe_(row, idx.DAY),
      getValueSafe_(row, idx.CAMPAIGN_ID),
      getValueSafe_(row, idx.ADGROUP_ID),
      getValueSafe_(row, idx.AD_ID),
      getValueSafe_(row, idx.AD_NAME),
      getValueSafe_(row, idx.AD_STATUS),
      getValueSafe_(row, idx.AD_TYPE),
      getNumberSafe_(row, idx.IMPRESSIONS),
      getNumberSafe_(row, idx.CLICKS),
      getNumberSafe_(row, idx.COST),
      // コンバージョン関連（複数取得）
      getNumberSafe_(row, idx.CONVERSIONS),
      getNumberSafe_(row, idx.TOTAL_CONVERSIONS),
      getNumberSafe_(row, idx.CONVERSION_VALUE),
      getNumberSafe_(row, idx.RESULTS),
      getNumberSafe_(row, idx.ACTIONS),
      getNumberSafe_(row, idx.VIEW_CONVERSIONS),
      getNumberSafe_(row, idx.CLICK_CONVERSIONS)
    ]);
  }

  return results;
}

// ===========================================
// 5. メディア一覧取得
// ===========================================

/**
 * 全アカウントのメディア一覧を取得
 */
function fetchAllAccountsMedia() {
  log_('===== 🚀 全アカウント メディア一覧取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  let allMediaData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      const medias = client.getMedias();

      if (medias.length > 0) {
        const formattedData = formatMediaData_(medias, account.accountId, account.accountName);
        allMediaData = allMediaData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allMediaData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`メディア総数: ${allMediaData.length}件`);

  const bqHeader = [
    'account_id', 'account_name',
    'media_id', 'media_name', 'media_type',
    'media_format', 'width', 'height', 'file_size',
    'playback_time', 'aspect_ratio',
    'review_status', 'review_result',
    'created_date', 'updated_date', 'fetch_timestamp'
  ];

  loadToBigQuery_(CONFIG.TABLES.MEDIA, bqHeader, allMediaData);

  return allMediaData;
}

/**
 * メディアデータをフォーマット
 */
function formatMediaData_(medias, accountId, accountName) {
  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');
  const results = [];

  medias.forEach(m => {
    results.push([
      accountId,
      accountName,
      m.id || m.mediaId || '',
      m.name || m.mediaName || m.title || '',
      m.type || m.mediaType || '',
      m.format || m.mediaFormat || m.mimeType || '',
      m.width || '',
      m.height || '',
      m.fileSize || m.size || '',
      m.playbackTime || m.duration || '',
      m.aspectRatio || '',
      m.reviewStatus || m.approvalStatus || '',
      m.reviewResult || '',
      m.createdDate || m.createdTime || '',
      m.updatedDate || m.updatedTime || '',
      timestamp
    ]);
  });

  return results;
}

// ===========================================
// 6. 性別レポート取得
// ===========================================

/**
 * 全アカウントの性別レポートを取得
 *
 * LINE Ads API breakdown: attribute: 'GENDER'
 */
function fetchAllAccountsGenderReport() {
  log_('===== 🚀 全アカウント 性別レポート取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
  log_(`📆 対象期間: ${startStr} ～ ${endStr}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      // GENDER breakdown を追加
      const csvData = client.createAndDownloadReport('AD', startStr, endStr, {
        time: 'DAY',
        attribute: 'GENDER'
      });

      if (csvData.length > 1) {
        const formattedData = formatGenderReportData_(csvData, account.accountId, account.accountName);
        allData = allData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`性別レポート総数: ${allData.length}件`);

  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id', 'gender',
    'impressions', 'clicks', 'conversions', 'cost',
    'ctr', 'cpc', 'cpm', 'cvr'
  ];

  loadToBigQuery_(CONFIG.TABLES.GENDER, bqHeader, allData);

  return allData;
}

/**
 * 性別レポートデータをフォーマット
 */
function formatGenderReportData_(csvData, accountId, accountName) {
  if (csvData.length < 2) return [];

  const header = csvData[0];
  const results = [];

  const idx = {
    DAY: findColumnIndex_(header, ['日付', 'date', 'day']),
    CAMPAIGN_ID: findColumnIndex_(header, ['キャンペーンID', 'campaign_id', 'campaignId']),
    ADGROUP_ID: findColumnIndex_(header, ['広告グループID', 'adgroup_id', 'adgroupId', 'ad_group_id']),
    GENDER: findColumnIndex_(header, ['性別', 'gender']),
    IMPRESSIONS: findColumnIndex_(header, ['インプレッション', 'impressions', 'imps']),
    CLICKS: findColumnIndex_(header, ['クリック', 'clicks']),
    CONVERSIONS: findColumnIndex_(header, ['コンバージョン', 'conversions', 'cv']),
    COST: findColumnIndex_(header, ['費用', 'cost', 'spend']),
    CTR: findColumnIndex_(header, ['CTR', 'ctr']),
    CPC: findColumnIndex_(header, ['CPC', 'cpc']),
    CPM: findColumnIndex_(header, ['CPM', 'cpm']),
    CVR: findColumnIndex_(header, ['CVR', 'cvr'])
  };

  for (let i = 1; i < csvData.length; i++) {
    const row = csvData[i];
    if (!row || row.length === 0 || !row[0]) continue;

    results.push([
      accountId,
      accountName,
      getValueSafe_(row, idx.DAY),
      getValueSafe_(row, idx.CAMPAIGN_ID),
      getValueSafe_(row, idx.ADGROUP_ID),
      getValueSafe_(row, idx.GENDER),
      getNumberSafe_(row, idx.IMPRESSIONS),
      getNumberSafe_(row, idx.CLICKS),
      getNumberSafe_(row, idx.CONVERSIONS),
      getNumberSafe_(row, idx.COST),
      getNumberSafe_(row, idx.CTR),
      getNumberSafe_(row, idx.CPC),
      getNumberSafe_(row, idx.CPM),
      getNumberSafe_(row, idx.CVR)
    ]);
  }

  return results;
}

// ===========================================
// 7. 年齢レポート取得
// ===========================================

/**
 * 全アカウントの年齢レポートを取得
 *
 * LINE Ads API breakdown: attribute: 'AGE'
 */
function fetchAllAccountsAgeReport() {
  log_('===== 🚀 全アカウント 年齢レポート取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
  log_(`📆 対象期間: ${startStr} ～ ${endStr}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      // AGE breakdown を追加
      const csvData = client.createAndDownloadReport('AD', startStr, endStr, {
        time: 'DAY',
        attribute: 'AGE'
      });

      if (csvData.length > 1) {
        const formattedData = formatAgeReportData_(csvData, account.accountId, account.accountName);
        allData = allData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`年齢レポート総数: ${allData.length}件`);

  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id', 'age',
    'impressions', 'clicks', 'conversions', 'cost',
    'ctr', 'cpc', 'cpm', 'cvr'
  ];

  loadToBigQuery_(CONFIG.TABLES.AGE, bqHeader, allData);

  return allData;
}

/**
 * 年齢レポートデータをフォーマット
 */
function formatAgeReportData_(csvData, accountId, accountName) {
  if (csvData.length < 2) return [];

  const header = csvData[0];
  const results = [];

  const idx = {
    DAY: findColumnIndex_(header, ['日付', 'date', 'day']),
    CAMPAIGN_ID: findColumnIndex_(header, ['キャンペーンID', 'campaign_id', 'campaignId']),
    ADGROUP_ID: findColumnIndex_(header, ['広告グループID', 'adgroup_id', 'adgroupId', 'ad_group_id']),
    AGE: findColumnIndex_(header, ['年齢', 'age']),
    IMPRESSIONS: findColumnIndex_(header, ['インプレッション', 'impressions', 'imps']),
    CLICKS: findColumnIndex_(header, ['クリック', 'clicks']),
    CONVERSIONS: findColumnIndex_(header, ['コンバージョン', 'conversions', 'cv']),
    COST: findColumnIndex_(header, ['費用', 'cost', 'spend']),
    CTR: findColumnIndex_(header, ['CTR', 'ctr']),
    CPC: findColumnIndex_(header, ['CPC', 'cpc']),
    CPM: findColumnIndex_(header, ['CPM', 'cpm']),
    CVR: findColumnIndex_(header, ['CVR', 'cvr'])
  };

  for (let i = 1; i < csvData.length; i++) {
    const row = csvData[i];
    if (!row || row.length === 0 || !row[0]) continue;

    results.push([
      accountId,
      accountName,
      getValueSafe_(row, idx.DAY),
      getValueSafe_(row, idx.CAMPAIGN_ID),
      getValueSafe_(row, idx.ADGROUP_ID),
      getValueSafe_(row, idx.AGE),
      getNumberSafe_(row, idx.IMPRESSIONS),
      getNumberSafe_(row, idx.CLICKS),
      getNumberSafe_(row, idx.CONVERSIONS),
      getNumberSafe_(row, idx.COST),
      getNumberSafe_(row, idx.CTR),
      getNumberSafe_(row, idx.CPC),
      getNumberSafe_(row, idx.CPM),
      getNumberSafe_(row, idx.CVR)
    ]);
  }

  return results;
}

// ===========================================
// 8. デバイス（OS）レポート取得
// ===========================================

/**
 * 全アカウントのデバイス（OS）レポートを取得
 *
 * LINE Ads API breakdown: attribute: 'OS'
 * ※LINE広告ではデバイス別ではなくOS別（iOS/Android）のレポートになります
 */
function fetchAllAccountsDeviceReport() {
  log_('===== 🚀 全アカウント デバイス（OS）レポート取得開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
  log_(`📆 対象期間: ${startStr} ～ ${endStr}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      // アカウントごとにクライアントを作成（認証情報が異なるため）
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      // OS breakdown を追加（LINE広告ではDEVICEの代わりにOS）
      const csvData = client.createAndDownloadReport('AD', startStr, endStr, {
        time: 'DAY',
        attribute: 'OS'
      });

      if (csvData.length > 1) {
        const formattedData = formatDeviceReportData_(csvData, account.accountId, account.accountName);
        allData = allData.concat(formattedData);
        log_(`  ✅ ${formattedData.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`デバイス（OS）レポート総数: ${allData.length}件`);

  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id', 'device_os',
    'impressions', 'clicks', 'conversions', 'cost',
    'ctr', 'cpc', 'cpm', 'cvr'
  ];

  loadToBigQuery_(CONFIG.TABLES.DEVICE, bqHeader, allData);

  return allData;
}

/**
 * デバイス（OS）レポートデータをフォーマット
 */
function formatDeviceReportData_(csvData, accountId, accountName) {
  if (csvData.length < 2) return [];

  const header = csvData[0];
  const results = [];

  const idx = {
    DAY: findColumnIndex_(header, ['日付', 'date', 'day']),
    CAMPAIGN_ID: findColumnIndex_(header, ['キャンペーンID', 'campaign_id', 'campaignId']),
    ADGROUP_ID: findColumnIndex_(header, ['広告グループID', 'adgroup_id', 'adgroupId', 'ad_group_id']),
    DEVICE_OS: findColumnIndex_(header, ['OS', 'os', 'device', 'デバイス']),
    IMPRESSIONS: findColumnIndex_(header, ['インプレッション', 'impressions', 'imps']),
    CLICKS: findColumnIndex_(header, ['クリック', 'clicks']),
    CONVERSIONS: findColumnIndex_(header, ['コンバージョン', 'conversions', 'cv']),
    COST: findColumnIndex_(header, ['費用', 'cost', 'spend']),
    CTR: findColumnIndex_(header, ['CTR', 'ctr']),
    CPC: findColumnIndex_(header, ['CPC', 'cpc']),
    CPM: findColumnIndex_(header, ['CPM', 'cpm']),
    CVR: findColumnIndex_(header, ['CVR', 'cvr'])
  };

  for (let i = 1; i < csvData.length; i++) {
    const row = csvData[i];
    if (!row || row.length === 0 || !row[0]) continue;

    results.push([
      accountId,
      accountName,
      getValueSafe_(row, idx.DAY),
      getValueSafe_(row, idx.CAMPAIGN_ID),
      getValueSafe_(row, idx.ADGROUP_ID),
      getValueSafe_(row, idx.DEVICE_OS),
      getNumberSafe_(row, idx.IMPRESSIONS),
      getNumberSafe_(row, idx.CLICKS),
      getNumberSafe_(row, idx.CONVERSIONS),
      getNumberSafe_(row, idx.COST),
      getNumberSafe_(row, idx.CTR),
      getNumberSafe_(row, idx.CPC),
      getNumberSafe_(row, idx.CPM),
      getNumberSafe_(row, idx.CVR)
    ]);
  }

  return results;
}

// ===========================================
// ユーティリティ関数
// ===========================================

/**
 * ヘッダーからカラムインデックスを検索
 */
function findColumnIndex_(header, possibleNames) {
  for (const name of possibleNames) {
    const idx = header.findIndex(h =>
      h && h.toString().toLowerCase().trim() === name.toLowerCase().trim()
    );
    if (idx !== -1) return idx;
  }
  return -1;
}

/**
 * 配列から安全に値を取得
 */
function getValueSafe_(row, idx) {
  if (idx < 0 || idx >= row.length) return '';
  const val = row[idx];
  return val === null || val === undefined ? '' : String(val).trim();
}

/**
 * 配列から安全に数値を取得
 */
function getNumberSafe_(row, idx) {
  if (idx < 0 || idx >= row.length) return 0;
  const val = row[idx];
  if (val === null || val === undefined || val === '') return 0;
  const num = Number(String(val).replace(/,/g, ''));
  return isNaN(num) ? 0 : num;
}

// ===========================================
// 一括実行関数
// ===========================================

/**
 * 全データを一括取得してBigQueryに転送
 */
function fetchAllData() {
  log_('🚀🚀🚀 LINE広告 全データ一括取得（BQ転送）開始 🚀🚀🚀');

  const startTime = new Date();

  try {
    // 1. アカウント一覧
    getAccountList();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 2. キャンペーン設定
    getCampaignSettings();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 3. 広告グループ設定
    getAdGroupSettings();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 4. ADレポート
    fetchAllAccountsAdReport();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 5. メディア一覧
    fetchAllAccountsMedia();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 6. 性別レポート
    fetchAllAccountsGenderReport();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 7. 年齢レポート
    fetchAllAccountsAgeReport();
    Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);

    // 8. デバイス（OS）レポート
    fetchAllAccountsDeviceReport();

  } catch (e) {
    log_(`❌ 致命的エラー: ${e.message}`);
    log_(e.stack);
  }

  const endTime = new Date();
  const duration = Math.round((endTime - startTime) / 1000 / 60);

  log_(`\n🎉🎉🎉 LINE広告 全データ一括取得（BQ転送）完了 🎉🎉🎉`);
  log_(`処理時間: 約${duration}分`);
}

// ===========================================
// 個別テスト用関数
// ===========================================

/**
 * 単一アカウントでADレポートをテスト取得
 */
function testSingleAccountAdReport() {
  const accounts = getTargetAccounts_();

  if (accounts.length === 0) {
    log_('❌ テスト対象のアカウントがありません');
    return;
  }

  const account = accounts[0];
  log_(`===== テスト: ${account.accountId} (${account.accountName}) =====`);

  const { startStr, endStr } = getDateRange_(7, false);
  log_(`📆 対象期間: ${startStr} ～ ${endStr}`);

  try {
    const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
    const csvData = client.createAndDownloadReport('AD', startStr, endStr, { time: 'DAY' });

    log_(`✅ 取得行数: ${csvData.length}`);

    if (csvData.length > 0) {
      log_('--- ヘッダー ---');
      log_(csvData[0].join(', '));

      if (csvData.length > 1) {
        log_('--- 最初のデータ行 ---');
        log_(csvData[1].join(', '));
      }
    }

  } catch (e) {
    log_(`❌ エラー: ${e.message}`);
    log_(e.stack);
  }
}

/**
 * アカウント認証テスト
 */
function testAccountAuth() {
  const accounts = getTargetAccounts_();

  if (accounts.length === 0) {
    log_('❌ テスト対象のアカウントがありません');
    return;
  }

  log_(`===== アカウント認証テスト (${accounts.length}件) =====`);

  accounts.forEach((account, i) => {
    log_(`\n[${i + 1}] ${account.accountId} (${account.accountName})`);

    try {
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      // キャンペーン一覧取得でテスト
      const campaigns = client.getCampaigns();
      log_(`  ✅ 認証成功 - キャンペーン数: ${campaigns.length}`);
    } catch (e) {
      log_(`  ❌ 認証失敗: ${e.message}`);
    }
  });
}

/**
 * メディア一覧テスト
 */
function testMediaList() {
  const accounts = getTargetAccounts_();

  if (accounts.length === 0) {
    log_('❌ テスト対象のアカウントがありません');
    return;
  }

  const account = accounts[0];
  log_(`===== メディア一覧テスト: ${account.accountId} (${account.accountName}) =====`);

  try {
    const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
    const medias = client.getMedias();

    log_(`✅ 取得件数: ${medias.length}`);

    if (medias.length > 0) {
      log_('--- 最初のメディア ---');
      log_(JSON.stringify(medias[0], null, 2));
    }

  } catch (e) {
    log_(`❌ エラー: ${e.message}`);
    log_(e.stack);
  }
}

// ===========================================
// スプレッドシート初期設定
// ===========================================

/**
 * 必要なシートを作成する初期設定関数
 */
function setupSpreadsheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // LINE広告アカウント一覧シート
  let accountSheet = ss.getSheetByName('LINE広告アカウント一覧');
  if (!accountSheet) {
    accountSheet = ss.insertSheet('LINE広告アカウント一覧');
    accountSheet.getRange(1, 1, 1, 4).setValues([
      ['アカウントID', 'アカウント名', 'AccessKey', 'SecretKey']
    ]).setFontWeight('bold');
    accountSheet.setColumnWidth(1, 150);
    accountSheet.setColumnWidth(2, 200);
    accountSheet.setColumnWidth(3, 250);
    accountSheet.setColumnWidth(4, 350);

    // サンプル行を追加
    accountSheet.getRange(2, 1, 1, 4).setValues([
      ['A12345678901', 'サンプルアカウント', 'your-access-key', 'your-secret-key']
    ]).setFontColor('#999999');

    log_('📊 LINE広告アカウント一覧シートを作成しました');
  }

  // ログシート
  let logSheet = ss.getSheetByName('ログ');
  if (!logSheet) {
    logSheet = ss.insertSheet('ログ');
    logSheet.getRange(1, 1, 1, 2).setValues([
      ['日時', 'メッセージ']
    ]).setFontWeight('bold');
    logSheet.setColumnWidth(1, 180);
    logSheet.setColumnWidth(2, 800);
    log_('📊 ログシートを作成しました');
  }

  log_('✅ スプレッドシートの初期設定が完了しました');
  log_('📝 「LINE広告アカウント一覧」シートにアカウント情報を入力してください');
  log_('');
  log_('【シート形式】');
  log_('  A列: アカウントID');
  log_('  B列: アカウント名');
  log_('  C列: AccessKey');
  log_('  D列: SecretKey');
  log_('');
  log_('※各アカウントのAccessKey/SecretKeyは広告マネージャーの「グループ設定」から取得できます');
}

// ===========================================
// スプレッドシート出力用関数
// ===========================================

/**
 * メイン関数 - ADレポートをスプレッドシートに出力
 *
 * 「シート1」にレポートデータを書き出します。
 * 取得フィールド:
 * - Day
 * - Ad account ID
 * - Campaign objective
 * - Ad group ID
 * - Ad name
 * - Ad ID
 * - Title
 * - Description
 * - Impressions
 * - Clicks
 * - Cost
 * - Currency
 * - CV (purchased) (ALL)
 */
function main() {
  const SHEET_NAME = 'シート1';

  log_('===== ADレポート → スプレッドシート出力開始 =====');

  const accounts = getTargetAccounts_();
  if (accounts.length === 0) {
    log_('対象アカウントがありません');
    return;
  }

  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
  log_(`対象期間: ${startStr} ~ ${endStr}`);

  // ヘッダー行
  const header = [
    'Day',
    'Ad account ID',
    'Campaign objective',
    'Ad group ID',
    'Ad name',
    'Ad ID',
    'Title',
    'Description',
    'Impressions',
    'Clicks',
    'Cost',
    'Currency',
    'CV (purchased) (ALL)'
  ];

  let allData = [header];

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`[${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    try {
      const client = new LineAdsClient(account.accountId, account.accessKey, account.secretKey);
      const csvData = client.createAndDownloadReport('AD', startStr, endStr, { time: 'DAY' });

      if (csvData.length > 1) {
        const formattedData = formatAdReportForSheet_(csvData, account.accountId);
        allData = allData.concat(formattedData);
        log_(`  ${formattedData.length}件取得`);
      } else {
        log_(`  データなし`);
      }

    } catch (e) {
      log_(`  エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(CONFIG.ACCOUNT_WAIT_MS);
    }
  }

  // スプレッドシートに書き出し
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
  }

  sheet.clearContents();

  const len = allData.length;
  if (len > 0) {
    sheet.getRange(1, 1, len, allData[0].length).setValues(allData);
    log_(`スプレッドシート「${SHEET_NAME}」に${len - 1}件のデータを出力しました`);
  } else {
    log_('出力するデータがありませんでした');
  }

  log_('===== ADレポート → スプレッドシート出力完了 =====');
}

/**
 * ADレポートデータをスプレッドシート出力用にフォーマット
 *
 * 出力フィールド:
 * - Day
 * - Ad account ID
 * - Campaign objective
 * - Ad group ID
 * - Ad name
 * - Ad ID
 * - Title
 * - Description
 * - Impressions
 * - Clicks
 * - Cost
 * - Currency
 * - CV (purchased) (ALL)
 */
function formatAdReportForSheet_(csvData, accountId) {
  if (csvData.length < 2) return [];

  const header = csvData[0];
  const results = [];

  // デバッグ用: ヘッダーをログ出力
  log_(`  CSVヘッダー: ${header.join(', ')}`);

  const idx = {
    DAY: findColumnIndex_(header, ['Day', '日付', 'date', 'day', 'Date']),
    AD_ACCOUNT_ID: findColumnIndex_(header, ['Ad account ID', 'アカウントID', 'account_id', 'adAccountId', 'adaccount_id']),
    CAMPAIGN_OBJECTIVE: findColumnIndex_(header, ['Campaign objective', 'キャンペーン目的', 'campaign_objective', 'campaignObjective', 'objective']),
    ADGROUP_ID: findColumnIndex_(header, ['Ad group ID', '広告グループID', 'adgroup_id', 'adgroupId', 'ad_group_id', 'adGroupId']),
    AD_NAME: findColumnIndex_(header, ['Ad name', '広告名', 'ad_name', 'adName', 'creative_name', 'creativeName']),
    AD_ID: findColumnIndex_(header, ['Ad ID', '広告ID', 'ad_id', 'adId', 'creative_id', 'creativeId']),
    TITLE: findColumnIndex_(header, ['Title', 'タイトル', 'title', 'headline']),
    DESCRIPTION: findColumnIndex_(header, ['Description', '説明文', 'description', 'desc', 'body']),
    IMPRESSIONS: findColumnIndex_(header, ['Impressions', 'インプレッション', 'impressions', 'imps', 'imp']),
    CLICKS: findColumnIndex_(header, ['Clicks', 'クリック', 'clicks', 'click']),
    COST: findColumnIndex_(header, ['Cost', '費用', 'cost', 'spend', '消化金額', '利用金額']),
    CURRENCY: findColumnIndex_(header, ['Currency', '通貨', 'currency']),
    CV_PURCHASED: findColumnIndex_(header, ['CV (purchased) (ALL)', 'コンバージョン（購入）', 'cv_purchased', 'conversions_purchase', 'purchase_conversions', 'CV(購入)', 'コンバージョン', 'conversions', 'cv', 'CV'])
  };

  // デバッグ用: 見つかったインデックスをログ出力
  log_(`  カラムインデックス: DAY=${idx.DAY}, AD_ACCOUNT_ID=${idx.AD_ACCOUNT_ID}, CAMPAIGN_OBJECTIVE=${idx.CAMPAIGN_OBJECTIVE}`);
  log_(`  ADGROUP_ID=${idx.ADGROUP_ID}, AD_NAME=${idx.AD_NAME}, AD_ID=${idx.AD_ID}`);
  log_(`  TITLE=${idx.TITLE}, DESCRIPTION=${idx.DESCRIPTION}, CV_PURCHASED=${idx.CV_PURCHASED}`);

  for (let i = 1; i < csvData.length; i++) {
    const row = csvData[i];
    if (!row || row.length === 0 || !row[0]) continue;

    // Ad account IDはCSVにあれば使用、なければ引数のaccountIdを使用
    const adAccountId = idx.AD_ACCOUNT_ID >= 0 ? getValueSafe_(row, idx.AD_ACCOUNT_ID) : accountId;

    results.push([
      getValueSafe_(row, idx.DAY),
      adAccountId,
      getValueSafe_(row, idx.CAMPAIGN_OBJECTIVE),
      getValueSafe_(row, idx.ADGROUP_ID),
      getValueSafe_(row, idx.AD_NAME),
      getValueSafe_(row, idx.AD_ID),
      getValueSafe_(row, idx.TITLE),
      getValueSafe_(row, idx.DESCRIPTION),
      getNumberSafe_(row, idx.IMPRESSIONS),
      getNumberSafe_(row, idx.CLICKS),
      getNumberSafe_(row, idx.COST),
      getValueSafe_(row, idx.CURRENCY),
      getNumberSafe_(row, idx.CV_PURCHASED)
    ]);
  }

  return results;
}

/**
 * 単一アカウント用のシンプルなスプレッドシート出力
 *
 * アカウント情報をコードに直接記述する場合に使用します。
 * スプレッドシートでアカウント管理しない場合向け。
 */
function exportSingleAccountToSheet() {
  const SHEET_NAME = 'シート1';

  // 単一アカウント設定（必要に応じて変更）
  const accountId = 'A55356342538';
  const accessKey = 'yK0nvZ2bbKccFykx';
  const secretKey = 'MiyccBCYHpz5QksbQvKpBu2e2lnRfoO5';

  log_('===== 単一アカウント ADレポート → スプレッドシート出力開始 =====');

  const { startStr, endStr } = getDateRange_(80, false);
  log_(`対象期間: ${startStr} ~ ${endStr}`);

  const header = [
    'Day',
    'Ad account ID',
    'Campaign objective',
    'Ad group ID',
    'Ad name',
    'Ad ID',
    'Title',
    'Description',
    'Impressions',
    'Clicks',
    'Cost',
    'Currency',
    'CV (purchased) (ALL)'
  ];

  let allData = [header];

  try {
    const client = new LineAdsClient(accountId, accessKey, secretKey);
    const csvData = client.createAndDownloadReport('AD', startStr, endStr, { time: 'DAY' });

    if (csvData.length > 1) {
      const formattedData = formatAdReportForSheet_(csvData, accountId);
      allData = allData.concat(formattedData);
      log_(`${formattedData.length}件取得`);
    } else {
      log_('データなし');
    }

  } catch (e) {
    log_(`エラー: ${e.message}`);
    log_(e.stack);
    return;
  }

  // スプレッドシートに書き出し
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);

  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
  }

  sheet.clearContents();

  const len = allData.length;
  if (len > 0) {
    sheet.getRange(1, 1, len, allData[0].length).setValues(allData);
    log_(`スプレッドシート「${SHEET_NAME}」に${len - 1}件のデータを出力しました`);
  }

  log_('===== 単一アカウント ADレポート → スプレッドシート出力完了 =====');
}

// ===========================================
// メニュー追加
// ===========================================

/**
 * スプレッドシートを開いたときにカスタムメニューを追加
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('LINE広告データ取得')
    .addItem('初期設定（シート作成）', 'setupSpreadsheet')
    .addSeparator()
    .addSubMenu(ui.createMenu('スプレッドシート出力')
      .addItem('ADレポート → シート1', 'main')
      .addItem('単一アカウント → シート1', 'exportSingleAccountToSheet'))
    .addSeparator()
    .addItem('全データ一括取得（BQ）', 'fetchAllData')
    .addSeparator()
    .addSubMenu(ui.createMenu('設定・マスタ取得')
      .addItem('アカウント一覧', 'getAccountList')
      .addItem('キャンペーン設定', 'getCampaignSettings')
      .addItem('広告グループ設定', 'getAdGroupSettings')
      .addItem('メディア一覧', 'fetchAllAccountsMedia'))
    .addSubMenu(ui.createMenu('レポート取得')
      .addItem('ADレポート', 'fetchAllAccountsAdReport')
      .addItem('性別レポート', 'fetchAllAccountsGenderReport')
      .addItem('年齢レポート', 'fetchAllAccountsAgeReport')
      .addItem('デバイス（OS）レポート', 'fetchAllAccountsDeviceReport'))
    .addSeparator()
    .addSubMenu(ui.createMenu('テスト')
      .addItem('単一アカウントADレポート', 'testSingleAccountAdReport')
      .addItem('アカウント認証', 'testAccountAuth')
      .addItem('メディア一覧', 'testMediaList'))
    .addToUi();
}
