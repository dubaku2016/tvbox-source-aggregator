// Hono 统一路由层

import { Hono } from 'hono';
import type { Storage } from './storage/interface';
import type { AppConfig, SourceEntry, MacCMSSourceEntry, LiveSourceEntry, NameTransformConfig } from './core/types';
import { KV_MERGED_CONFIG, KV_MERGED_CONFIG_FULL, KV_MANUAL_SOURCES, KV_LAST_UPDATE, KV_MACCMS_SOURCES, KV_LIVE_SOURCES, KV_BLACKLIST, LIVE_PROXY_TTL, IMG_PROXY_TTL, KV_INLINE_PREFIX, KV_NAME_TRANSFORM, KV_CRON_INTERVAL, DEFAULT_CRON_INTERVAL, KV_SOURCE_HEALTH, KV_SPEED_TEST_ENABLED, KV_JAR_REGISTRY_ENABLED, KV_JAR_REGISTRY, KV_JAR_BIN_PREFIX } from './core/config';
import { parseConfigJson, isMultiRepoConfig, extractMultiRepoEntries } from './core/fetcher';
import { decodeConfigResponse } from './core/decoder';
import { validateMacCMS } from './core/maccms';
import { lookupJarUrl, isMd5Key, base64ToUint8Array } from './core/jar-proxy';
import { loadJarRegistry, buildJarRegistry, assignJars, buildJarStorageAdapter } from './core/jar-registry';
import { lookupLiveUrl } from './core/live-source';
import { adminHtml } from './core/admin';
import { dashboardHtml } from './core/dashboard';
import { configEditorHtml } from './core/config-editor';
import { siteFingerprint, loadBlacklist, saveBlacklist } from './core/blacklist';
import type { TVBoxConfig } from './core/types';

export interface AppDeps {
  storage: Storage;
  config: AppConfig;
  triggerRefresh: () => Promise<void>;
  onCronIntervalChange?: (intervalMinutes: number) => void;
}

export function createApp(deps: AppDeps): Hono {
  const app = new Hono();
  const { storage, config } = deps;

  // ─── 主配置 ────────────────────────────────────────────
  app.get('/', async (c) => {
    const cached = await storage.get(KV_MERGED_CONFIG);

    if (!cached) {
      return c.json(
        { error: 'No config available yet. Add sources in /admin and trigger a refresh.' },
        503,
      );
    }

    return c.body(cached, 200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'public, max-age=1800',
      'Access-Control-Allow-Origin': '*',
    });
  });

  // ─── 纯直播配置 ────────────────────────────────────────
  app.get('/live-config', async (c) => {
    const cached = await storage.get(KV_MERGED_CONFIG);

    if (!cached) {
      return c.json({ error: 'No config available yet.' }, 503);
    }

    try {
      const full = JSON.parse(cached);
      const liveConfig = { lives: full.lives || [] };
      return c.body(JSON.stringify(liveConfig), 200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'public, max-age=1800',
        'Access-Control-Allow-Origin': '*',
      });
    } catch {
      return c.json({ error: 'Config parse error' }, 500);
    }
  });

  // ─── 监控面板 ──────────────────────────────────────────
  app.get('/status', (c) => {
    return c.html(dashboardHtml);
  });

  app.get('/status-data', async (c) => {
    const lastUpdate = await storage.get(KV_LAST_UPDATE);
    const sources = await storage.get(KV_MANUAL_SOURCES);
    const macCMSSources = await storage.get(KV_MACCMS_SOURCES);
    const liveSources = await storage.get(KV_LIVE_SOURCES);
    const cached = await storage.get(KV_MERGED_CONFIG);

    let siteCount = 0;
    let parseCount = 0;
    let liveCount = 0;
    if (cached) {
      try {
        const parsed = JSON.parse(cached);
        siteCount = parsed.sites?.length || 0;
        parseCount = parsed.parses?.length || 0;
        liveCount = parsed.lives?.length || 0;
      } catch {
        // ignore
      }
    }

    return c.json({
      lastUpdate: lastUpdate || 'never',
      sourceCount: sources ? JSON.parse(sources).length : 0,
      macCMSCount: macCMSSources ? JSON.parse(macCMSSources).length : 0,
      liveSourceCount: liveSources ? JSON.parse(liveSources).length : 0,
      sites: siteCount,
      parses: parseCount,
      lives: liveCount,
    });
  });

  // ─── 源健康状态（无认证，Dashboard 需要访问）─────────────
  app.get('/source-status', async (c) => {
    const raw = await storage.get(KV_SOURCE_HEALTH);
    const records = raw ? JSON.parse(raw) : [];
    return c.json(records);
  });

  // ─── Admin 页面 ────────────────────────────────────────
  app.get('/admin', (c) => {
    return c.html(adminHtml);
  });

  // ─── Admin API（需鉴权）────────────────────────────────
  app.get('/admin/sources', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    const raw = await storage.get(KV_MANUAL_SOURCES);
    const sources: SourceEntry[] = raw ? JSON.parse(raw) : [];
    return c.json(sources);
  });

  app.post('/admin/sources', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { name?: string; url?: string; configKey?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    let url = body.url?.trim() || '';
    if (!url) return c.json({ error: 'URL is required' }, 400);

    // 自动提取 ;pk; 密钥
    let configKey = body.configKey?.trim() || '';
    const pkMatch = url.match(/;pk;(.+)$/);
    if (pkMatch) {
      configKey = configKey || pkMatch[1];
      url = url.replace(/;pk;.+$/, '');
    }

    try {
      new URL(url);
    } catch {
      return c.json({ error: 'Invalid URL format' }, 400);
    }

    const name = body.name?.trim() || '';
    const raw = await storage.get(KV_MANUAL_SOURCES);
    const sources: SourceEntry[] = raw ? JSON.parse(raw) : [];

    if (sources.some((s) => s.url === url)) {
      return c.json({ error: 'Source already exists' }, 409);
    }

    const entry: SourceEntry = { name, url };
    if (configKey) entry.configKey = configKey;
    sources.push(entry);
    await storage.put(KV_MANUAL_SOURCES, JSON.stringify(sources));

    return c.json({ success: true });
  });

  app.delete('/admin/sources', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { url?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const url = body.url?.trim();
    if (!url) return c.json({ error: 'URL is required' }, 400);

    const raw = await storage.get(KV_MANUAL_SOURCES);
    const sources: SourceEntry[] = raw ? JSON.parse(raw) : [];
    const filtered = sources.filter((s) => s.url !== url);
    await storage.put(KV_MANUAL_SOURCES, JSON.stringify(filtered));

    return c.json({ success: true });
  });

  // ─── JSON 导入 ─────────────────────────────────────────
  app.post('/admin/sources/import', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { input?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const input = body.input?.trim();
    if (!input) return c.json({ error: 'input is required' }, 400);

    // 判断是 URL 还是 JSON 内容
    const isUrl = /^https?:\/\//i.test(input);
    let jsonText: string;
    let sourceUrl: string | null = null;

    // 自动提取 ;pk; 密钥
    let configKey: string | undefined;
    let fetchUrl = input;
    if (isUrl) {
      const pkMatch = input.match(/;pk;(.+)$/);
      if (pkMatch) {
        configKey = pkMatch[1];
        fetchUrl = input.replace(/;pk;.+$/, '');
      }
      sourceUrl = fetchUrl;
      try {
        const resp = await fetch(fetchUrl, {
          headers: { 'Accept': 'application/json, text/plain, */*', 'User-Agent': 'okhttp/3.12.0' },
        });
        if (!resp.ok) return c.json({ error: `Fetch failed: HTTP ${resp.status}` }, 502);
        const buffer = await resp.arrayBuffer();
        const decoded = await decodeConfigResponse(buffer, configKey);
        jsonText = decoded || '';
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        return c.json({ error: `Fetch failed: ${msg}` }, 502);
      }
    } else {
      jsonText = input;
    }

    const parsed = parseConfigJson(jsonText);
    if (!parsed) return c.json({ error: 'Failed to parse JSON' }, 400);

    // 读取现有源
    const raw = await storage.get(KV_MANUAL_SOURCES);
    const sources: SourceEntry[] = raw ? JSON.parse(raw) : [];
    const existingUrls = new Set(sources.map(s => s.url));

    let added = 0;
    let duplicates = 0;
    const addedSources: string[] = [];

    if (isMultiRepoConfig(parsed)) {
      // 多仓：提取子 URL 批量添加
      const entries = extractMultiRepoEntries(parsed, 'Imported');
      for (const entry of entries) {
        if (existingUrls.has(entry.url)) {
          duplicates++;
        } else {
          sources.push(entry);
          existingUrls.add(entry.url);
          addedSources.push(entry.url);
          added++;
        }
      }
      await storage.put(KV_MANUAL_SOURCES, JSON.stringify(sources));
      return c.json({ type: 'multi', added, duplicates, sources: addedSources });
    } else {
      // 单仓
      if (sourceUrl) {
        // 来自 URL：直接添加
        if (existingUrls.has(sourceUrl)) {
          return c.json({ type: 'single', added: 0, duplicates: 1, sources: [] });
        }
        const entry: SourceEntry = { name: 'Imported', url: sourceUrl };
        if (configKey) entry.configKey = configKey;
        sources.push(entry);
        await storage.put(KV_MANUAL_SOURCES, JSON.stringify(sources));
        return c.json({ type: 'single', added: 1, duplicates: 0, sources: [sourceUrl] });
      } else {
        // 粘贴的内容：存 KV 用 inline:// 引用
        const key = `${KV_INLINE_PREFIX}${Date.now()}`;
        await storage.put(key, jsonText);
        const inlineUrl = `inline://${key}`;
        sources.push({ name: 'Inline Config', url: inlineUrl });
        await storage.put(KV_MANUAL_SOURCES, JSON.stringify(sources));
        return c.json({ type: 'single', added: 1, duplicates: 0, sources: [inlineUrl] });
      }
    }
  });

  // ─── 名称定制 API ──────────────────────────────────────
  app.get('/admin/name-transform', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    const raw = await storage.get(KV_NAME_TRANSFORM);
    const transform: NameTransformConfig = raw ? JSON.parse(raw) : {};
    return c.json(transform);
  });

  app.put('/admin/name-transform', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: NameTransformConfig;
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    // 验证额外正则语法
    if (body.extraCleanPatterns) {
      for (const p of body.extraCleanPatterns) {
        try { new RegExp(p); } catch {
          return c.json({ error: `Invalid regex: ${p}` }, 400);
        }
      }
    }

    const transform: NameTransformConfig = {
      prefix: body.prefix || undefined,
      suffix: body.suffix || undefined,
      promoReplacement: body.promoReplacement || undefined,
      extraCleanPatterns: body.extraCleanPatterns?.length ? body.extraCleanPatterns : undefined,
    };

    await storage.put(KV_NAME_TRANSFORM, JSON.stringify(transform));
    return c.json({ success: true });
  });

  // ─── 定时任务间隔 API ──────────────────────────────────
  app.get('/admin/cron-interval', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    const raw = await storage.get(KV_CRON_INTERVAL);
    const interval = raw ? parseInt(raw) : DEFAULT_CRON_INTERVAL;
    return c.json({ interval });
  });

  app.put('/admin/cron-interval', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { interval?: number };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const interval = body.interval;
    const validIntervals = [60, 180, 360, 720, 1440];
    if (!interval || !validIntervals.includes(interval)) {
      return c.json({ error: `interval must be one of: ${validIntervals.join(', ')}` }, 400);
    }

    await storage.put(KV_CRON_INTERVAL, String(interval));

    if (deps.onCronIntervalChange) {
      deps.onCronIntervalChange(interval);
    }

    return c.json({ success: true, interval });
  });

  // ─── 站点测速开关 ──────────────────────────────────────
  app.get('/admin/speed-test', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    const raw = await storage.get(KV_SPEED_TEST_ENABLED);
    return c.json({ enabled: raw !== 'false' });
  });

  app.put('/admin/speed-test', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { enabled?: boolean };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    if (typeof body.enabled !== 'boolean') {
      return c.json({ error: 'enabled must be a boolean' }, 400);
    }

    await storage.put(KV_SPEED_TEST_ENABLED, String(body.enabled));
    return c.json({ success: true, enabled: body.enabled });
  });

  // ─── JAR 仓库 Admin API ──────────────────────────────
  app.get('/admin/jar-registry/enabled', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) return c.json({ error: 'Unauthorized' }, 401);
    const raw = await storage.get(KV_JAR_REGISTRY_ENABLED);
    return c.json({ enabled: raw === 'true' });
  });

  app.put('/admin/jar-registry/enabled', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) return c.json({ error: 'Unauthorized' }, 401);
    let body: { enabled?: boolean };
    try { body = await c.req.json(); } catch { return c.json({ error: 'Invalid JSON' }, 400); }
    if (typeof body.enabled !== 'boolean') return c.json({ error: 'enabled must be a boolean' }, 400);
    await storage.put(KV_JAR_REGISTRY_ENABLED, String(body.enabled));
    return c.json({ success: true, enabled: body.enabled });
  });

  app.get('/admin/jar-registry', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) return c.json({ error: 'Unauthorized' }, 401);
    const registry = await loadJarRegistry(storage);
    return c.json(registry || { version: 1, updatedAt: null, jars: {} });
  });

  app.get('/admin/jar-registry/report', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) return c.json({ error: 'Unauthorized' }, 401);
    const registry = await loadJarRegistry(storage);
    if (!registry) return c.json({ error: 'No JAR registry. Enable and refresh first.' }, 404);

    const configRaw = await storage.get(KV_MERGED_CONFIG_FULL);
    if (!configRaw) return c.json({ error: 'No merged config available' }, 404);

    // 构造 SourcedConfig[] 用于 assignJars
    const mergedConfig: TVBoxConfig = JSON.parse(configRaw);
    const fakeSourced = [{ sourceUrl: 'merged', sourceName: 'merged', config: mergedConfig }];
    const assignment = assignJars(registry, fakeSourced);

    const jarSummaries = Object.values(registry.jars).map((j) => ({
      url: j.url,
      status: j.status,
      classCount: j.classes.length,
      sizeBytes: j.sizeBytes,
      lastFetchedAt: j.lastFetchedAt,
      errorMessage: j.errorMessage,
      isGlobal: j.url === assignment.globalSpiderUrl,
    }));

    const orphanedList = Array.from(assignment.orphanedKeys).map((dk) => {
      const [key, api] = dk.split('|');
      return { key, api, neededClass: api?.startsWith('csp_') ? api.substring(4) : api };
    });

    return c.json({
      globalSpider: assignment.globalSpiderUrl,
      stats: assignment.stats,
      jars: jarSummaries.sort((a, b) => b.classCount - a.classCount),
      orphanedSites: orphanedList,
    });
  });

  app.post('/admin/jar-registry/refresh', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) return c.json({ error: 'Unauthorized' }, 401);

    try {
    let body: { reset?: boolean } = {};
    try { body = await c.req.json(); } catch { /* no body is fine */ }

    const configRaw = await storage.get(KV_MERGED_CONFIG_FULL);
    if (!configRaw) return c.json({ error: 'No merged config. Run aggregation first.' }, 404);

    const mergedConfig: TVBoxConfig = JSON.parse(configRaw);
    const fakeSourced = [{ sourceUrl: 'merged', sourceName: 'merged', config: mergedConfig }];

    // reset=true 时清除旧 registry 强制全部重下载
    if (body.reset) {
      await storage.put(KV_JAR_REGISTRY, '');
    }

    // refresh 只解析类名，不存 JAR 二进制（省 CPU，二进制存储由聚合流程处理）
    const BATCH_SIZE = 1; // CF 免费版每次只处理 1 个 JAR（DEFLATE 解压吃 CPU）
    const result = await buildJarRegistry(fakeSourced, storage, config.fetchTimeoutMs, undefined, {
      skipColdStartCheck: true,
      batchLimit: BATCH_SIZE,
    });
    if (!result) return c.json({ error: 'No JARs found in config' }, 404);

    const { registry, processed, remaining, total } = result;
    const okCount = Object.values(registry.jars).filter((j) => j.status === 'ok').length;
    const totalClasses = Object.values(registry.jars).reduce((s, j) => s + j.classes.length, 0);

    return c.json({
      success: true,
      jarCount: Object.keys(registry.jars).length,
      okCount,
      totalClasses,
      processed,
      remaining,
      total,
      done: remaining === 0,
    });
    } catch (err: unknown) {
      const msg = err instanceof Error ? `${err.message}\n${err.stack}` : String(err);
      return c.json({ error: msg }, 500);
    }
  });

  // ─── MacCMS 边缘代理（仅 CF 版）──────────────────────
  if (config.workerBaseUrl) {
    app.all('/api/:key', async (c) => {
      const key = c.req.param('key');
      const raw = await storage.get(KV_MACCMS_SOURCES);
      const sources: MacCMSSourceEntry[] = raw ? JSON.parse(raw) : [];
      const source = sources.find((s) => s.key === key);

      if (!source) {
        return c.json({ error: 'Unknown MacCMS source' }, 404);
      }

      try {
        const targetUrl = new URL(source.api);
        const reqUrl = new URL(c.req.url);
        reqUrl.searchParams.forEach((v, k) => targetUrl.searchParams.set(k, v));

        const resp = await fetch(targetUrl.toString());
        const data = await resp.json();

        return c.json(data, 200, {
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=300',
        });
      } catch (error: unknown) {
        const msg = error instanceof Error ? error.message : String(error);
        return c.json({ error: msg }, 502);
      }
    });
  }

  // ─── JAR 代理（仅 CF 版）─────────────────────────────
  if (config.workerBaseUrl) {
    app.get('/jar/:key', async (c) => {
      const key = c.req.param('key');

      // 1. 查 CF Cache
      const cache = (caches as any).default as Cache;
      const cacheKey = new Request(c.req.url);
      const cached = await cache.match(cacheKey);
      if (cached) return cached;

      // 2. 查 KV 拿原始 URL
      const originalUrl = await lookupJarUrl(key, storage);
      if (!originalUrl) {
        return c.json({ error: 'Unknown JAR key' }, 404);
      }

      // 3. 流式透传
      const ttl = isMd5Key(key) ? 86400 : 21600; // MD5 key → 24h, URL hash → 6h
      try {
        const resp = await fetch(originalUrl, {
          headers: { 'User-Agent': 'okhttp/3.12.0' },
        });

        if (resp.ok) {
          const response = new Response(resp.body, {
            headers: {
              'Content-Type': 'application/octet-stream',
              'Cache-Control': `public, max-age=${ttl}`,
              'Access-Control-Allow-Origin': '*',
            },
          });
          c.executionCtx.waitUntil(cache.put(cacheKey, response.clone()));
          return response;
        }

        // Origin 失败 → 降级到 KV 二进制缓存
        console.log(`[jar-proxy] Origin returned ${resp.status} for ${key}, trying KV binary cache`);
      } catch (error: unknown) {
        console.log(`[jar-proxy] Origin fetch error for ${key}: ${error instanceof Error ? error.message : error}`);
      }

      // 4. 降级：从 KV 读取 base64 编码的 JAR 二进制
      const binBase64 = await storage.get(KV_JAR_BIN_PREFIX + key);
      if (binBase64) {
        console.log(`[jar-proxy] Serving ${key} from KV binary cache`);
        const binary = base64ToUint8Array(binBase64);
        const response = new Response(binary, {
          headers: {
            'Content-Type': 'application/octet-stream',
            'Cache-Control': `public, max-age=${ttl}`,
            'Access-Control-Allow-Origin': '*',
          },
        });
        c.executionCtx.waitUntil(cache.put(cacheKey, response.clone()));
        return response;
      }

      return c.json({ error: 'JAR unavailable from origin and no binary cache' }, 502);
    });
  }

  // ─── 直播源代理（仅 CF 版）──────────────────────────────
  if (config.workerBaseUrl) {
    app.get('/live/:key', async (c) => {
      const key = c.req.param('key');

      // 1. 查 CF Cache
      const cache = (caches as any).default as Cache;
      const cacheKey = new Request(c.req.url);
      const cached = await cache.match(cacheKey);
      if (cached) return cached;

      // 2. 查 KV 拿原始 URL
      const originalUrl = await lookupLiveUrl(key, storage);
      if (!originalUrl) {
        return c.json({ error: 'Unknown live source key' }, 404);
      }

      // 3. 流式透传
      try {
        const resp = await fetch(originalUrl, {
          headers: { 'User-Agent': 'okhttp/3.12.0' },
        });

        if (!resp.ok) {
          return c.json({ error: `Origin returned ${resp.status}` }, 502);
        }

        // 4. 构建响应 + 异步写缓存
        const response = new Response(resp.body, {
          headers: {
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': `public, max-age=${LIVE_PROXY_TTL}`,
            'Access-Control-Allow-Origin': '*',
          },
        });

        c.executionCtx.waitUntil(cache.put(cacheKey, response.clone()));
        return response;
      } catch (error: unknown) {
        const msg = error instanceof Error ? error.message : String(error);
        return c.json({ error: msg }, 502);
      }
    });
  }

  // ─── 图片代理（仅 CF 版）──────────────────────────────
  if (config.workerBaseUrl) {
    app.get('/img/*', async (c) => {
      // 从完整 URL 中提取原始图片地址（/img/ 之后的所有内容，含 query string）
      const fullUrl = c.req.url;
      const marker = '/img/';
      const markerIdx = fullUrl.indexOf(marker);
      const originalUrl = fullUrl.substring(markerIdx + marker.length);

      if (!originalUrl.startsWith('http://') && !originalUrl.startsWith('https://')) {
        return c.json({ error: 'Invalid image URL' }, 400);
      }

      // 1. 查 CF Cache
      const cache = (caches as any).default as Cache;
      const cacheKey = new Request(c.req.url);
      const cached = await cache.match(cacheKey);
      if (cached) return cached;

      // 2. 回源拉取
      try {
        const resp = await fetch(originalUrl, {
          headers: { 'User-Agent': 'okhttp/3.12.0' },
        });

        if (!resp.ok) {
          return c.json({ error: `Origin returned ${resp.status}` }, 502);
        }

        // 3. 构建响应 + 异步写缓存
        const contentType = resp.headers.get('Content-Type') || 'image/jpeg';
        const response = new Response(resp.body, {
          headers: {
            'Content-Type': contentType,
            'Cache-Control': `public, max-age=${IMG_PROXY_TTL}`,
            'Access-Control-Allow-Origin': '*',
          },
        });

        c.executionCtx.waitUntil(cache.put(cacheKey, response.clone()));
        return response;
      } catch (error: unknown) {
        const msg = error instanceof Error ? error.message : String(error);
        return c.json({ error: msg }, 502);
      }
    });
  }

  // ─── Live Sources Admin API ────────────────────────────
  app.get('/admin/lives', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    const raw = await storage.get(KV_LIVE_SOURCES);
    const entries: LiveSourceEntry[] = raw ? JSON.parse(raw) : [];
    return c.json(entries);
  });

  app.post('/admin/lives', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { name?: string; url?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const url = body.url?.trim();
    if (!url) return c.json({ error: 'URL is required' }, 400);

    try {
      new URL(url);
    } catch {
      return c.json({ error: 'Invalid URL format' }, 400);
    }

    const name = body.name?.trim() || '';
    const raw = await storage.get(KV_LIVE_SOURCES);
    const entries: LiveSourceEntry[] = raw ? JSON.parse(raw) : [];

    if (entries.some((e) => e.url === url)) {
      return c.json({ error: 'Live source already exists' }, 409);
    }

    entries.push({ name, url });
    await storage.put(KV_LIVE_SOURCES, JSON.stringify(entries));

    return c.json({ success: true });
  });

  app.delete('/admin/lives', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { url?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const url = body.url?.trim();
    if (!url) return c.json({ error: 'URL is required' }, 400);

    const raw = await storage.get(KV_LIVE_SOURCES);
    const entries: LiveSourceEntry[] = raw ? JSON.parse(raw) : [];
    const filtered = entries.filter((e) => e.url !== url);
    await storage.put(KV_LIVE_SOURCES, JSON.stringify(filtered));

    return c.json({ success: true });
  });

  // ─── MacCMS Admin API ─────────────────────────────────
  app.get('/admin/maccms', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    const raw = await storage.get(KV_MACCMS_SOURCES);
    const sources: MacCMSSourceEntry[] = raw ? JSON.parse(raw) : [];
    return c.json(sources);
  });

  app.post('/admin/maccms', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: MacCMSSourceEntry | MacCMSSourceEntry[];
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const newEntries = Array.isArray(body) ? body : [body];

    // 验证字段
    for (const entry of newEntries) {
      if (!entry.key?.trim() || !entry.name?.trim() || !entry.api?.trim()) {
        return c.json({ error: 'Each entry requires key, name, and api' }, 400);
      }
      try {
        new URL(entry.api);
      } catch {
        return c.json({ error: `Invalid URL: ${entry.api}` }, 400);
      }
    }

    const raw = await storage.get(KV_MACCMS_SOURCES);
    const sources: MacCMSSourceEntry[] = raw ? JSON.parse(raw) : [];
    const existingKeys = new Set(sources.map((s) => s.key));

    let added = 0;
    for (const entry of newEntries) {
      if (!existingKeys.has(entry.key)) {
        sources.push({ key: entry.key.trim(), name: entry.name.trim(), api: entry.api.trim() });
        existingKeys.add(entry.key);
        added++;
      }
    }

    await storage.put(KV_MACCMS_SOURCES, JSON.stringify(sources));
    return c.json({ success: true, added, total: sources.length });
  });

  app.delete('/admin/maccms', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { key?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const key = body.key?.trim();
    if (!key) return c.json({ error: 'key is required' }, 400);

    const raw = await storage.get(KV_MACCMS_SOURCES);
    const sources: MacCMSSourceEntry[] = raw ? JSON.parse(raw) : [];
    const filtered = sources.filter((s) => s.key !== key);
    await storage.put(KV_MACCMS_SOURCES, JSON.stringify(filtered));

    return c.json({ success: true });
  });

  app.post('/admin/maccms/validate', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { api?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const api = body.api?.trim();
    if (!api) return c.json({ error: 'api is required' }, 400);

    const ok = await validateMacCMS(api, config.siteTimeoutMs);
    return c.json({ api, valid: ok });
  });

  // ─── Config Editor 页面 ─────────────────────────────────
  app.get('/admin/config-editor', (c) => {
    return c.html(configEditorHtml);
  });

  // ─── Config Editor API ─────────────────────────────────
  app.get('/admin/config-data', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    // 读取过滤前的完整配置（含被屏蔽的项），降级到已过滤配置
    const full = await storage.get(KV_MERGED_CONFIG_FULL);
    const cached = full || await storage.get(KV_MERGED_CONFIG);
    if (!cached) {
      return c.json({ sites: [], parses: [], lives: [] });
    }

    let parsed: TVBoxConfig;
    try {
      parsed = JSON.parse(cached);
    } catch {
      return c.json({ error: 'Config parse error' }, 500);
    }

    const blacklist = await loadBlacklist(storage);
    const siteSet = new Set(blacklist.sites);
    const parseSet = new Set(blacklist.parses);
    const liveSet = new Set(blacklist.lives);

    // Build sites with fingerprint + blocked status + group
    const sites = [];
    for (const site of parsed.sites || []) {
      const fp = await siteFingerprint(site);
      const api = site.api || '';
      let group = '其他';
      if (api.startsWith('csp_') || api.startsWith('py_') || api.startsWith('js_')) {
        group = api;
      } else if (api.startsWith('http')) {
        try { group = '远程: ' + new URL(api).hostname; } catch { group = '远程源'; }
      }
      sites.push({ ...site, fingerprint: fp, blocked: siteSet.has(fp), group });
    }

    const parses = (parsed.parses || []).map(p => ({
      ...p,
      blocked: parseSet.has(p.url),
    }));

    const lives = (parsed.lives || []).map(l => ({
      ...l,
      blocked: liveSet.has(l.url || l.api || ''),
    }));

    return c.json({ sites, parses, lives });
  });

  app.post('/admin/blacklist', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { type?: string; id?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const { type, id } = body;
    if (!type || !id) return c.json({ error: 'type and id are required' }, 400);
    if (!['sites', 'parses', 'lives'].includes(type)) {
      return c.json({ error: 'type must be sites, parses, or lives' }, 400);
    }

    const blacklist = await loadBlacklist(storage);
    const list = blacklist[type as keyof typeof blacklist] as string[];
    if (!list.includes(id)) {
      list.push(id);
    }
    await saveBlacklist(storage, blacklist);

    return c.json({ success: true });
  });

  app.delete('/admin/blacklist', async (c) => {
    if (!verifyAdmin(c.req.raw, config)) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    let body: { type?: string; id?: string };
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    const { type, id } = body;
    if (!type || !id) return c.json({ error: 'type and id are required' }, 400);
    if (!['sites', 'parses', 'lives'].includes(type)) {
      return c.json({ error: 'type must be sites, parses, or lives' }, 400);
    }

    const blacklist = await loadBlacklist(storage);
    const key = type as keyof typeof blacklist;
    (blacklist[key] as string[]) = (blacklist[key] as string[]).filter((v: string) => v !== id);
    await saveBlacklist(storage, blacklist);

    return c.json({ success: true });
  });

  // ─── 刷新 ─────────────────────────────────────────────
  app.post('/refresh', async (c) => {
    if (config.refreshToken || config.adminToken) {
      const auth = c.req.raw.headers.get('Authorization');
      const validTokens = [config.refreshToken, config.adminToken].filter(Boolean);
      if (!validTokens.some((t) => auth === `Bearer ${t}`)) {
        return c.json({ error: 'Unauthorized' }, 401);
      }
    }

    try {
      await deps.triggerRefresh();
      return c.json({ success: true, message: 'Refresh completed' });
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      return c.json({ success: false, error: msg }, 500);
    }
  });

  return app;
}

function verifyAdmin(request: Request, config: AppConfig): boolean {
  const token = config.adminToken;
  if (!token) return false;
  const auth = request.headers.get('Authorization');
  return auth === `Bearer ${token}`;
}
