// JAR 仓库核心：收集、解析、存储、匹配

import type { Storage } from '../storage/interface';
import type { SourcedConfig, JarMeta, JarRegistry, JarAssignment } from './types';
import { extractSpiderJarUrl } from './parser';
import { parseSpiderString, urlToKey, uint8ArrayToBase64 } from './jar-proxy';
import { decodeJarBytes, extractDexFromJar, extractSpiderClasses } from './dex-parser';
import { KV_JAR_REGISTRY, KV_JAR_BIN_PREFIX } from './config';

const MAX_CONCURRENT_DOWNLOADS = 5;
const COLD_START_THRESHOLD = 10;

// ─── 持久化 ────────────────────────────────────────────

export async function loadJarRegistry(storage: Storage): Promise<JarRegistry | null> {
  const raw = await storage.get(KV_JAR_REGISTRY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed.version !== 1) return null;
    return parsed as JarRegistry;
  } catch {
    return null;
  }
}

export async function saveJarRegistry(storage: Storage, registry: JarRegistry): Promise<void> {
  await storage.put(KV_JAR_REGISTRY, JSON.stringify(registry));
}

// ─── JAR 收集与解析 ──────────────────────────────────────

export interface BuildJarRegistryResult {
  registry: JarRegistry;
  processed: number;    // 本次处理的 JAR 数
  remaining: number;    // 还剩多少未处理
  total: number;        // 总 JAR 数
}

export async function buildJarRegistry(
  configs: SourcedConfig[],
  storage: Storage,
  fetchTimeoutMs: number,
  saveJarBinary?: (urlHash: string, data: Uint8Array) => Promise<void>,
  options?: { skipColdStartCheck?: boolean; batchLimit?: number },
): Promise<BuildJarRegistryResult | null> {
  const skipColdStartCheck = options?.skipColdStartCheck;
  const batchLimit = options?.batchLimit; // 每次最多处理几个 JAR（CF 免费版用 3-5）

  // 1. 收集唯一 JAR URL
  const jarUrls = new Map<string, { md5: string | null; fullSpider: string }>();

  for (const sourced of configs) {
    const spiderUrl = extractSpiderJarUrl(sourced.config.spider);
    if (spiderUrl && !jarUrls.has(spiderUrl)) {
      const parsed = parseSpiderString(sourced.config.spider!);
      jarUrls.set(spiderUrl, { md5: parsed.md5, fullSpider: sourced.config.spider! });
    }

    for (const site of sourced.config.sites || []) {
      if (site.jar) {
        const url = extractSpiderJarUrl(site.jar);
        if (url && !jarUrls.has(url)) {
          const parsed = parseSpiderString(site.jar);
          jarUrls.set(url, { md5: parsed.md5, fullSpider: site.jar });
        }
      }
    }
  }

  if (jarUrls.size === 0) return null;

  // 2. 加载缓存
  const existing = await loadJarRegistry(storage);
  const cachedJars = existing?.jars || {};

  // 3. 分类
  const needDownload: Array<{ url: string; md5: string | null }> = [];
  const reuse: Record<string, JarMeta> = {};

  for (const [url, info] of jarUrls) {
    const cached = cachedJars[url];
    if (cached && cached.status) {
      // 任何已有状态的（ok/fetch_failed/parse_failed）都复用，不重新下载
      reuse[url] = { ...cached, md5: info.md5 };
    } else {
      needDownload.push({ url, md5: info.md5 });
    }
  }

  // 冷启动保护（手动刷新时跳过）
  if (!skipColdStartCheck && !existing && needDownload.length > COLD_START_THRESHOLD) {
    console.warn(
      `[jar-registry] Cold start with ${needDownload.length} JARs (>${COLD_START_THRESHOLD}), ` +
      `skipping. Use admin UI to initialize.`,
    );
    return null;
  }

  // 分批：如果有 batchLimit，只处理前 N 个
  const toProcess = batchLimit && batchLimit < needDownload.length
    ? needDownload.slice(0, batchLimit)
    : needDownload;
  const remaining = needDownload.length - toProcess.length;

  console.log(
    `[jar-registry] ${jarUrls.size} unique JARs: ${Object.keys(reuse).length} cached, ` +
    `${toProcess.length} to download` + (remaining > 0 ? `, ${remaining} deferred` : ''),
  );

  // 4. 并发下载（本批次）
  const downloaded = toProcess.length > 0
    ? await downloadAndParseJars(toProcess, fetchTimeoutMs, saveJarBinary)
    : [];

  // 5. 合并（已缓存 + 本次下载 + 上次缓存中的失败项保留原样）
  const jars: Record<string, JarMeta> = { ...reuse };
  for (const meta of downloaded) {
    jars[meta.url] = meta;
  }
  // 未处理的保留旧状态（下次再试）
  for (const item of needDownload.slice(toProcess.length)) {
    if (cachedJars[item.url]) {
      jars[item.url] = cachedJars[item.url];
    }
  }

  const registry: JarRegistry = {
    version: 1,
    updatedAt: new Date().toISOString(),
    jars,
  };

  // 有新数据时才写入（节省 KV 写入配额）
  if (downloaded.length > 0 || !existing) {
    await saveJarRegistry(storage, registry);
  }

  const okCount = Object.values(jars).filter((j) => j.status === 'ok').length;
  const totalClasses = Object.values(jars).reduce((sum, j) => sum + j.classes.length, 0);
  console.log(`[jar-registry] Saved: ${okCount}/${Object.keys(jars).length} ok, ${totalClasses} total classes`);

  return { registry, processed: toProcess.length, remaining, total: jarUrls.size };
}

async function downloadAndParseJars(
  jars: Array<{ url: string; md5: string | null }>,
  fetchTimeoutMs: number,
  saveJarBinary?: (urlHash: string, data: Uint8Array) => Promise<void>,
): Promise<JarMeta[]> {
  const results: JarMeta[] = [];

  // 分批并发
  for (let i = 0; i < jars.length; i += MAX_CONCURRENT_DOWNLOADS) {
    const batch = jars.slice(i, i + MAX_CONCURRENT_DOWNLOADS);
    const batchResults = await Promise.all(
      batch.map((j) => downloadAndParseOneJar(j.url, j.md5, fetchTimeoutMs, saveJarBinary)),
    );
    results.push(...batchResults);
  }

  return results;
}

async function downloadAndParseOneJar(
  url: string,
  md5: string | null,
  fetchTimeoutMs: number,
  saveJarBinary?: (urlHash: string, data: Uint8Array) => Promise<void>,
): Promise<JarMeta> {
  const now = new Date().toISOString();

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), fetchTimeoutMs);

    const resp = await fetch(url, {
      headers: { 'User-Agent': 'okhttp/3.12.0' },
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (!resp.ok) {
      console.warn(`[jar-registry] Fetch failed ${url}: HTTP ${resp.status}`);
      return { url, md5, classes: [], lastFetchedAt: now, status: 'fetch_failed', errorMessage: `HTTP ${resp.status}` };
    }

    const rawBytes = new Uint8Array(await resp.arrayBuffer());

    // 解码（处理图片伪装 / base64 编码）
    const jarBytes = decodeJarBytes(rawBytes);
    if (!jarBytes) {
      console.warn(`[jar-registry] Not a valid JAR/ZIP: ${url}`);
      return { url, md5, classes: [], sizeBytes: rawBytes.length, lastFetchedAt: now, status: 'parse_failed', errorMessage: 'Not a valid JAR (decode failed)' };
    }

    // 存储二进制（CF 环境）
    if (saveJarBinary) {
      const hash = await urlToKey(url);
      await saveJarBinary(md5 || hash, jarBytes);
    }

    // 解析 DEX
    const dexBytes = await extractDexFromJar(jarBytes);
    if (!dexBytes) {
      console.warn(`[jar-registry] No classes.dex in ${url}`);
      return { url, md5, classes: [], sizeBytes: jarBytes.length, lastFetchedAt: now, status: 'parse_failed', errorMessage: 'No classes.dex found' };
    }

    const classes = extractSpiderClasses(dexBytes);
    console.log(`[jar-registry] Parsed ${url.substring(0, 60)}...: ${classes.length} classes`);

    return { url, md5, classes, sizeBytes: jarBytes.length, lastFetchedAt: now, status: 'ok' };
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`[jar-registry] Error fetching ${url}: ${msg}`);
    return { url, md5, classes: [], lastFetchedAt: now, status: 'fetch_failed', errorMessage: msg };
  }
}

// ─── JAR 分配 ────────────────────────────────────────────

export function assignJars(registry: JarRegistry, configs: SourcedConfig[]): JarAssignment {
  // 1. 构建 className → JarMeta[] 倒排索引
  const classIndex = new Map<string, JarMeta[]>();
  const okJars = Object.values(registry.jars).filter((j) => j.status === 'ok');

  for (const jar of okJars) {
    for (const cls of jar.classes) {
      const list = classIndex.get(cls);
      if (list) list.push(jar);
      else classIndex.set(cls, [jar]);
    }
  }

  // 2. 收集所有 type:3 csp_ 站点需要的类
  const cspSites: Array<{ key: string; api: string; neededClass: string; sourceSpider?: string }> = [];
  const urlApiSites: Array<{ key: string; api: string; sourceSpider?: string }> = [];

  for (const sourced of configs) {
    for (const site of sourced.config.sites || []) {
      if (site.type !== 3) continue;
      if (site.api.startsWith('csp_')) {
        cspSites.push({
          key: site.key,
          api: site.api,
          neededClass: site.api.substring(4),
          sourceSpider: sourced.config.spider,
        });
      } else if (!site.jar) {
        urlApiSites.push({ key: site.key, api: site.api, sourceSpider: sourced.config.spider });
      }
    }
  }

  // 3. 选全局 spider（覆盖最多 csp_ 站点的 JAR）
  let globalJar: JarMeta | null = null;
  let globalScore = 0;

  for (const jar of okJars) {
    const jarClasses = new Set(jar.classes);
    let score = 0;
    for (const s of cspSites) {
      if (!s.sourceSpider) continue; // 已有 per-site jar 的不计入
      if (jarClasses.has(s.neededClass)) score++;
    }
    // 也算 URL api 站点（如果它们的源 spider 指向这个 JAR）
    for (const s of urlApiSites) {
      const sourceJarUrl = extractSpiderJarUrl(s.sourceSpider);
      if (sourceJarUrl === jar.url) score++;
    }
    if (score > globalScore || (score === globalScore && globalJar && jar.url < globalJar.url)) {
      globalScore = score;
      globalJar = jar;
    }
  }

  // 找到完整 spider 字符串
  let globalSpiderFull: string | null = null;
  if (globalJar) {
    for (const sourced of configs) {
      const url = extractSpiderJarUrl(sourced.config.spider);
      if (url === globalJar.url && sourced.config.spider) {
        globalSpiderFull = sourced.config.spider;
        break;
      }
    }
  }

  // 4. 分配 per-site jar
  const globalClasses = new Set(globalJar?.classes || []);
  const siteJarMap = new Map<string, string>();
  const orphanedKeys = new Set<string>();
  const stats = { totalType3: cspSites.length + urlApiSites.length, coveredByGlobal: 0, coveredByPerSite: 0, orphaned: 0, urlBasedApi: urlApiSites.length };

  for (const s of cspSites) {
    const dk = `${s.key}|${s.api}`;

    if (globalClasses.has(s.neededClass)) {
      stats.coveredByGlobal++;
      continue;
    }

    // 需要 per-site jar
    const candidates = classIndex.get(s.neededClass);
    if (!candidates || candidates.length === 0) {
      orphanedKeys.add(dk);
      stats.orphaned++;
      continue;
    }

    // 优先选站点原始源引用的 JAR
    const sourceJarUrl = extractSpiderJarUrl(s.sourceSpider);
    const preferred = candidates.find((j) => j.url === sourceJarUrl);
    const chosen = preferred || candidates[0];

    // 找到该 JAR 的完整 spider 字符串
    let fullSpider: string | null = null;
    for (const sourced of configs) {
      const url = extractSpiderJarUrl(sourced.config.spider);
      if (url === chosen.url && sourced.config.spider) {
        fullSpider = sourced.config.spider;
        break;
      }
    }

    if (fullSpider) {
      siteJarMap.set(dk, fullSpider);
      stats.coveredByPerSite++;
    } else {
      orphanedKeys.add(dk);
      stats.orphaned++;
    }
  }

  return {
    globalSpiderUrl: globalJar?.url || null,
    globalSpiderFull,
    siteJarMap,
    orphanedKeys,
    stats,
  };
}

// ─── admin API 用 ────────────────────────────────────────

export function buildJarStorageAdapter(
  storage: Storage,
  isCfMode: boolean,
): ((urlHash: string, data: Uint8Array) => Promise<void>) | undefined {
  if (!isCfMode) return undefined;
  return async (urlHash: string, data: Uint8Array) => {
    const b64 = uint8ArrayToBase64(data);
    await storage.put(KV_JAR_BIN_PREFIX + urlHash, b64);
  };
}
