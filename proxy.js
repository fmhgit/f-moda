'use strict';

const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');

// ============================
// 配置加载
// ============================
const CONFIG_PATH = path.join(__dirname, 'config.json');
let config;

function loadConfig() {
  // 默认配置
  const defaults = {
    name: '未命名端点',
    port: 4200,
    logLevel: 'info',
    timeout: { connection: 30000, request: 120000, streamChunk: 60000 },
    retry: { maxRetries: 3, retryDelay: 1000, retryOnStatusCodes: [429, 500, 502, 503, 504] },
    rateLimit: { enabled: false, windowMs: 60000, maxRequests: 100 },
    proxy: { enabled: false, url: '' },
    authKey: '',
  };

  // 尝试加载 config.json（可选）
  let fileConfig = {};
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      const raw = fs.readFileSync(CONFIG_PATH, 'utf-8');
      fileConfig = JSON.parse(raw);
    }
  } catch (err) {
    console.warn(`[WARN] config.json 解析失败，使用默认配置: ${err.message}`);
  }

  // 环境变量覆盖（优先级最高）
  config = Object.assign({}, defaults, fileConfig);

  if (process.env.NAME) config.name = process.env.NAME;
  if (process.env.API_KEY) config.apiKey = process.env.API_KEY;
  if (process.env.AUTH_KEY) config.authKey = process.env.AUTH_KEY;
  if (process.env.MODEL) config.model = process.env.MODEL;
  if (process.env.PORT) config.port = parseInt(process.env.PORT, 10);
  if (process.env.LOG_LEVEL) config.logLevel = process.env.LOG_LEVEL;

  if (process.env.TIMEOUT_CONNECTION) config.timeout.connection = parseInt(process.env.TIMEOUT_CONNECTION, 10);
  if (process.env.TIMEOUT_REQUEST) config.timeout.request = parseInt(process.env.TIMEOUT_REQUEST, 10);
  if (process.env.TIMEOUT_STREAM_CHUNK) config.timeout.streamChunk = parseInt(process.env.TIMEOUT_STREAM_CHUNK, 10);

  if (process.env.RETRY_MAX_RETRIES) config.retry.maxRetries = parseInt(process.env.RETRY_MAX_RETRIES, 10);
  if (process.env.RETRY_DELAY) config.retry.retryDelay = parseInt(process.env.RETRY_DELAY, 10);
  if (process.env.RETRY_ON_STATUS) config.retry.retryOnStatusCodes = process.env.RETRY_ON_STATUS.split(',').map(Number);

  if (process.env.RATE_LIMIT_ENABLED) config.rateLimit.enabled = process.env.RATE_LIMIT_ENABLED === 'true';
  if (process.env.RATE_LIMIT_WINDOW_MS) config.rateLimit.windowMs = parseInt(process.env.RATE_LIMIT_WINDOW_MS, 10);
  if (process.env.RATE_LIMIT_MAX_REQUESTS) config.rateLimit.maxRequests = parseInt(process.env.RATE_LIMIT_MAX_REQUESTS, 10);

  if (process.env.PROXY_ENABLED) config.proxy.enabled = process.env.PROXY_ENABLED === 'true';
  if (process.env.PROXY_URL) config.proxy.url = process.env.PROXY_URL;

  const name = config.name || '未命名端点';
  log('info', `配置加载成功 - ${name}, 端口: ${config.port}`);
}

// ============================
// 日志系统
// ============================
const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

function log(level, message) {
  const currentLevel = config ? (LOG_LEVELS[config.logLevel] || LOG_LEVELS.info) : LOG_LEVELS.info;
  if (LOG_LEVELS[level] >= currentLevel) {
    const timestamp = new Date().toISOString();
    const prefix = `[${timestamp}] [${level.toUpperCase()}]`;
    if (level === 'error') {
      console.error(`${prefix} ${message}`);
    } else if (level === 'warn') {
      console.warn(`${prefix} ${message}`);
    } else {
      console.log(`${prefix} ${message}`);
    }
  }
}

// ============================
// 基础 URL (硬编码)
// ============================
const BASE_URL = 'https://api-inference.modelscope.cn/v1';

// ============================
// 超时控制 - 自定义 AbortController
// ============================
function createTimeoutController(ms) {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort();
    log('warn', `请求超时 (${ms}ms)`);
  }, ms);
  // 当请求完成时清除定时器
  controller.signal.addEventListener('abort', () => clearTimeout(timer));
  return { controller, clearTimer: () => clearTimeout(timer) };
}

// ============================
// 重试机制
// ============================
function shouldRetry(statusCode) {
  const retryCodes = config.retry?.retryOnStatusCodes || [429, 500, 502, 503, 504];
  return retryCodes.includes(statusCode);
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, options, maxRetries) {
  const retries = maxRetries ?? config.retry?.maxRetries ?? 3;
  const delay = config.retry?.retryDelay ?? 1000;
  let lastError;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      log('debug', `请求尝试 ${attempt}/${retries} - ${url}`);

      // 每次重试创建新的超时控制器（流式请求使用更长的超时）
      const isStream = options.headers?.['Accept'] === 'text/event-stream' ||
                       options.body?._isStreamRequest;
      const timeoutMs = isStream
        ? (config.timeout?.streamChunk ?? 60000) * 10  // 流式请求给更长的总超时
        : config.timeout?.request ?? 120000;

      const { controller, clearTimer } = createTimeoutController(timeoutMs);
      const fetchOptions = { ...options, signal: controller.signal };

      const response = await fetch(url, fetchOptions);

      // 如果状态码需要重试
      if (!response.ok && shouldRetry(response.status)) {
        clearTimer();
        lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
        log('warn', `请求失败 HTTP ${response.status}, ${attempt < retries ? `等待 ${delay * attempt}ms 后重试` : '已达最大重试次数'}`);

        if (attempt < retries) {
          await sleep(delay * attempt); // 递增延迟
          continue;
        }
        return response; // 返回最后的失败响应
      }

      clearTimer();
      return response;
    } catch (err) {
      lastError = err;
      if (err.name === 'AbortError') {
        log('warn', `请求超时, ${attempt < retries ? `重试中...` : '已达最大重试次数'}`);
      } else {
        log('error', `请求异常: ${err.message}`);
      }

      if (attempt < retries) {
        await sleep(delay * attempt);
        continue;
      }
      throw lastError;
    }
  }
  throw lastError;
}

// ============================
// 简易限流器
// ============================
const requestCounts = new Map();

function rateLimitCheck(req) {
  const windowMs = config.rateLimit?.windowMs ?? 60000;
  const maxRequests = config.rateLimit?.maxRequests ?? 100;
  const ip = req.ip || req.connection.remoteAddress || 'unknown';
  const now = Date.now();

  const record = requestCounts.get(ip);
  if (!record || now - record.startTime > windowMs) {
    requestCounts.set(ip, { startTime: now, count: 1 });
    return true;
  }

  if (record.count >= maxRequests) {
    return false;
  }

  record.count++;
  return true;
}

// 定期清理过期限流记录
setInterval(() => {
  const windowMs = config.rateLimit?.windowMs ?? 60000;
  const now = Date.now();
  for (const [ip, record] of requestCounts.entries()) {
    if (now - record.startTime > windowMs * 2) {
      requestCounts.delete(ip);
    }
  }
}, 60000);

// ============================
// 请求体大小限制
// ============================
const MAX_BODY_SIZE = 10 * 1024 * 1024; // 10MB

// ============================
// Express 应用
// ============================
const app = express();
app.use(cors());
app.use(express.json({ limit: MAX_BODY_SIZE }));

// 健康检查端点
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    name: config.name || '未命名',
    model: config.model || '',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// 首页
app.get('/', async (req, res) => {
  const lines = [
    '沁园春·雪',
    '',
    '北国风光，千里冰封，万里雪飘。',
    '望长城内外，惟余莽莽；大河上下，顿失滔滔。',
    '山舞银蛇，原驰蜡象，欲与天公试比高。',
    '须晴日，看红装素裹，分外妖娆。',
    '',
    '江山如此多娇，引无数英雄竞折腰。',
    '惜秦皇汉武，略输文采；唐宗宋祖，稍逊风骚。',
    '一代天骄，成吉思汗，只识弯弓射大雕。',
    '俱往矣，数风流人物，还看今朝。',
  ];

  try {
    let ip = null;
    const ipServices = [
      'https://api.ipify.org',
      'https://ifconfig.me/ip',
      'https://icanhazip.com',
      'https://checkip.amazonaws.com',
    ];

    for (const svc of ipServices) {
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 3000);
        const resp = await fetch(svc, { signal: controller.signal });
        clearTimeout(timer);
        if (resp.ok) {
          ip = (await resp.text()).trim();
          break;
        }
      } catch (_) { /* 尝试下一个 */ }
    }

    lines.push('', ip || '(公网IP获取失败)');
  } catch (_) {
    lines.push('', '(公网IP获取失败)');
  }

  res.type('text/plain; charset=utf-8').send(lines.join('\n'));
});

// 配置查看端点（不暴露 apiKey）
app.get('/config', (req, res) => {
  res.json({
    name: config.name || '未命名',
    model: config.model || '',
    port: config.port,
    timeout: config.timeout,
    retry: config.retry,
    rateLimitEnabled: config.rateLimit?.enabled !== false,
    rateLimit: config.rateLimit,
    proxyEnabled: config.proxy?.enabled ?? false,
    logLevel: config.logLevel
  });
});

// ============================
// 核心: /v1/chat/completions 代理
// ============================
app.post('/v1/chat/completions', async (req, res) => {
  // 限流检查（开关控制）
  if (config.rateLimit?.enabled !== false && !rateLimitCheck(req)) {
    log('warn', `限流拒绝 - IP: ${req.ip}`);
    return res.status(429).json({
      error: {
        message: '请求过于频繁，请稍后再试',
        type: 'rate_limit_error',
        code: 'rate_limit_exceeded'
      }
    });
  }

  const requestBody = req.body;

  // 验证必要字段
  if (!requestBody.messages || !Array.isArray(requestBody.messages) || requestBody.messages.length === 0) {
    return res.status(400).json({
      error: {
        message: 'messages 字段为必填项，且必须是非空数组',
        type: 'invalid_request_error',
        code: 'missing_messages'
      }
    });
  }

  // 构建转发请求体 - 使用 config 中的 model，但允许请求中覆盖
  const forwardBody = {
    ...requestBody,
    model: requestBody.model || config.model
  };

  // 清理 Cherry Studio 等客户端发送的 "[undefined]" 字符串垃圾字段
  for (const key of Object.keys(forwardBody)) {
    if (forwardBody[key] === '[undefined]') {
      delete forwardBody[key];
    }
  }

  // apiKey 认证: Authorization 头必须携带正确的私有 key
  let incomingKey = '';
  if (req.headers['authorization']) {
    const auth = req.headers['authorization'];
    if (auth.startsWith('Bearer ')) {
      incomingKey = auth.slice(7);
    }
  }

  if (!incomingKey || incomingKey !== config.authKey) {
    log('warn', `认证失败 - IP: ${req.ip}, Key: ${incomingKey.substring(0, 10)}...`);
    return res.status(401).json({
      error: {
        message: 'Unauthorized: invalid or missing API key',
        type: 'authentication_error',
        code: 'invalid_api_key'
      }
    });
  }

  // apiKey: x-api-key 优先（调度端传来的），fallback config
  let apiKey = config.apiKey;
  const xApiKey = req.headers['x-api-key'];
  if (xApiKey) {
    apiKey = xApiKey;
    log('debug', '使用 x-api-key 中的调度端 API Key');
  }

  // baseUrl: x-base-url 优先（调度端传来的），fallback 硬编码
  const baseUrl = req.headers['x-base-url'] || BASE_URL;

  const isStream = forwardBody.stream === true;

  log('info', `收到请求 - 模型: ${forwardBody.model}, 流式: ${isStream}, 消息数: ${forwardBody.messages.length}`);

  const targetUrl = `${baseUrl}/chat/completions`;

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`,
  };

  if (isStream) {
    headers['Accept'] = 'text/event-stream';
  }

  // 代理配置
  let fetchProxy = undefined;
  if (config.proxy?.enabled && config.proxy?.url) {
    fetchProxy = config.proxy.url;
    log('debug', `使用 HTTP 代理: ${fetchProxy}`);
  }

  try {
    if (isStream) {
      // ============================
      // 流式请求处理 - 逐 chunk 转发，带 chunk 超时保活
      // ============================
      const streamTimeoutMs = (config.timeout?.streamChunk ?? 60000) * 10;
      const chunkTimeoutMs = config.timeout?.streamChunk ?? 60000;

      log('debug', `流式请求 - 总超时: ${streamTimeoutMs}ms, chunk超时: ${chunkTimeoutMs}ms`);

      const response = await fetchWithRetry(targetUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(forwardBody),
        proxy: fetchProxy,
      });

      if (!response.ok) {
        const errorText = await response.text();
        log('error', `上游返回错误 HTTP ${response.status}: ${errorText.substring(0, 500)}`);
        return res.status(response.status).json({
          error: {
            message: `上游服务返回错误: ${response.status} ${response.statusText}`,
            type: 'upstream_error',
            code: `http_${response.status}`,
            details: errorText.substring(0, 1000)
          }
        });
      }

      // 设置 SSE 响应头
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no'); // nginx 优化
      res.flushHeaders();

      // 流式转发，带 chunk 超时检测
      const upstreamStream = response.body;
      let lastChunkTime = Date.now();
      let chunkCount = 0;

      // chunk 超时保活定时器
      const chunkTimer = setInterval(() => {
        const elapsed = Date.now() - lastChunkTime;
        if (elapsed > chunkTimeoutMs) {
          log('warn', `流式 chunk 超时 (${elapsed}ms), 发送保活注释`);
          // 发送 SSE 保活注释，防止连接被断开
          res.write(': keep-alive\n\n');
          lastChunkTime = Date.now(); // 重置计时
        }
      }, chunkTimeoutMs / 3); // 每1/3超时时间检查一次

      // 总超时定时器
      const totalTimer = setTimeout(() => {
        log('warn', `流式总超时 (${streamTimeoutMs}ms), 终止连接`);
        clearInterval(chunkTimer);
        try {
          res.write(`data: [DONE]\n\n`);
          res.end();
        } catch (e) { /* 连接可能已关闭 */ }
      }, streamTimeoutMs);

      upstreamStream.on('data', (chunk) => {
        lastChunkTime = Date.now();
        chunkCount++;
        const data = chunk.toString('utf-8');
        res.write(data);
      });

      upstreamStream.on('end', () => {
        clearInterval(chunkTimer);
        clearTimeout(totalTimer);
        log('info', `流式转发完成 - 共 ${chunkCount} 个 chunk`);
        try {
          res.end();
        } catch (e) { /* 连接可能已关闭 */ }
      });

      upstreamStream.on('error', (err) => {
        clearInterval(chunkTimer);
        clearTimeout(totalTimer);
        log('error', `流式上游错误: ${err.message}`);
        try {
          res.write(`data: {"error":{"message":"流式传输中断: ${err.message}","type":"stream_error"}}\n\n`);
          res.write('data: [DONE]\n\n');
          res.end();
        } catch (e) { /* 连接可能已关闭 */ }
      });

      // 客户端断开处理
      req.on('close', () => {
        clearInterval(chunkTimer);
        clearTimeout(totalTimer);
        log('info', '客户端断开连接');
        try { upstreamStream.destroy(); } catch (e) { /* 忽略 */ }
      });

    } else {
      // ============================
      // 非流式请求处理
      // ============================
      const requestTimeoutMs = config.timeout?.request ?? 120000;
      const { controller, clearTimer } = createTimeoutController(requestTimeoutMs);

      const response = await fetchWithRetry(targetUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(forwardBody),
        signal: controller.signal,
        proxy: fetchProxy,
      });

      clearTimer();

      if (!response.ok) {
        const errorText = await response.text();
        log('error', `上游返回错误 HTTP ${response.status}: ${errorText.substring(0, 500)}`);
        return res.status(response.status).json({
          error: {
            message: `上游服务返回错误: ${response.status} ${response.statusText}`,
            type: 'upstream_error',
            code: `http_${response.status}`,
            details: errorText.substring(0, 1000)
          }
        });
      }

      const data = await response.json();
      log('info', `非流式请求完成 - 模型: ${forwardBody.model}`);
      res.json(data);
    }
  } catch (err) {
    log('error', `代理请求异常: ${err.message}`);

    if (err.name === 'AbortError') {
      return res.status(504).json({
        error: {
          message: '请求超时，上游服务未在规定时间内响应',
          type: 'timeout_error',
          code: 'gateway_timeout'
        }
      });
    }

    return res.status(500).json({
      error: {
        message: `代理服务内部错误: ${err.message}`,
        type: 'internal_error',
        code: 'proxy_error'
      }
    });
  }
});

// ============================
// 其他 OpenAI 兼容端点代理
// ============================

// 模型列表
app.get('/v1/models', async (req, res) => {
  let incomingKey = '';
  if (req.headers['authorization']?.startsWith('Bearer ')) {
    incomingKey = req.headers['authorization'].slice(7);
  }

  if (!incomingKey || incomingKey !== config.authKey) {
    return res.status(401).json({
      error: {
        message: 'Unauthorized: invalid or missing API key',
        type: 'authentication_error',
        code: 'invalid_api_key'
      }
    });
  }

  let apiKey = config.apiKey;
  const xApiKey = req.headers['x-api-key'];
  if (xApiKey) {
    apiKey = xApiKey;
  }

  const baseUrl = req.headers['x-base-url'] || BASE_URL;

  try {
    const response = await fetch(`${baseUrl}/models`, {
      headers: { 'Authorization': `Bearer ${apiKey}` },
      proxy: config.proxy?.enabled ? config.proxy?.url : undefined,
    });

    if (!response.ok) {
      return res.status(response.status).json({
        error: { message: `获取模型列表失败: ${response.status}`, type: 'upstream_error' }
      });
    }

    const data = await response.json();
    res.json(data);
  } catch (err) {
    log('error', `获取模型列表失败: ${err.message}`);
    res.status(500).json({
      error: { message: err.message, type: 'internal_error' }
    });
  }
});

// ============================
// 优雅关闭处理
// ============================
function gracefulShutdown(signal) {
  log('info', `收到 ${signal} 信号，正在优雅关闭...`);
  server.close(() => {
    log('info', '服务器已关闭');
    process.exit(0);
  });

  // 10秒后强制退出
  setTimeout(() => {
    log('warn', '强制退出');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// 未捕获异常处理
process.on('uncaughtException', (err) => {
  log('error', `未捕获异常: ${err.message}\n${err.stack}`);
});

process.on('unhandledRejection', (reason, promise) => {
  log('error', `未处理的 Promise 拒绝: ${reason}`);
});

// ============================
// 启动服务器
// ============================
loadConfig();

// 支持环境变量覆盖端口
const PORT = process.env.PORT || config.port || 3000;

const server = app.listen(PORT, '0.0.0.0', () => {
  log('info', `============================================`);
  log('info', `  ModelScope 代理服务器已启动`);
  log('info', `  监听端口: ${PORT}`);
  log('info', `  上游地址: ${BASE_URL}`);
  log('info', `  端点名称: ${config.name || '未命名'}`);
  log('info', `============================================`);
  log('info', `使用方式:`);
  log('info', `  OpenAI 兼容接口: http://localhost:${PORT}/v1/chat/completions`);
  log('info', `  健康检查:        http://localhost:${PORT}/health`);
  log('info', `  配置查看:        http://localhost:${PORT}/config`);
  log('info', `============================================`);
});

// 服务器超时设置
server.timeout = 300000; // 5分钟总超时
server.keepAliveTimeout = 120000; // 2分钟 keep-alive
server.headersTimeout = 125000; // 略大于 keepAliveTimeout
