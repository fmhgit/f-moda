# f-moda-zsy 端点代理 - Node.js 版
# 注意: 镜像内不含 config.json 和任何密钥，必须运行时注入
#
# === 方式一: 环境变量（推荐 Docker 部署） ===
# docker build -t fjhdocker/f-moda-zsy:latest .
# docker run -d -p 4200:4200 \
#   -e NAME="端点-1" \
#   -e API_KEY="ms-your-key" \
#   -e AUTH_KEY="change-me-random-string" \
#   -e MODEL="Qwen/Qwen3.5-35B-A3B" \
#   --name f-moda-zsy fjhdocker/f-moda-zsy:latest
#
# === 方式二: config.prod.json 挂载（运行时注入，不入镜像） ===
# docker run -d -p 4200:4200 \
#   -v $(pwd)/config.prod.json:/app/config.json:ro \
#   --name f-moda-zsy fjhdocker/f-moda-zsy:latest

FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --production --no-optional && npm cache clean --force

FROM node:20-alpine

RUN addgroup -g 1001 appuser && adduser -u 1001 -G appuser -s /bin/sh -D appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app/node_modules ./node_modules
COPY --chown=appuser:appuser proxy.js ./
RUN chmod 644 /app/proxy.js

EXPOSE 4200

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://localhost:4200/health').then(r => r.ok ? process.exit(0) : process.exit(1)).catch(() => process.exit(1))"

USER appuser

ENV NODE_ENV=production

CMD ["node", "proxy.js"]
