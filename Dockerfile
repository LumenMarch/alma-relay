FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY index.mjs shared.mjs apns.mjs ./
COPY scripts/mint-token.mjs ./scripts/
USER node
ENV PORT=8787
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s \
    CMD wget -qO- "http://127.0.0.1:${PORT}/healthz" >/dev/null || exit 1
CMD ["node", "index.mjs"]
