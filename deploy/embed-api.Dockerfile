FROM node:22.21-alpine@sha256:0340fa682d72068edf603c305bfbc10e23219fb0e40df58d9ea4d6f33a9798bf AS build
RUN corepack enable
WORKDIR /src
COPY . .
RUN pnpm install --frozen-lockfile --filter @bae6/context-embed-api...
RUN pnpm --filter @bae6/context-embed-api... build
# Only production dependencies and dist reach the runtime image.
RUN pnpm --filter @bae6/context-embed-api deploy --prod --legacy /app

FROM node:22.21-alpine@sha256:0340fa682d72068edf603c305bfbc10e23219fb0e40df58d9ea4d6f33a9798bf
WORKDIR /app
COPY --from=build /app .
ENV NODE_ENV=production
USER node
CMD ["node", "dist/main.js"]
