FROM node:8.9.4-alpine

RUN mkdir -p /app
WORKDIR /app

COPY . /app

RUN apk add --no-cache make gcc g++ python && \
  npm install --production && \
  apk del make gcc g++ python

CMD ["npm", "start"]
