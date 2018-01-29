FROM node:8.9.4-alpine

RUN mkdir -p /app
WORKDIR /app

COPY . /app

RUN npm install --production
CMD ["npm", "start"]
