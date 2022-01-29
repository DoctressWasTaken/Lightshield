FROM node:10-alpine as build-step

RUN mkdir -p /project
WORKDIR /project
COPY package.json .
RUN npm install
COPY . .
RUN npm run build --prod


FROM nginx:1.17.1-alpine

COPY --from=build-step /project/dist/frontend /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf
