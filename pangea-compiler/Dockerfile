FROM ruby:3.3-alpine

RUN apk add --no-cache build-base

WORKDIR /app
COPY Gemfile Gemfile.lock* ./
RUN bundle install --without development test --jobs 4

COPY . .

EXPOSE 8082

USER 65534:65534
CMD ["bundle", "exec", "puma", "-p", "8082", "-b", "tcp://0.0.0.0"]
