# Warning! This Dockerfile is used in the production deployment, make changes with care.
FROM materialsproject/devops:python-3.1213.2 AS python-base
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  libjpeg62 libopenblas-dev supervisor git vim swig openbabel libopenbabel-dev curl \
  && apt-get clean
# NUM_WORKERS = NUM_CPUS+1 recommended, more is better but beware memory usage
# NUM_THREADS = 2-4 * NUM_WORKERS recommended
# MP_DEBUG_MODE should always be False in production, see mp_web/settings.py for more options
# RELOAD = "--reload" for live updates while debugging
# MAX_REQUESTS a worker can serve before restarting (fallback in case of memory leaks, ~2000 / 5 minutes)
# MAX_REQUESTS_JITTER so that all workers do not restart at once (10% of MAX_REQUESTS seems sensible)
ENV PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  NUM_WORKERS=4 \
  NUM_THREADS=12 \
  MP_DEBUG_MODE=False \
  RELOAD="" \
  MAX_REQUESTS=0 \
  MAX_REQUESTS_JITTER=0 \
  PIP_FLAGS="--no-cache-dir --compile" \
  ECS_CONTAINER_METADATA_URI_V4=""

FROM python-base AS builder-base
RUN apt-get update && apt-get install -yqq --no-install-recommends build-essential cmake wget && apt-get clean
WORKDIR /app
# COPY ./web/requirements/deployment.txt ./requirements.txt
# RUN pip install --upgrade pip && pip install $PIP_FLAGS -r requirements.txt
RUN wget -q https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
  chmod +x wait-for-it.sh && mv wait-for-it.sh /usr/local/bin/

FROM python-base
WORKDIR /app
COPY --from=builder-base /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder-base /usr/local/bin /usr/local/bin

ARG APP_NAME
WORKDIR /app
COPY ./MPShips/apps/${APP_NAME}/ .
COPY ./MPShips/docker/app/wsgi.py ./src/${APP_NAME}/wsgi.py
COPY ./MPShips/docker/app/start.sh .
COPY ./MPShips/docker/app/healthcheck.sh .
COPY ./MPShips/docker/app/main.py .
COPY ./MPShips/docker/app/supervisord.conf .


RUN mkdir -p /var/log/supervisor
RUN chmod +x main.py start.sh healthcheck.sh
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${WEB_VERSION}
RUN pip install $PIP_FLAGS .
#COPY crystaltoolkit crystaltoolkit
#RUN cd crystaltoolkit && pip install $PIP_FLAGS --no-deps -e .

ARG VERSION
ENV FLASK_APP="${APP_NAME}.wsgi:application" \
  DD_ENV=prod \
  DD_SERVICE=${APP_NAME} \
  DD_VERSION=$VERSION \
  DD_PROFILING_ENABLED=true \
  DD_LOGS_INJECTION=true \
  DD_TRACE_SAMPLE_RATE=1 \
  DD_TRACE_HOST=localhost:8126
LABEL com.datadoghq.ad.logs="[{\"source\":\"mp-frontend\",\"service\":\"${APP_NAME}\"}]"
EXPOSE 8081

CMD ["/usr/bin/supervisord", "-c", "supervisord.conf"]
