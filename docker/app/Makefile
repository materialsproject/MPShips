# Load AWS_ACCOUNT_ID (and any other vars) from .env, if present.
-include .env

CODEARTIFACT_DOMAIN = mp-private
CODEARTIFACT_REPO = mp-ui-pypi
CODEARTIFACT_REGION = us-east-1

install-mp-ui:
	@if [ -z "$$PIP_INDEX_URL" ]; then \
		export PIP_INDEX_URL=$$($(MAKE) -s print-pip-index-url) || exit 1; \
	fi && \
	pip install --upgrade pip && \
	pip install --no-cache-dir --compile mp-ui==v0.0.22rc1

# sync-mp-ui-assets: safe to call standalone even if src/$(APP_NAME)/assets doesn't
# exist yet (e.g. in a Docker stage that hasn't run the JS build).
sync-mp-ui-assets:
	@mkdir -p src/$(APP_NAME)/assets && \
	MP_UI_ASSETS="$$(python -c 'import mp_ui, os; print(os.path.dirname(mp_ui.__file__))')/styling/assets" && \
	rm -rf src/$(APP_NAME)/assets/10_mp-ui && \
	cp -r "$$MP_UI_ASSETS" src/$(APP_NAME)/assets/10_mp-ui


# sync-mp-ui-devops-scripts: copies the frontend deployment scripts bundled
# inside the installed mp_ui package to where the app expects them, mirroring
# the Dockerfile's old COPY layout (start.sh/healthcheck.sh/main.py/
# supervisord.conf/gunicorn.conf.py at the app root, wsgi.py under src/$(APP_NAME)/).
sync-devops-scripts:
	@SCRIPTS="$$(python -c 'import mp_ui, os; print(os.path.dirname(mp_ui.__file__))')/frontend-devops-scripts" && \
	mkdir -p src/$(APP_NAME) && \
	cp "$$SCRIPTS/start.sh" . && \
	cp "$$SCRIPTS/healthcheck.sh" . && \
	cp "$$SCRIPTS/main.py" . && \
	cp "$$SCRIPTS/supervisord.conf" . && \
	cp "$$SCRIPTS/gunicorn.conf.py" .
